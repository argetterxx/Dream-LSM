//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/remote_flush_job.h"

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <iterator>
#include <mutex>
#include <thread>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/internal_stats.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_checker.h"
#include "db/system_clock_factory.h"
#include "db/tcprw.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/macro.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table_properties.h"
#include "table/internal_iterator.h"
#include "util/autovector.h"
#include "utilities/rf_stats.h"

// for socket API
#ifdef __linux
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#endif  //__linux

#include <algorithm>
#include <cinttypes>
#include <memory>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/flush_job_basic.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

RemoteFlushJob::RemoteFlushJob(
    const std::string& dbname, ColumnFamilyData* cfd,
    const ImmutableDBOptions& db_options,
    const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
    const FileOptions& file_options, VersionSet* versions,
    InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, JobContext* job_context,
    FlushReason flush_reason, LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_file_directory, CompressionType output_compression,
    Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
    const bool sync_output_directory, const bool write_manifest,
    const std::shared_ptr<IOTracer>& io_tracer,
    const SeqnoToTimeMapping& seqno_time_mapping,
#ifdef ROCKSDB_RDMA
    RDMAClient* rdma_client,
#endif
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low, BlobFileCompletionCallback* blob_callback)
    : local_generator_node(sockaddr_in{}, 0),
#ifdef ROCKSDB_RDMA
      local_generator_rdma_client(rdma_client),
#endif
      dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      max_memtable_id_(max_memtable_id),
      file_options_(file_options),
      versions_(versions),
      db_mutex_(db_mutex),
      shutting_down_(shutting_down),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      job_context_(job_context),
      flush_reason_(flush_reason),
      db_directory_(db_directory),
      output_file_directory_(output_file_directory),
      output_compression_(output_compression),
      sync_output_directory_(sync_output_directory),
      write_manifest_(write_manifest),
      edit_(nullptr),
      base_(nullptr),
      pick_memtable_called(false),
      clock_(db_options_.clock),
      full_history_ts_low_(std::move(full_history_ts_low)),
      stats_(stats),
      db_impl_seqno_time_mapping_(seqno_time_mapping) {
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("RemoteFlushJob::RemoteFlushJob()");
}

void RemoteFlushJob::PackLocal(TransferService* node) const {
  LOG("RemoteFlushJob::PackLocal thread_id:", std::this_thread::get_id(),
      "Pack data from local side");
  versions_->PackLocal(node);
  edit_->PackLocal(node);
  if (job_context_ != nullptr) {
    LOG("jobcontext not null");
    size_t msg_ = 1;
    node->send(&msg_, sizeof(size_t));
    job_context_->PackLocal(node);
  } else {
    LOG("jobcontext null");
    size_t msg_ = 0;
    node->send(&msg_, sizeof(size_t));
  }
  // Pack db_impl_seqno_time_mapping_
  db_impl_seqno_time_mapping_.PackLocal(node);
  seqno_to_time_mapping_.PackLocal(node);
  // Pack cfd_
  cfd_->PackLocal(node, db_mutex_);
  assert(base_ != nullptr && base_->cfd() == cfd_);
  if (base_ != cfd_->current()) {
    size_t msg = 0;
    node->send(&msg, sizeof(size_t));
    base_->PackLocal(node);
  } else {
    size_t msg = 1;
    node->send(&msg, sizeof(size_t));
  }
  db_mutex_->Unlock();
  for (int i = 0; i < 4; i++) meta_[i].PackLocal(node);
  file_options_.PackLocal(node);
  // db_mutex_->Lock();
  // db_mutex_->Unlock();
  // mutable_cf_options_.PackLocal(server_socket_fd);
  size_t dbname_len = dbname_.size();
  node->send(&dbname_len, sizeof(size_t));
  if (!dbname_.empty()) node->send(dbname_.data(), dbname_len);

  size_t len = full_history_ts_low_.size();
  node->send(&len, sizeof(size_t));
  if (!full_history_ts_low_.empty())
    node->send(full_history_ts_low_.data(), len);

  size_t db_id_len = db_id_.size();
  node->send(&db_id_len, sizeof(size_t));
  if (!db_id_.empty()) node->send(db_id_.data(), db_id_len);

  size_t db_session_id_len = db_session_id_.size();
  node->send(&db_session_id_len, sizeof(size_t));
  if (!db_session_id_.empty())
    node->send(db_session_id_.data(), db_session_id_len);

  size_t existing_snapshots_len = existing_snapshots_.size();
  node->send(&existing_snapshots_len, sizeof(size_t));
  if (existing_snapshots_len) {
    for (size_t i = 0; i < existing_snapshots_len; i++)
      node->send(&existing_snapshots_[i], sizeof(SequenceNumber));
  }

  if (snapshot_checker_ == nullptr) {
    size_t msg = 0xff;
    node->send(&msg, sizeof(size_t));
  } else {
    size_t msg = 0x02;
    node->send(&msg, sizeof(size_t));
    snapshot_checker_->PackLocal(node);
  }
  assert(stats_ == cfd_->ioptions()->stats);
  node->send(reinterpret_cast<const void*>(this), sizeof(RemoteFlushJob));
}

void RemoteFlushJob::DoubleCheck(TransferService* node) const {
  LOG_CERR("RemoteFlushJob::DoubleCheck::versions_");
  versions_->DoubleCheck(node);
  LOG_CERR("RemoteFlushJob::DoubleCheck::edit_: ptr=",
           reinterpret_cast<void*>(edit_));
  if (edit_ != nullptr) edit_->DoubleCheck(node);
  cfd_->DoubleCheck(node);
  LOG_CERR("RemoteFlushJob::DoubleCheck::base_: ptr=",
           reinterpret_cast<void*>(base_));
  if (base_ != nullptr) base_->DoubleCheck(node);
  LOG_CERR("RemoteFlushJob::DoubleCheck::meta_");
  for (int i = 0; i < 4; i++) meta_[i].DoubleCheck(node);
}

void* RemoteFlushJob::UnPackLocal(RDMAClient* client, TransferService* node,
                                  DBImpl* remote_db,
                                  std::vector<MemTable*>& temp_mems) {
  LOG("RemoteFlushJob::UnPackLocal thread_id:", std::this_thread::get_id(),
      "UnPack data from local side");
  void* mem = malloc(sizeof(RemoteFlushJob));
  auto* local_handler = reinterpret_cast<RemoteFlushJob*>(mem);

  // void* output_file_directory_ret =
  //     FSDirectoryFactory::UnPackLocal(worker_socket_fd);
  // void* db_directory_ret = FSDirectoryFactory::UnPackLocal(worker_socket_fd);
  void* clock_ret_addr = Env::Default()->GetSystemClock().get();

  // RemoteFlushJob recv other data
  size_t memtable_size = temp_mems.size();

  void* versions_ret = VersionSet::UnPackLocal(node);
  LOG_CERR("RemoteFlushJob::UnPackLocal::versions_");
  void* edit_ret = VersionEdit::UnPackLocal(node);
  LOG_CERR("RemoteFlushJob::UnPackLocal::edit_")
  void* job_context_ret = JobContext::UnPackLocal(node);
  LOG_CERR("RemoteFlushJob::UnPackLocal::job_context_");
  void* db_impl_seqno_time_mapping_ret = SeqnoToTimeMapping::UnPackLocal(node);
  void* seqno_to_time_mapping_ret = SeqnoToTimeMapping::UnPackLocal(node);
  LOG_CERR("RemoteFlushJob::UnPackLocal::seqno_to_time_mapping_")
  void* cfd_ret = ColumnFamilyData::UnPackLocal(node);
  LOG_CERR("RemoteFlushJob::UnPackLocal::cfd_");
  size_t base_version_flag = 0;
  node->receive(&base_version_flag, sizeof(size_t));
  void* base_version_ret = nullptr;
  if (base_version_flag == 0) {
    LOG_CERR("RemoteFlushJob::UnPackLocal::base_");
    base_version_ret = Version::UnPackLocal(node, cfd_ret);
  }
  LOG_CERR("RemoteFlushJob::UnPackLocal::base_::1");
  void* meta_ret[4] = {nullptr, nullptr, nullptr, nullptr};
  for (int i = 0; i < 4; i++) {
    LOG_CERR("RemoteFlushJob::UnPackLocal::meta_", i);
    meta_ret[i] = FileMetaData::UnPackLocal(node);
  }
  LOG_CERR("RemoteFlushJob::UnPackLocal::meta_::p");
  void* file_options_ret = FileOptions::UnPackLocal(node);
  // void* mutable_cf_options_ret =
  //     MutableCFOptions::UnPackLocal(worker_socket_fd);
  size_t dbname_len = 0;
  node->receive(&dbname_len, sizeof(size_t));
  std::string local_dbname;
  local_dbname.resize(dbname_len);
  if (local_dbname.size())
    node->receive(local_dbname.data(), local_dbname.size());

  size_t ts_len = 0;
  node->receive(&ts_len, sizeof(size_t));
  std::string local_ts_low;
  local_ts_low.resize(ts_len);
  if (local_ts_low.size())
    node->receive(local_ts_low.data(), local_ts_low.size());

  size_t db_id_len = 0;
  node->receive(&db_id_len, sizeof(size_t));
  std::string local_db_id;
  local_db_id.resize(db_id_len);
  if (local_db_id.size()) node->receive(local_db_id.data(), local_db_id.size());

  size_t db_session_id_len = 0;
  node->receive(&db_session_id_len, sizeof(size_t));
  std::string local_db_session_id;
  local_db_session_id.resize(db_session_id_len);
  if (local_db_session_id.size())
    node->receive(local_db_session_id.data(), local_db_session_id.size());

  size_t existing_snapshots_len = 0;
  node->receive(&existing_snapshots_len, sizeof(size_t));
  std::vector<SequenceNumber> existing_snapshots;
  existing_snapshots.resize(existing_snapshots_len);
  if (existing_snapshots_len) {
    for (size_t i = 0; i < existing_snapshots_len; i++) {
      node->receive(&existing_snapshots[i], sizeof(SequenceNumber));
    }
  }

  auto* local_snapshot_checker = reinterpret_cast<SnapshotChecker*>(
      SnapshotCheckerFactory::UnPackLocal(node));

  node->receive(mem, sizeof(RemoteFlushJob));
  LOG("worker recv ", std::hex, mem, std::dec);

  // fill local_handler
  local_handler->remote_db_ = remote_db;
  local_handler->clock_ = reinterpret_cast<SystemClock*>(clock_ret_addr);
  local_handler->db_mutex_ = new InstrumentedMutex();
  // local_handler->db_mutex_->Unlock();
  new (&local_handler->mems_) autovector<MemTable*>();
  local_handler->job_context_ = reinterpret_cast<JobContext*>(job_context_ret);
  local_handler->output_file_directory_ = nullptr;

  // local_handler->output_file_directory_ =
  //     reinterpret_cast<FSDirectory*>(output_file_directory_ret);
  // assert(local_handler->sync_output_directory_);
  // local_handler->db_directory_ =
  //     reinterpret_cast<FSDirectory*>(db_directory_ret);

  local_handler->db_directory_ = nullptr;
  LOG("local_handler copy FileMetaData");
  for (int i = 0; i < 4; i++) {
    new (&(local_handler->meta_[i]))
        FileMetaData(*reinterpret_cast<FileMetaData*>(meta_ret[i]));
    free(meta_ret[i]);
  }
  for (auto& memtable : temp_mems) {
    LOG("worker copy data: ", memtable);
    local_handler->mems_.emplace_back(memtable);
  }
  char* ptr = reinterpret_cast<char*>(&local_handler->seqno_to_time_mapping_);
  ptr -= sizeof(SeqnoToTimeMapping*);
  memcpy(reinterpret_cast<void*>(ptr),
         reinterpret_cast<void*>(&db_impl_seqno_time_mapping_ret),
         sizeof(SeqnoToTimeMapping*));
  memcpy(reinterpret_cast<void*>(const_cast<SeqnoToTimeMapping*>(
             &(local_handler->seqno_to_time_mapping_))),
         reinterpret_cast<void*>(seqno_to_time_mapping_ret),
         sizeof(SeqnoToTimeMapping));
  free(seqno_to_time_mapping_ret);

  local_handler->cfd_ = reinterpret_cast<ColumnFamilyData*>(cfd_ret);
  local_handler->base_ = base_version_flag == 1
                             ? local_handler->cfd_->current()
                             : reinterpret_cast<Version*>(base_version_ret);
  local_handler->versions_ = reinterpret_cast<VersionSet*>(versions_ret);
  local_handler->edit_ = reinterpret_cast<VersionEdit*>(edit_ret);
  new (const_cast<std::string*>(&local_handler->full_history_ts_low_))
      std::string(local_ts_low);
  new (const_cast<std::string*>(&local_handler->db_id_))
      std::string(local_db_id);
  new (const_cast<std::string*>(&local_handler->db_session_id_))
      std::string(local_db_session_id);
  new (const_cast<std::string*>(&local_handler->dbname_))
      std::string(local_dbname);
  new (const_cast<FileOptions*>(&local_handler->file_options_))
      FileOptions(*reinterpret_cast<FileOptions*>(file_options_ret));
  delete reinterpret_cast<FileOptions*>(file_options_ret);
  new (&local_handler->existing_snapshots_) std::vector<SequenceNumber>();
  for (auto& snapshot : existing_snapshots) {
    local_handler->existing_snapshots_.emplace_back(snapshot);
  }
  // table_properties_ construct local empty version
  for (int i = 0; i < 4; i++)
    new (&local_handler->table_properties_[i]) TableProperties();
  char* hack_dboption_ptr = reinterpret_cast<char*>(&local_handler->cfd_);
  hack_dboption_ptr += sizeof(ColumnFamilyData*);
  void* local_db_options = const_cast<void*>(
      reinterpret_cast<const void*>(local_handler->versions_->db_options()));
  memcpy(reinterpret_cast<void*>(hack_dboption_ptr),
         reinterpret_cast<void*>(&local_db_options),
         sizeof(ImmutableDBOptions*));
  hack_dboption_ptr = reinterpret_cast<char*>(&local_handler->max_memtable_id_);
  hack_dboption_ptr -= sizeof(MutableCFOptions*);
  auto* mptr = local_handler->cfd_->GetLatestMutableCFOptions();
  memcpy(reinterpret_cast<void*>(hack_dboption_ptr),
         reinterpret_cast<const void*>(&mptr), sizeof(MutableCFOptions*));
  local_handler->stats_ = local_handler->cfd_->ioptions()->stats;
  LOG("local_handler copy done");
  // new (const_cast<MutableCFOptions*>(&local_handler->mutable_cf_options_))
  //     MutableCFOptions(*local_handler->cfd_->GetLatestMutableCFOptions());
  LOG("RemoteFlushJob::UnPackLocal done");
  return mem;
}

void RemoteFlushJob::PackRemote(TransferService* node) const {
  LOG("RemoteFlushJob::PackRemote thread_id:", std::this_thread::get_id(),
      "Pack data from remote side");
  assert(mems_.size() > 0);
  mems_[0]->PackRemote(node);
  versions_->PackRemote(node);
  for (int i = 0; i < 4; i++) table_properties_[i].PackRemote(node);
  edit_->PackRemote(node);
  for (int i = 0; i < 4; i++) meta_[i].PackRemote(node);
  edit_->free_remote();
  cfd_->free_remote();
  for (int i = 0; i < 4; i++) table_properties_[i].~TableProperties();
  versions_->free_remote();
  // delete db_mutex_;
  const_cast<std::string*>(&db_id_)->~basic_string();
  const_cast<std::string*>(&db_session_id_)->~basic_string();
  const_cast<std::string*>(&dbname_)->~basic_string();
  const_cast<std::vector<SequenceNumber>*>(&existing_snapshots_)->clear();
  const_cast<std::string*>(&full_history_ts_low_)->~basic_string();
  const_cast<FileOptions*>(&file_options_)->~FileOptions();
  free(cfd_);
  free(versions_);
  free(job_context_);
  free(edit_);
  char* ptr = reinterpret_cast<char*>(
      const_cast<SeqnoToTimeMapping*>(&seqno_to_time_mapping_));
  ptr -= sizeof(SeqnoToTimeMapping*);
  free(*reinterpret_cast<void**>(ptr));
  LOG("RemoteFlushJob::PackRemote done");
}
void RemoteFlushJob::UnPackRemote(TransferService* node) {
  assert(mems_.size() > 0);
  mems_[0]->UnPackRemote(node);
  db_mutex_->Lock();
  VersionSet::UnPackRemote(node, versions_);
  for (int i = 0; i < 4; i++) {
    auto new_table_properties =
        reinterpret_cast<TableProperties*>(TableProperties::UnPackRemote(node));
    table_properties_[i] = *new_table_properties;
    delete new_table_properties;
  }
  db_mutex_->Unlock();
  edit_->UnPackRemote(node);
  for (int i = 0; i < 4; i++) {
    auto remote_metadata =
        reinterpret_cast<FileMetaData*>(FileMetaData::UnPackRemote(node));

    meta_[i].fd.file_size = remote_metadata->fd.file_size;
    meta_[i].fd.smallest_seqno = remote_metadata->fd.smallest_seqno;
    meta_[i].fd.largest_seqno = remote_metadata->fd.largest_seqno;
    meta_[i].smallest = remote_metadata->smallest;
    meta_[i].largest = remote_metadata->largest;
    meta_[i].epoch_number = remote_metadata->epoch_number;
    assert(meta_[i].smallest.Valid());
    assert(meta_[i].largest.Valid());
    // meta_.oldest_blob_file_number = remote_metadata->oldest_blob_file_number;
    meta_[i].compensated_range_deletion_size =
        remote_metadata->compensated_range_deletion_size;
    meta_[i].file_creation_time = remote_metadata->file_creation_time;
    meta_[i].marked_for_compaction = remote_metadata->marked_for_compaction;
    meta_[i].file_checksum = remote_metadata->file_checksum;
    meta_[i].file_checksum_func_name = remote_metadata->file_checksum_func_name;
    meta_[i].unique_id = remote_metadata->unique_id;
    meta_[i].oldest_ancester_time = remote_metadata->oldest_ancester_time;
    free(remote_metadata);
    std::unique_ptr<InternalIterator> it(cfd_->table_cache()->NewIterator(
        ReadOptions(), file_options_, cfd_->internal_comparator(), meta_[i],
        nullptr /* range_del_agg */, mutable_cf_options_.prefix_extractor,
        nullptr,
        (cfd_->internal_stats() == nullptr)
            ? nullptr
            : cfd_->internal_stats()->GetFileReadHist(0),
        TableReaderCaller::kFlush, /*arena=*/nullptr,
        /*skip_filter=*/false, 0, MaxFileSizeForL0MetaPin(mutable_cf_options_),
        /*smallest_compaction_key=*/nullptr,
        /*largest_compaction_key*/ nullptr,
        /*allow_unprepared_value*/ false));
  }
  // close connection with remote worker
  db_mutex_->Lock();
  base_->Unref();
  LOG("RemoteFlushJob::UnPackRemote done");
}

std::shared_ptr<RemoteFlushJob> RemoteFlushJob::CreateRemoteFlushJob(
    const std::string& dbname, ColumnFamilyData* cfd,
    const ImmutableDBOptions& db_options,
    const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
    const FileOptions& file_options, VersionSet* versions,
    InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, JobContext* job_context,
    FlushReason flush_reason, LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_file_directory, CompressionType output_compression,
    Statistics* stats, EventLogger* event_logger, bool measure_io_stats,
    const bool sync_output_directory, const bool write_manifest,
    const std::shared_ptr<IOTracer>& io_tracer,
    const SeqnoToTimeMapping& seqno_time_mapping,
#ifdef ROCKSDB_RDMA
    RDMAClient* rdma_client,
#endif
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low,
    BlobFileCompletionCallback* blob_callback) {
  auto mem = new RemoteFlushJob(
      dbname, cfd, db_options, mutable_cf_options, max_memtable_id,
      file_options, versions, db_mutex, shutting_down,
      std::move(existing_snapshots), earliest_write_conflict_snapshot,
      snapshot_checker, job_context, flush_reason, log_buffer, db_directory,
      output_file_directory, output_compression, stats, event_logger,
      measure_io_stats, sync_output_directory, write_manifest, io_tracer,
      seqno_time_mapping,
#ifdef ROCKSDB_RDMA
      rdma_client,
#endif
      db_id, db_session_id, std::move(full_history_ts_low), blob_callback);
  return std::shared_ptr<RemoteFlushJob>(mem);
}
RemoteFlushJob::~RemoteFlushJob() { ThreadStatusUtil::ResetThreadStatus(); }

void RemoteFlushJob::ReportStartedFlush() {
  ThreadStatusUtil::SetColumnFamily(cfd_, cfd_->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_context_->job_id);
  IOSTATS_RESET(bytes_written);
}

void RemoteFlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
}

void RemoteFlushJob::RecordFlushIOStats() {
  RecordTick(stats_, FLUSH_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

void RemoteFlushJob::PickMemTable() {
  db_mutex_->AssertHeld();
  assert(!pick_memtable_called);
  pick_memtable_called = true;

  // Maximum "NextLogNumber" of the memtables to flush.
  // When mempurge feature is turned off, this variable is useless
  // because the memtables are implicitly sorted by increasing order of
  // creation time. Therefore mems_->back()->GetNextLogNumber() is already
  // equal to max_next_log_number. However when Mempurge is on, the
  // memtables are no longer sorted by increasing order of creation time.
  // Therefore this variable becomes necessary because
  // mems_->back()->GetNextLogNumber() is no longer necessarily equal to
  // max_next_log_number.
  uint64_t max_next_log_number = 0;

  // Save the contents of the earliest memtable as a new Table
  cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_,
                                    &max_next_log_number);
  if (mems_.empty()) {
    return;
  }

  ReportFlushInputSize(mems_);

  // entries mems are (implicitly) sorted in ascending order by their
  // created time. We will use the first memtable's `edit` to keep the meta
  // info for this flush.
  MemTable* m = mems_[0];
  edit_ = m->GetEdits();
  edit_->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit_->SetLogNumber(max_next_log_number);
  edit_->SetColumnFamily(cfd_->GetID());

  // path 0 for level 0 file.
  for (int i = 0; i < 4; i++)
    meta_[i].fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  // uint64_t new_epoch = cfd_->NewEpochNumber();
  for (int i = 0; i < 4; i++) meta_[i].epoch_number = cfd_->NewEpochNumber();

  base_ = cfd_->current();
  base_->Ref();  // it is likely that we do not need this reference
}

Status RemoteFlushJob::RunRemote(
    std::vector<std::pair<std::string, size_t>>* memnodes_,
    std::function<int()>* get_available_port, const std::string& local_ip,
    LogsWithPrepTracker* prep_tracker, FileMetaData* file_meta,
    bool* switched_to_mempurge) {
  uint64_t start = Env::Default()->NowMicros();
  TEST_SYNC_POINT("RemoteFlushJob::Start");
  db_mutex_->AssertHeld();
  assert(pick_memtable_called);
  // Mempurge threshold can be dynamically changed.
  // For sake of consistency, mempurge_threshold is
  // saved locally to maintain consistency in each
  // RemoteFlushJob::Run call.
  double mempurge_threshold =
      mutable_cf_options_.experimental_mempurge_threshold;

  AutoThreadOperationStageUpdater stage_run(
      ThreadStatus::STAGE_REMOTE_FLUSH_RUN);
  if (mems_.empty()) return Status::OK();

  Status mempurge_s = Status::NotFound("No MemPurge.");
  if ((mempurge_threshold > 0.0) &&
      (flush_reason_ == FlushReason::kWriteBufferFull) && (!mems_.empty()) &&
      MemPurgeDecider(mempurge_threshold) && !(db_options_.atomic_flush)) {
    cfd_->SetMempurgeUsed();
    mempurge_s = MemPurge();
    if (!mempurge_s.ok()) {
      // Mempurge is typically aborted when the output
      // bytes cannot be contained onto a single output memtable.
      if (mempurge_s.IsAborted()) {
        ROCKS_LOG_INFO(db_options_.info_log, "Mempurge process aborted: %s\n",
                       mempurge_s.ToString().c_str());
      } else {
        // However the mempurge process can also fail for
        // other reasons (eg: new_mem->Add() fails).
        ROCKS_LOG_WARN(db_options_.info_log, "Mempurge process failed: %s\n",
                       mempurge_s.ToString().c_str());
      }
    } else {
      if (switched_to_mempurge) {
        *switched_to_mempurge = true;
      } else {
        // The mempurge process was successful, but no switch_to_mempurge
        // pointer provided so no way to propagate the state of flush job.
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Mempurge process succeeded"
                       "but no 'switched_to_mempurge' ptr provided.\n");
      }
    }
  }
  Status s;
  if (mempurge_s.ok()) {
    base_->Unref();
    s = Status::OK();
  } else {
    // This will release and re-acquire the mutex.
    LOG("Run job: write l0table");
    LOG(edit_->DebugString());
    LOG(meta_.DebugString());
    LOG(table_properties_.ToString());
    db_mutex_->Unlock();
    // We create connection with one memnode and send package to it
    // assert(MatchMemNode(smemnodes_) == Status::OK());  // todo: false
    // tolerant
    std::chrono::high_resolution_clock::time_point f1 =
        std::chrono::high_resolution_clock::now();
    void* tmp_data = local_generator_rdma_client->get_buf() +
                     cfd_->get_built_memreg_info()->rf_meta_local_offset;
    size_t buf_size =
        cfd_->get_built_memreg_info()->rf_meta_remote_offset.second -
        cfd_->get_built_memreg_info()->rf_meta_remote_offset.first;
    BufTransferService transfer_service(tmp_data, buf_size);

    size_t mem_size = mems_.size();
    transfer_service.send(&mem_size, sizeof(size_t));
    for (auto& memtable : mems_) {
      uint64_t mixed_id = ((uint64_t)cfd_->GetID() << 32) | memtable->GetID();
      transfer_service.send(&mixed_id, sizeof(uint64_t));
      if (!memtable->IsTransferCalled())
        cfd_->register_imm_trans(memtable, true);
    }

    for (auto& memtable : mems_)
      memtable->PackLocal(&transfer_service,
                          ((uint64_t)cfd_->GetID() << 32) | memtable->GetID());
    PackLocal(&transfer_service);

    int port = (*get_available_port)();
    transfer_service.send(&port, sizeof(int));
    size_t ip_size = local_ip.size();
    transfer_service.send(&ip_size, sizeof(size_t));
    if (ip_size > 0) transfer_service.send(local_ip.c_str(), local_ip.size());

    // note: exceed buf size might cause free(): invalid pointer or double free
    // error assert(transfer_service.get_size() < buf_size);  // 39775/46065 for
    // now
    ASSERT_RW(MatchMemNode(memnodes_) == Status::OK());
    std::chrono::high_resolution_clock::time_point f2 =
        std::chrono::high_resolution_clock::now();

    local_generator_rdma_client->rdma_write(
        rdma_conn, buf_size,
        cfd_->get_built_memreg_info()->rf_meta_local_offset,
        cfd_->get_built_memreg_info()->rf_meta_remote_offset.first);
    ASSERT_RW(local_generator_rdma_client->poll_completion(rdma_conn) == 0);
    {
      char req_type = 8;
      ASSERT_RW(writen(rdma_conn->sock, &req_type, sizeof(char)) ==
                sizeof(char));
      ASSERT_RW(readn(rdma_conn->sock, &req_type, sizeof(char)) ==
                sizeof(char));
    }
    cfd_->putback_meta_conn(rdma_conn);

    // close connection with memnode
    ASSERT_RW(QuitMemNode().ok());
    // s = WriteLevel0Table();
    // assert(s == Status::OK());
    std::chrono::high_resolution_clock::time_point f3 =
        std::chrono::high_resolution_clock::now();

    // We receive some metadata directly from remote worker
    // Or maybe we could receive from memnode
    ASSERT_RW(MatchRemoteWorker(port) == Status::OK());
    TCPTransferService transfer_service2(&local_generator_node);
    UnPackRemote(&transfer_service2);
    std::chrono::high_resolution_clock::time_point f4 =
        std::chrono::high_resolution_clock::now();
    LOG_CERR(
        ":::::::: ",
        std::chrono::duration_cast<std::chrono::microseconds>(f2 - f1).count(),
        " ",
        std::chrono::duration_cast<std::chrono::microseconds>(f3 - f2).count(),
        " ",
        std::chrono::duration_cast<std::chrono::microseconds>(f4 - f3).count(),
        " ",
        std::chrono::duration_cast<std::chrono::microseconds>(f4 - f1).count());
    // s = Status::OK();
    ASSERT_RW(QuitRemoteWorker() == Status::OK());
    LOG(edit_->DebugString());
    LOG(meta_.DebugString());
    LOG(table_properties_.ToString());
    assert(stats_ == cfd_->ioptions()->stats);
  }
  db_mutex_->AssertHeld();
  if (s.ok() && cfd_->IsDropped()) {
    s = Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((s.ok() || s.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_acquire)) {
    s = Status::ShutdownInProgress("Database shutdown");
  }

  if (!s.ok()) {
    LOG("Run job: write l0table failed, rollback");
    for (int i = 0; i < 4; i++)
      cfd_->imm()->RollbackMemtableFlush(mems_, meta_[i].fd.GetNumber());
  } else if (write_manifest_) {
    LOG("Run job: write l0table success, install results");
    TEST_SYNC_POINT("RemoteFlushJob::InstallResults");
    // Replace immutable memtable with the generated Table
    uint64_t fdnum[4] = {0, 0, 0, 0};
    for (int i = 0; i < 4; i++) fdnum[i] = meta_[i].fd.GetNumber();
    s = cfd_->imm()->TryInstallMemtableFlushResults(
        cfd_, mutable_cf_options_, mems_, prep_tracker, versions_, db_mutex_,
         &job_context_->memtables_to_free, db_directory_,
        nullptr, &committed_flush_jobs_info_,
        fdnum,!(mempurge_s.ok()) /* write_edit : true if no mempurge happened (or if aborted),
                              but 'false' if mempurge successful: no new min log number
                              or new level 0 file path to write to manifest. */);
    LOG("Run job: write l0table success, install results done");
  }

  if (s.ok() && file_meta != nullptr) {
    for (int i = 0; i < 4; i++) file_meta[i] = meta_[i];
  }
  RecordFlushIOStats();
  uint64_t end = Env::Default()->NowMicros();
  uint64_t elapsed_micros = end - start;
  Singleton<rf_stats>::GetInstance()->ReportFlush(start, end, elapsed_micros);
  return s;
}

Status RemoteFlushJob::MatchRemoteWorker(int port) {
  // local_generator_node.~TCPNode();
  memset(reinterpret_cast<void*>(&local_generator_node), 0, sizeof(TCPNode));
  if ((local_generator_node.connection_info_.listen_sockfd =
           socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    LOG("Socket creation error");
    return Status::IOError("Socket creation error");
  }
  int opt = ~SOCK_NONBLOCK;
  if (setsockopt(local_generator_node.connection_info_.listen_sockfd,
                 SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
    LOG("setsockopt failed");
    return Status::IOError("setsockopt");
  }
  local_generator_node.connection_info_.sin_addr.sin_family = AF_INET;
  local_generator_node.connection_info_.sin_addr.sin_port = htons(port);
  if (bind(local_generator_node.connection_info_.listen_sockfd,
           (struct sockaddr*)&local_generator_node.connection_info_.sin_addr,
           sizeof(local_generator_node.connection_info_.sin_addr)) < 0) {
    LOG("bind failed");
    fprintf(stderr, "bind failed with port %d\n", port);
    return Status::IOError("bind failed");
  }
  if (listen(local_generator_node.connection_info_.listen_sockfd, 1) < 0) {
    LOG("listen failed");
    return Status::IOError("listen failed");
  }
  socklen_t len = sizeof(local_generator_node.connection_info_.sin_addr);
  if ((local_generator_node.connection_info_.client_sockfd = accept(
           local_generator_node.connection_info_.listen_sockfd,
           (struct sockaddr*)&local_generator_node.connection_info_.sin_addr,
           &len)) < 0) {
    LOG("accept failed");
    return Status::IOError("accept failed");
  }
  close(local_generator_node.connection_info_.listen_sockfd);
  local_generator_node.connection_info_.listen_sockfd = 0;
  LOG("create connection with remote flush worker, ready to receive data");
  return Status::OK();
}

Status RemoteFlushJob::QuitMemNode() { return Status::OK(); }
Status RemoteFlushJob::MatchMemNode(
    std::vector<std::pair<std::string, size_t>>* ip_port_list) {
  while (true) {
    rdma_conn = cfd_->try_get_meta_conn();
    if (rdma_conn) break;
  }
  return Status::OK();
}

Status RemoteFlushJob::QuitRemoteWorker() {
  close(local_generator_node.connection_info_.client_sockfd);
  local_generator_node.connection_info_.client_sockfd = 0;
  return Status::OK();
}

Status RemoteFlushJob::RunLocal(LogsWithPrepTracker* prep_tracker,
                                FileMetaData* file_meta,
                                bool* switched_to_mempurge) {
  LOG("RemoteFlushJob::RunLocal thread_id:", std::this_thread::get_id(),
      " RemoteFlushJob ptr: ", std::hex, reinterpret_cast<void*>(this),
      std::dec);
  LOG("worker calculation: ");
  std::chrono::high_resolution_clock::time_point local_flush_begin =
      std::chrono::high_resolution_clock::now();
  db_mutex_->Lock();
  std::mutex thr_mu;
  std::vector<std::thread> thrs;
  thrs.reserve(4);
  for (int i = 0; i < 4; i++) {
    thrs.emplace_back([this, i, &thr_mu]() {
      Status ret = WriteLevel0Table(i, &thr_mu);
      if (!ret.ok()) {
        LOG_CERR("WriteLevel0Table failed: ", ret.ToString());
      }
    });
  }
  for (auto& thr : thrs) {
    thr.join();
  }
  std::chrono::high_resolution_clock::time_point local_flush_end =
      std::chrono::high_resolution_clock::now();
  LOG_CERR("RemoteFlushJob::RunLocal local_flush_all_time: ",
           std::chrono::duration_cast<std::chrono::milliseconds>(
               local_flush_end - local_flush_begin)
               .count(),
           "ms");
  LOG("worker calculation finished");
  return Status::OK();
}

void RemoteFlushJob::Cancel() {
  db_mutex_->AssertHeld();
  assert(base_ != nullptr);
  base_->Unref();
}

Status RemoteFlushJob::MemPurge() {
  Status s;
  db_mutex_->AssertHeld();
  db_mutex_->Unlock();
  assert(!mems_.empty());

  // Measure purging time.
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();

  MemTable* new_mem = nullptr;
  // For performance/log investigation purposes:
  // look at how much useful payload we harvest in the new_mem.
  // This value is then printed to the DB log.
  double new_mem_capacity = 0.0;

  // Create two iterators, one for the memtable data (contains
  // info from puts + deletes), and one for the memtable
  // Range Tombstones (from DeleteRanges).
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  std::vector<InternalIterator*> memtables;
  std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
      range_del_iters;
  for (MemTable* m : mems_) {
    memtables.push_back(m->NewIterator(ro, &arena));
    auto* range_del_iter = m->NewRangeTombstoneIterator(
        ro, kMaxSequenceNumber, true /* immutable_memtable */);
    if (range_del_iter != nullptr) {
      range_del_iters.emplace_back(range_del_iter);
    }
  }

  assert(!memtables.empty());
  SequenceNumber first_seqno = kMaxSequenceNumber;
  SequenceNumber earliest_seqno = kMaxSequenceNumber;
  // Pick first and earliest seqno as min of all first_seqno
  // and earliest_seqno of the mempurged memtables.
  for (const auto& mem : mems_) {
    first_seqno = mem->GetFirstSequenceNumber() < first_seqno
                      ? mem->GetFirstSequenceNumber()
                      : first_seqno;
    earliest_seqno = mem->GetEarliestSequenceNumber() < earliest_seqno
                         ? mem->GetEarliestSequenceNumber()
                         : earliest_seqno;
  }

  ScopedArenaIterator iter(
      NewMergingIterator(&(cfd_->internal_comparator()), memtables.data(),
                         static_cast<int>(memtables.size()), &arena));

  auto* ioptions = cfd_->ioptions();

  // Place iterator at the First (meaning most recent) key node.
  iter->SeekToFirst();

  const std::string* const full_history_ts_low = &(cfd_->GetFullHistoryTsLow());
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&(cfd_->internal_comparator()),
                                       existing_snapshots_,
                                       full_history_ts_low));
  for (auto& rd_iter : range_del_iters) {
    range_del_agg->AddTombstones(std::move(rd_iter));
  }

  // If there is valid data in the memtable,
  // or at least range tombstones, copy over the info
  // to the new memtable.
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    // MaxSize is the size of a memtable.
    size_t maxSize = mutable_cf_options_.write_buffer_size;
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions->compaction_filter_factory != nullptr &&
        ioptions->compaction_filter_factory->ShouldFilterTableFileCreation(
            TableFileCreationReason::kFlush)) {
      CompactionFilter::Context ctx;
      ctx.is_full_compaction = false;
      ctx.is_manual_compaction = false;
      ctx.column_family_id = cfd_->GetID();
      ctx.reason = TableFileCreationReason::kFlush;
      compaction_filter =
          ioptions->compaction_filter_factory->CreateCompactionFilter(ctx);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s = Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
        return s;
      }
    }

    new_mem = new MemTable((cfd_->internal_comparator()), *(cfd_->ioptions()),
                           mutable_cf_options_, cfd_->write_buffer_mgr(),
                           earliest_seqno, cfd_->GetID());
    assert(new_mem != nullptr);

    Env* env = db_options_.env;
    assert(env);
    MergeHelper merge(
        env, (cfd_->internal_comparator()).user_comparator(),
        (ioptions->merge_operator).get(), compaction_filter.get(),
        ioptions->logger, true /* internal key corruption is not ok */,
        existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
        snapshot_checker_);
    assert(job_context_);
    SequenceNumber job_snapshot_seq = job_context_->GetJobSnapshotSequence();
    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter.get(), (cfd_->internal_comparator()).user_comparator(), &merge,
        kMaxSequenceNumber, &existing_snapshots_,
        earliest_write_conflict_snapshot_, job_snapshot_seq, snapshot_checker_,
        env, ShouldReportDetailedTime(env, ioptions->stats),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        nullptr, ioptions->allow_data_in_errors,
        ioptions->enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, ioptions->info_log, full_history_ts_low);

    // Set earliest sequence number in the new memtable
    // to be equal to the earliest sequence number of the
    // memtable being flushed (See later if there is a need
    // to update this number!).
    new_mem->SetEarliestSequenceNumber(earliest_seqno);
    // Likewise for first seq number.
    new_mem->SetFirstSequenceNumber(first_seqno);
    SequenceNumber new_first_seqno = kMaxSequenceNumber;

    c_iter.SeekToFirst();

    // Key transfer
    for (; c_iter.Valid(); c_iter.Next()) {
      const ParsedInternalKey ikey = c_iter.ikey();
      const Slice value = c_iter.value();
      new_first_seqno =
          ikey.sequence < new_first_seqno ? ikey.sequence : new_first_seqno;

      // Should we update "OldestKeyTime" ???? -> timestamp appear
      // to still be an "experimental" feature.
      s = new_mem->Add(
          ikey.sequence, ikey.type, ikey.user_key, value,
          nullptr,   // KV protection info set as nullptr since it
                     // should only be useful for the first add to
                     // the original memtable.
          false,     // : allow concurrent_memtable_writes_
                     // Not seen as necessary for now.
          nullptr,   // get_post_process_info(m) must be nullptr
                     // when concurrent_memtable_writes is switched off.
          nullptr);  // hint, only used when concurrent_memtable_writes_
                     // is switched on.
      if (!s.ok()) {
        break;
      }

      // If new_mem has size greater than maxSize,
      // then rollback to regular flush operation,
      // and destroy new_mem.
      if (new_mem->ApproximateMemoryUsage() > maxSize) {
        s = Status::Aborted("Mempurge filled more than one memtable.");
        new_mem_capacity = 1.0;
        break;
      }
    }

    // Check status and propagate
    // potential error status from c_iter
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    // Range tombstone transfer.
    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        new_first_seqno =
            tombstone.seq_ < new_first_seqno ? tombstone.seq_ : new_first_seqno;
        s = new_mem->Add(
            tombstone.seq_,        // Sequence number
            kTypeRangeDeletion,    // KV type
            tombstone.start_key_,  // Key is start key.
            tombstone.end_key_,    // Value is end key.
            nullptr,               // KV protection info set as nullptr since it
                                   // should only be useful for the first add to
                                   // the original memtable.
            false,                 // : allow concurrent_memtable_writes_
                                   // Not seen as necessary for now.
            nullptr,               // get_post_process_info(m) must be nullptr
                      // when concurrent_memtable_writes is switched off.
            nullptr);  // hint, only used when concurrent_memtable_writes_
                       // is switched on.

        if (!s.ok()) {
          break;
        }

        // If new_mem has size greater than maxSize,
        // then rollback to regular flush operation,
        // and destroy new_mem.
        if (new_mem->ApproximateMemoryUsage() > maxSize) {
          s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
          new_mem_capacity = 1.0;
          break;
        }
      }
    }

    // If everything happened smoothly and new_mem contains valid data,
    // decide if it is flushed to storage or kept in the imm()
    // memtable list (memory).
    if (s.ok() && (new_first_seqno != kMaxSequenceNumber)) {
      // Rectify the first sequence number, which (unlike the earliest seq
      // number) needs to be present in the new memtable.
      new_mem->SetFirstSequenceNumber(new_first_seqno);

      // The new_mem is added to the list of immutable memtables
      // only if it filled at less than 100% capacity and isn't flagged
      // as in need of being flushed.
      if (new_mem->ApproximateMemoryUsage() < maxSize &&
          !(new_mem->ShouldFlushNow())) {
        // Construct fragmented memtable range tombstones without mutex
        new_mem->ConstructFragmentedRangeTombstones();
        db_mutex_->Lock();
        uint64_t new_mem_id = mems_[0]->GetID();

        new_mem->SetID(new_mem_id);
        new_mem->SetNextLogNumber(mems_[0]->GetNextLogNumber());

        // This addition will not trigger another flush, because
        // we do not call SchedulePendingFlush().
        cfd_->imm()->Add(new_mem, &job_context_->memtables_to_free);
        new_mem->Ref();
        // Piggyback RemoteFlushJobInfo on the first flushed memtable.
        db_mutex_->AssertHeld();
        for (int i = 0; i < 4; i++) meta_[i].fd.file_size = 0;
        mems_[0]->SetFlushJobInfo(GetFlushJobInfo(0));
        db_mutex_->Unlock();
      } else {
        s = Status::Aborted(Slice("Mempurge filled more than one memtable."));
        new_mem_capacity = 1.0;
        if (new_mem) {
          job_context_->memtables_to_free.push_back(new_mem);
        }
      }
    } else {
      // In this case, the newly allocated new_mem is empty.
      assert(new_mem != nullptr);
      job_context_->memtables_to_free.push_back(new_mem);
    }
  }

  // Reacquire the mutex for WriteLevel0 function.
  db_mutex_->Lock();

  // If mempurge successful, don't write input tables to level0,
  // but write any full output table to level0.
  if (s.ok()) {
    TEST_SYNC_POINT("DBImpl::RemoteFlushJob:MemPurgeSuccessful");
  } else {
    TEST_SYNC_POINT("DBImpl::RemoteFlushJob:MemPurgeUnsuccessful");
  }
  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Mempurge lasted %" PRIu64
                 " microseconds, and %" PRIu64
                 " cpu "
                 "microseconds. Status is %s ok. Perc capacity: %f\n",
                 cfd_->GetName().c_str(), job_context_->job_id, micros,
                 cpu_micros, s.ok() ? "" : "not", new_mem_capacity);

  return s;
}

bool RemoteFlushJob::MemPurgeDecider(double threshold) {
  // Never trigger mempurge if threshold is not a strictly positive value.
  if (!(threshold > 0.0)) {
    return false;
  }
  if (threshold > (1.0 * mems_.size())) {
    return true;
  }
  // Payload and useful_payload (in bytes).
  // The useful payload ratio of a given MemTable
  // is estimated to be useful_payload/payload.
  uint64_t payload = 0, useful_payload = 0, entry_size = 0;

  // Local variables used repetitively inside the for-loop
  // when iterating over the sampled entries.
  Slice key_slice, value_slice;
  ParsedInternalKey res;
  SnapshotImpl min_snapshot;
  std::string vget;
  Status mget_s, parse_s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0, sqno = 0,
                 min_seqno_snapshot = 0;
  bool get_res, can_be_useful_payload, not_in_next_mems;

  // If estimated_useful_payload is > threshold,
  // then flush to storage, else MemPurge.
  double estimated_useful_payload = 0.0;
  // Cochran formula for determining sample size.
  // 95% confidence interval, 7% precision.
  //    n0 = (1.96*1.96)*0.25/(0.07*0.07) = 196.0
  double n0 = 196.0;
  ReadOptions ro;
  ro.total_order_seek = true;

  // Iterate over each memtable of the set.
  for (auto mem_iter = std::begin(mems_); mem_iter != std::end(mems_);
       mem_iter++) {
    MemTable* mt = *mem_iter;

    // Else sample from the table.
    uint64_t nentries = mt->num_entries();
    // Corrected Cochran formula for small populations
    // (converges to n0 for large populations).
    uint64_t target_sample_size =
        static_cast<uint64_t>(ceil(n0 / (1.0 + (n0 / nentries))));
    std::unordered_set<const char*> sentries = {};
    // Populate sample entries set.
    mt->UniqueRandomSample(target_sample_size, &sentries);

    // Estimate the garbage ratio by comparing if
    // each sample corresponds to a valid entry.
    for (const char* ss : sentries) {
      key_slice = GetLengthPrefixedSlice(ss);
      parse_s = ParseInternalKey(key_slice, &res, true /*log_err_key*/);
      if (!parse_s.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Memtable Decider: ParseInternalKey did not parse "
                       "key_slice %s successfully.",
                       key_slice.data());
      }

      // Size of the entry is "key size (+ value size if KV entry)"
      entry_size = key_slice.size();
      if (res.type == kTypeValue) {
        value_slice =
            GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        entry_size += value_slice.size();
      }

      // Count entry bytes as payload.
      payload += entry_size;

      LookupKey lkey(res.user_key, kMaxSequenceNumber);

      // Paranoia: zero out these values just in case.
      max_covering_tombstone_seq = 0;
      sqno = 0;

      // Pick the oldest existing snapshot that is more recent
      // than the sequence number of the sampled entry.
      min_seqno_snapshot = kMaxSequenceNumber;
      for (SequenceNumber seq_num : existing_snapshots_) {
        if (seq_num > res.sequence && seq_num < min_seqno_snapshot) {
          min_seqno_snapshot = seq_num;
        }
      }
      min_snapshot.number_ = min_seqno_snapshot;
      ro.snapshot =
          min_seqno_snapshot < kMaxSequenceNumber ? &min_snapshot : nullptr;

      // Estimate if the sample entry is valid or not.
      get_res = mt->Get(lkey, &vget, /*columns=*/nullptr, /*timestamp=*/nullptr,
                        &mget_s, &merge_context, &max_covering_tombstone_seq,
                        &sqno, ro, true /* immutable_memtable */);
      if (!get_res) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Memtable Get returned false when Get(sampled entry). "
            "Yet each sample entry should exist somewhere in the memtable, "
            "unrelated to whether it has been deleted or not.");
      }

      // TODO(bjlemaire): evaluate typeMerge.
      // This is where the sampled entry is estimated to be
      // garbage or not. Note that this is a garbage *estimation*
      // because we do not include certain items such as
      // CompactionFitlers triggered at flush, or if the same delete
      // has been inserted twice or more in the memtable.

      // Evaluate if the entry can be useful payload
      // Situation #1: entry is a KV entry, was found in the memtable mt
      //               and the sequence numbers match.
      can_be_useful_payload = (res.type == kTypeValue) && get_res &&
                              mget_s.ok() && (sqno == res.sequence);

      // Situation #2: entry is a delete entry, was found in the memtable mt
      //               (because gres==true) and no valid KV entry is found.
      //               (note: duplicate delete entries are also taken into
      //               account here, because the sequence number 'sqno'
      //               in memtable->Get(&sqno) operation is set to be equal
      //               to the most recent delete entry as well).
      can_be_useful_payload |=
          ((res.type == kTypeDeletion) || (res.type == kTypeSingleDeletion)) &&
          mget_s.IsNotFound() && get_res && (sqno == res.sequence);

      // If there is a chance that the entry is useful payload
      // Verify that the entry does not appear in the following memtables
      // (memtables with greater memtable ID/larger sequence numbers).
      if (can_be_useful_payload) {
        not_in_next_mems = true;
        for (auto next_mem_iter = mem_iter + 1;
             next_mem_iter != std::end(mems_); next_mem_iter++) {
          if ((*next_mem_iter)
                  ->Get(lkey, &vget, /*columns=*/nullptr,
                        /*timestamp=*/nullptr, &mget_s, &merge_context,
                        &max_covering_tombstone_seq, &sqno, ro,
                        true /* immutable_memtable */)) {
            not_in_next_mems = false;
            break;
          }
        }
        if (not_in_next_mems) {
          useful_payload += entry_size;
        }
      }
    }
    if (payload > 0) {
      // We use the estimated useful payload ratio to
      // evaluate how many of the memtable bytes are useful bytes.
      estimated_useful_payload +=
          (mt->ApproximateMemoryUsage()) * (useful_payload * 1.0 / payload);

      ROCKS_LOG_INFO(db_options_.info_log,
                     "Mempurge sampling [CF %s] - found garbage ratio from "
                     "sampling: %f. Threshold is %f\n",
                     cfd_->GetName().c_str(),
                     (payload - useful_payload) * 1.0 / payload, threshold);
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Mempurge sampling: null payload measured, and collected "
                     "sample size is %zu\n.",
                     sentries.size());
    }
  }
  // We convert the total number of useful payload bytes
  // into the proportion of memtable necessary to store all these bytes.
  // We compare this proportion with the threshold value.
  return ((estimated_useful_payload / mutable_cf_options_.write_buffer_size) <
          threshold);
}

Status RemoteFlushJob::WriteLevel0Table(int sep, std::mutex* thr_mtx) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_REMOTE_FLUSH_WRITE_L0);
  db_mutex_->AssertHeld();
  const uint64_t start_micros = clock_->NowMicros();
  const uint64_t start_cpu_micros = clock_->CPUMicros();
  SequenceNumber smallest_seqno = mems_.front()->GetEarliestSequenceNumber();
  Status s;
  if (!db_impl_seqno_time_mapping_.Empty()) {
    // make a local copy, as the seqno_time_mapping from db_impl is not
    // thread safe, which will be used while not holding the db_mutex.
    seqno_to_time_mapping_ = db_impl_seqno_time_mapping_.Copy(smallest_seqno);
  }

  {
    auto write_hint = cfd_->CalculateSSTWriteHint(0);
    Env::IOPriority io_priority = GetRateLimiterPriorityForWrite();
    db_mutex_->Unlock();
    // memtables and range_del_iters store internal iterators over each data
    // memtable and its associated range deletion memtable, respectively, at
    // corresponding indexes.
    std::vector<InternalIterator*> memtables;
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters;
    ReadOptions ro;
    ro.total_order_seek = true;
    Arena arena;
    uint64_t total_num_entries = 0;
    // Used for testing:
    uint64_t mems_size = mems_.size();
    (void)mems_size;  // avoids unused variable error when
                      // TEST_SYNC_POINT_CALLBACK not used.
    assert(job_context_);
    for (MemTable* m : mems_) {
      memtables.push_back(m->NewIterator(ro, &arena, sep));
      auto* range_del_iter = m->NewRangeTombstoneIterator(
          ro, kMaxSequenceNumber, true /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
      total_num_entries += m->num_entries();
    }

    {
      ScopedArenaIterator iter(
          NewMergingIterator(&cfd_->internal_comparator(), memtables.data(),
                             static_cast<int>(memtables.size()), &arena));
      TEST_SYNC_POINT_CALLBACK(
          "RemoteFlushJob::WriteLevel0Table:output_compression",
          &output_compression_);
      int64_t _current_time = 0;
      auto status = clock_->GetCurrentTime(&_current_time);
      const uint64_t current_time = static_cast<uint64_t>(_current_time);

      uint64_t oldest_key_time = mems_.front()->ApproximateOldestKeyTime();

      // It's not clear whether oldest_key_time is always available. In case
      // it is not available, use current_time.
      uint64_t oldest_ancester_time = std::min(current_time, oldest_key_time);

      TEST_SYNC_POINT_CALLBACK(
          "RemoteFlushJob::WriteLevel0Table:oldest_ancester_time",
          &oldest_ancester_time);
      meta_[sep].oldest_ancester_time = oldest_ancester_time;
      meta_[sep].file_creation_time = current_time;

      uint64_t num_input_entries = 0;
      uint64_t memtable_payload_bytes = 0;
      uint64_t memtable_garbage_bytes = 0;
      IOStatus io_s;
      const std::string* const full_history_ts_low =
          (full_history_ts_low_.empty()) ? nullptr : &full_history_ts_low_;
      TableBuilderOptions tboptions(
          *cfd_->ioptions(), mutable_cf_options_, cfd_->internal_comparator(),
          cfd_->int_tbl_prop_collector_factories(), output_compression_,
          mutable_cf_options_.compression_opts, cfd_->GetID(), cfd_->GetName(),
          0 /* level */, false /* is_bottommost */,
          TableFileCreationReason::kFlush, oldest_key_time, current_time,
          db_id_, db_session_id_, 0 /* target_file_size */,
          meta_[sep].fd.GetNumber());
      // LOG("Get SnapshotSeqnum");
      const SequenceNumber job_snapshot_seq =
          job_context_->GetJobSnapshotSequence();
      // LOG("RemoteFlushJob::WriteLevel0Table: BuildTable");
      s = RemoteBuildTable(
          dbname_, versions_, db_options_, tboptions, file_options_, iter.get(),
          std::move(range_del_iters), meta_ + sep, existing_snapshots_,
          earliest_write_conflict_snapshot_, job_snapshot_seq,
          snapshot_checker_, mutable_cf_options_.paranoid_file_checks, &io_s,
          seqno_to_time_mapping_, job_context_->job_id, io_priority,
          &table_properties_[sep], write_hint, full_history_ts_low, base_,
          &num_input_entries, &memtable_payload_bytes, &memtable_garbage_bytes);
      // TODO: Cleanup io_status in BuildTable and table builders
      // LOG("RemoteFlushJob::WriteLevel0Table: BuildTable done");
      assert(!s.ok() || io_s.ok());
      io_s.PermitUncheckedError();
      if (num_input_entries != total_num_entries && s.ok()) {
        std::string msg = "Expected " + std::to_string(total_num_entries) +
                          " entries in memtables, but read " +
                          std::to_string(num_input_entries);
        LOG_CERR("sep:: ", sep, ":: ", msg);
      }
      if (tboptions.reason == TableFileCreationReason::kFlush) {
        TEST_SYNC_POINT("DBImpl::RemoteFlushJob:Flush");
        RecordTick(stats_, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
                   memtable_payload_bytes);
        RecordTick(stats_, MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
                   memtable_garbage_bytes);
      }
      LogFlush(db_options_.info_log);
    }

    if (s.ok() && output_file_directory_ != nullptr && sync_output_directory_) {
      s = output_file_directory_->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
    TEST_SYNC_POINT_CALLBACK("RemoteFlushJob::WriteLevel0Table", &mems_);
    db_mutex_->Lock();
  }
  // base_->Unref();

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  const bool has_output = meta_[sep].fd.GetFileSize() > 0;
  LOG_CERR("sep:: new file size: ", sep, ' ', meta_[sep].fd.GetFileSize());

  if (s.ok() && has_output) {
    TEST_SYNC_POINT("DBImpl::RemoteFlushJob:SSTFileCreated");
    // if we have more than 1 background thread, then we cannot
    // insert files directly into higher levels because some other
    // threads could be concurrently producing compacted files for
    // that key range.
    // Add file to L0
    thr_mtx->lock();
    edit_->AddFile(
        0 /* level */, meta_[sep].fd.GetNumber(), meta_[sep].fd.GetPathId(),
        meta_[sep].fd.GetFileSize(), meta_[sep].smallest, meta_[sep].largest,
        meta_[sep].fd.smallest_seqno, meta_[sep].fd.largest_seqno,
        meta_[sep].marked_for_compaction, meta_[sep].temperature,
        meta_[sep].oldest_blob_file_number, meta_[sep].oldest_ancester_time,
        meta_[sep].file_creation_time, meta_[sep].epoch_number,
        meta_[sep].file_checksum, meta_[sep].file_checksum_func_name,
        meta_[sep].unique_id,
        meta_[sep].compensated_range_deletion_size);  // packremote
    thr_mtx->unlock();
  }
  // Piggyback RemoteFlushJobInfo on the first first flushed memtable.
  mems_[0]->SetFlushJobInfo(GetFlushJobInfo(0));  // packremote

  // Note that here we treat flush as level 0 compaction in internal stats
  InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
  const uint64_t micros = clock_->NowMicros() - start_micros;
  const uint64_t cpu_micros = clock_->CPUMicros() - start_cpu_micros;
  stats.micros = micros;
  stats.cpu_micros = cpu_micros;
  if (has_output) {
    stats.bytes_written = meta_[sep].fd.GetFileSize();
    stats.num_output_files = 1;
  }

  const auto& blobs = edit_->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  stats.num_output_files_blob = static_cast<int>(blobs.size());

  RecordTimeToHistogram(stats_, FLUSH_TIME, stats.micros);
  thr_mtx->lock();
  cfd_->internal_stats()->AddCompactionStats(0 /* level */, Env::Priority::HIGH,
                                             stats);  // packremote
  cfd_->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      stats.bytes_written + stats.bytes_written_blob);  // packremote
  RecordFlushIOStats();
  thr_mtx->unlock();
  db_mutex_->Unlock();
  return s;
}

Env::IOPriority RemoteFlushJob::GetRateLimiterPriorityForWrite() {
  if (remote_db_ == nullptr) {
    if (versions_ && versions_->GetColumnFamilySet() &&
        versions_->GetColumnFamilySet()->write_controller()) {
      WriteController* write_controller =
          versions_->GetColumnFamilySet()->write_controller();
      if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
        return Env::IO_USER;
      }
    }
  } else {
    if (remote_db_->GetVersionSet() &&
        remote_db_->GetVersionSet()->GetColumnFamilySet() &&
        remote_db_->GetVersionSet()->GetColumnFamilySet()->write_controller()) {
      WriteController* write_controller =
          remote_db_->GetVersionSet()->GetColumnFamilySet()->write_controller();
      if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
        return Env::IO_USER;
      }
    }
  }

  return Env::IO_HIGH;
}

std::unique_ptr<FlushJobInfo> RemoteFlushJob::GetFlushJobInfo(int sep) const {
  db_mutex_->AssertHeld();
  std::unique_ptr<FlushJobInfo> info(new FlushJobInfo{});
  info->cf_id = cfd_->GetID();
  info->cf_name = cfd_->GetName();
  // todo: partial flushjobinfo
  const uint64_t file_number = meta_[sep].fd.GetNumber();
  info->file_path =
      MakeTableFileName(cfd_->ioptions()->cf_paths[0].path, file_number);
  info->file_number = file_number;
  info->oldest_blob_file_number = meta_[0].oldest_blob_file_number;
  info->thread_id = db_options_.env->GetThreadID();
  info->job_id = job_context_->job_id;
  info->smallest_seqno = meta_[sep].fd.smallest_seqno;
  info->largest_seqno = meta_[sep].fd.largest_seqno;
  info->table_properties = table_properties_[sep];
  info->flush_reason = flush_reason_;
  info->blob_compression_type = mutable_cf_options_.blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : edit_->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(cfd_->ioptions()->cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }
  return info;
}

}  // namespace ROCKSDB_NAMESPACE
