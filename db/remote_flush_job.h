//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class RemoteFlushJob {
 public:
  TCPNode local_generator_node;
#ifdef ROCKSDB_RDMA
  RDMAClient* local_generator_rdma_client;

 private:
  RDMANode::rdma_connection* rdma_conn;
#endif

 public:
  static std::shared_ptr<RemoteFlushJob> CreateRemoteFlushJob(
      const std::string& dbname, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
      const FileOptions& file_options, VersionSet* versions,
      InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
      std::vector<SequenceNumber> existing_snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      SnapshotChecker* snapshot_checker, JobContext* job_context,
      FlushReason flush_reason, LogBuffer* log_buffer,
      FSDirectory* db_directory, FSDirectory* output_file_directory,
      CompressionType output_compression, Statistics* stats,
      EventLogger* event_logger, bool measure_io_stats,
      const bool sync_output_directory, const bool write_manifest,
      const std::shared_ptr<IOTracer>& io_tracer,
      const SeqnoToTimeMapping& seq_time_mapping,
#ifdef ROCKSDB_RDMA
      RDMAClient* rdma_client,
#endif
      const std::string& db_id = "", const std::string& db_session_id = "",
      std::string full_history_ts_low = "",
      BlobFileCompletionCallback* blob_callback = nullptr);
  void PackLocal(TransferService* node) const;
  void DoubleCheck(TransferService* node) const;
  static void* UnPackLocal(RDMAClient* client, TransferService* node,
                           DBImpl* remote_db, std::vector<MemTable*>& mems);
  void PackRemote(TransferService* node) const;
  void UnPackRemote(TransferService* node);

 private:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while RemoteFlushJob is
  // alive
  RemoteFlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                 const ImmutableDBOptions& db_options,
                 const MutableCFOptions& mutable_cf_options,
                 uint64_t max_memtable_id, const FileOptions& file_options,
                 VersionSet* versions, InstrumentedMutex* db_mutex,
                 std::atomic<bool>* shutting_down,
                 std::vector<SequenceNumber> existing_snapshots,
                 SequenceNumber earliest_write_conflict_snapshot,
                 SnapshotChecker* snapshot_checker, JobContext* job_context,
                 FlushReason flush_reason, LogBuffer* log_buffer,
                 FSDirectory* db_directory, FSDirectory* output_file_directory,
                 CompressionType output_compression, Statistics* stats,
                 EventLogger* event_logger, bool measure_io_stats,
                 const bool sync_output_directory, const bool write_manifest,
                 const std::shared_ptr<IOTracer>& io_tracer,
                 const SeqnoToTimeMapping& seq_time_mapping,
#ifdef ROCKSDB_RDMA
                 RDMAClient* rdma_client,
#endif
                 const std::string& db_id = "",
                 const std::string& db_session_id = "",
                 std::string full_history_ts_low = "",
                 BlobFileCompletionCallback* blob_callback = nullptr);

 public:
  ~RemoteFlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  Status RunLocal(LogsWithPrepTracker* prep_tracker = nullptr,
                  FileMetaData* file_meta = nullptr,
                  bool* switched_to_mempurge = nullptr);
  Status RunRemote(std::vector<std::pair<std::string, size_t>>*,
                   std::function<int()>*, const std::string&,
                   LogsWithPrepTracker* prep_tracker = nullptr,
                   FileMetaData* file_meta = nullptr,
                   bool* switched_to_mempurge = nullptr);
  Status MatchMemNode(std::vector<std::pair<std::string, size_t>>*);
  Status QuitMemNode();
  Status MatchRemoteWorker(int port);
  Status QuitRemoteWorker();

  void Cancel();
  const autovector<MemTable*>& GetMemTables() const { return mems_; }

  std::list<std::unique_ptr<FlushJobInfo>>* GetCommittedFlushJobsInfo() {
    return &committed_flush_jobs_info_;
  }

 private:
  friend class FlushJobTest_GetRateLimiterPriorityForWrite_Test;

  void ReportStartedFlush();
  void ReportFlushInputSize(const autovector<MemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table(int sep, std::mutex* thr_mtx);

  // Memtable Garbage Collection algorithm: a MemPurge takes the list
  // of immutable memtables and filters out (or "purge") the outdated bytes
  // out of it. The output (the filtered bytes, or "useful payload") is
  // then transfered into a new memtable. If this memtable is filled, then
  // the mempurge is aborted and rerouted to a regular flush process. Else,
  // depending on the heuristics, placed onto the immutable memtable list.
  // The addition to the imm list will not trigger a flush operation. The
  // flush of the imm list will instead be triggered once the mutable memtable
  // is added to the imm list.
  // This process is typically intended for workloads with heavy overwrites
  // when we want to avoid SSD writes (and reads) as much as possible.
  // "MemPurge" is an experimental feature still at a very early stage
  // of development. At the moment it is only compatible with the Get, Put,
  // Delete operations as well as Iterators and CompactionFilters.
  // For this early version, "MemPurge" is called by setting the
  // options.experimental_mempurge_threshold value as >0.0. When this is
  // the case, ALL automatic flush operations (kWRiteBufferManagerFull) will
  // first go through the MemPurge process. Therefore, we strongly
  // recommend all users not to set this flag as true given that the MemPurge
  // process has not matured yet.
  Status MemPurge();
  bool MemPurgeDecider(double threshold);
  // The rate limiter priority (io_priority) is determined dynamically here.
  Env::IOPriority GetRateLimiterPriorityForWrite();
  std::unique_ptr<FlushJobInfo> GetFlushJobInfo(int sep) const;

  const std::string dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  // A variable storing the largest memtable id to flush in this
  // flush job. RocksDB uses this variable to select the memtables to flush in
  // this job. All memtables in this column family with an ID smaller than or
  // equal to max_memtable_id_ will be selected for flush.
  uint64_t max_memtable_id_;
  const FileOptions file_options_;
  VersionSet* versions_;
  InstrumentedMutex* db_mutex_;
  std::atomic<bool>* shutting_down_;
  std::vector<SequenceNumber> existing_snapshots_;
  SequenceNumber earliest_write_conflict_snapshot_;
  SnapshotChecker* snapshot_checker_;
  JobContext* job_context_;
  FlushReason flush_reason_;
  FSDirectory* db_directory_;
  FSDirectory* output_file_directory_;
  CompressionType output_compression_;
  TableProperties table_properties_[4];
  // True if this flush job should call fsync on the output directory. False
  // otherwise.
  // Usually sync_output_directory_ is true. A flush job needs to call sync on
  // the output directory before committing to the MANIFEST.
  // However, an individual flush job does not have to call sync on the output
  // directory if it is part of an atomic flush. After all flush jobs in the
  // atomic flush succeed, call sync once on each distinct output directory.
  const bool sync_output_directory_;
  // True if this flush job should write to MANIFEST after successfully
  // flushing memtables. False otherwise.
  // Usually write_manifest_ is true. A flush job commits to the MANIFEST after
  // flushing the memtables.
  // However, an individual flush job cannot rashly write to the MANIFEST
  // immediately after it finishes the flush if it is part of an atomic flush.
  // In this case, only after all flush jobs succeed in flush can RocksDB
  // commit to the MANIFEST.
  const bool write_manifest_;
  // The current flush job can commit flush result of a concurrent flush job.
  // We collect RemoteFlushJobInfo of all jobs committed by current job and fire
  // OnFlushCompleted for them.
  std::list<std::unique_ptr<FlushJobInfo>> committed_flush_jobs_info_;

  // Variables below are set by PickMemTable():
  FileMetaData meta_[4];
  autovector<MemTable*> mems_;
  VersionEdit* edit_;
  Version* base_;
  bool pick_memtable_called;

  SystemClock* clock_;

  const std::string full_history_ts_low_;
  // BlobFileCompletionCallback* blob_callback_;

  Statistics* stats_;
  // reference to the seqno_time_mapping_ in db_impl.h, not safe to read without
  // db mutex
  const SeqnoToTimeMapping& db_impl_seqno_time_mapping_;
  SeqnoToTimeMapping seqno_to_time_mapping_;
  DBImpl* remote_db_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
