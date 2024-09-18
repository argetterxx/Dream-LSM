//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"

#include <sys/socket.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction_picker.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/tcprw.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/blockingconcurrentqueue.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/options.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/table.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/merging_iterator.h"
#include "table/table_properties_collector_pack_factory.h"
#include "trace_replay/trace_replay.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(
    ColumnFamilyData* column_family_data, DBImpl* db, InstrumentedMutex* mutex)
    : cfd_(column_family_data), db_(db), mutex_(mutex) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    for (auto& listener : cfd_->ioptions()->listeners) {
      listener->OnColumnFamilyHandleDeletionStarted(this);
    }
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    // Need to hold some shared pointers owned by the initial_cf_options
    // before final cleaning up finishes.
    ColumnFamilyOptions initial_cf_options_copy = cfd_->initial_cf_options();
    JobContext job_context(0);
    mutex_->Lock();
    bool dropped = cfd_->IsDropped();
    if (cfd_->UnrefAndTryDelete()) {
      if (dropped) {
        db_->FindObsoleteFiles(&job_context, false, true);
      }
    }
    mutex_->Unlock();
    if (job_context.HaveSomethingToDelete()) {
      bool defer_purge =
          db_->immutable_db_options().avoid_unnecessary_blocking_io;
      db_->PurgeObsoleteFiles(job_context, defer_purge);
    }
    LOG("traccking test");
    job_context.Clean();
  }
}

uint32_t ColumnFamilyHandleImpl::GetID() const { return cfd()->GetID(); }

const std::string& ColumnFamilyHandleImpl::GetName() const {
  return cfd()->GetName();
}

Status ColumnFamilyHandleImpl::GetDescriptor(ColumnFamilyDescriptor* desc) {
  // accessing mutable cf-options requires db mutex.
  InstrumentedMutexLock l(mutex_);
  *desc = ColumnFamilyDescriptor(cfd()->GetName(), cfd()->GetLatestCFOptions());
  return Status::OK();
}

const Comparator* ColumnFamilyHandleImpl::GetComparator() const {
  return cfd()->user_comparator();
}

void GetIntTblPropCollectorFactory(
    const ImmutableCFOptions& ioptions,
    IntTblPropCollectorFactories* int_tbl_prop_collector_factories) {
  assert(int_tbl_prop_collector_factories);

  auto& collector_factories = ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < ioptions.table_properties_collector_factories.size();
       ++i) {
    assert(collector_factories[i]);
    int_tbl_prop_collector_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(collector_factories[i]));
  }
}

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      if (!CompressionTypeSupported(cf_options.compression_per_level[level])) {
        return Status::InvalidArgument(
            "Compression type " +
            CompressionTypeToString(cf_options.compression_per_level[level]) +
            " is not linked with the binary.");
      }
    }
  } else {
    if (!CompressionTypeSupported(cf_options.compression)) {
      return Status::InvalidArgument(
          "Compression type " +
          CompressionTypeToString(cf_options.compression) +
          " is not linked with the binary.");
    }
  }
  if (cf_options.compression_opts.zstd_max_train_bytes > 0) {
    if (cf_options.compression_opts.use_zstd_dict_trainer) {
      if (!ZSTD_TrainDictionarySupported()) {
        return Status::InvalidArgument(
            "zstd dictionary trainer cannot be used because ZSTD 1.1.3+ "
            "is not linked with the binary.");
      }
    } else if (!ZSTD_FinalizeDictionarySupported()) {
      return Status::InvalidArgument(
          "zstd finalizeDictionary cannot be used because ZSTD 1.4.5+ "
          "is not linked with the binary.");
    }
    if (cf_options.compression_opts.max_dict_bytes == 0) {
      return Status::InvalidArgument(
          "The dictionary size limit (`CompressionOptions::max_dict_bytes`) "
          "should be nonzero if we're using zstd's dictionary generator.");
    }
  }

  if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    std::ostringstream oss;
    oss << "The specified blob compression type "
        << CompressionTypeToString(cf_options.blob_compression_type)
        << " is not available.";

    return Status::InvalidArgument(oss.str());
  }

  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    return Status::InvalidArgument(
        "In-place memtable updates (inplace_update_support) is not compatible "
        "with concurrent writes (allow_concurrent_memtable_write)");
  }
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't concurrent writes (allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles. ");
    } else if (cf_options.cf_paths.empty() && db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles. ");
    }
  }
  return Status::OK();
}

namespace {
const uint64_t kDefaultTtl = 0xfffffffffffffffe;
const uint64_t kDefaultPeriodicCompSecs = 0xfffffffffffffffe;
}  // anonymous namespace

ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                    const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  ClipToRange(&result.write_buffer_size, (static_cast<size_t>(64)) << 10,
              clamp_max);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size =
        std::min(size_t{1024 * 1024}, result.write_buffer_size / 8);

    // Align up to 4k
    const size_t align = 4 * 1024;
    result.arena_block_size =
        ((result.arena_block_size + align - 1) / align) * align;
  }
  result.min_write_buffer_number_to_merge =
      std::min(result.min_write_buffer_number_to_merge,
               result.max_write_buffer_number - 1);
  if (result.min_write_buffer_number_to_merge < 1) {
    result.min_write_buffer_number_to_merge = 1;
  }

  if (db_options.atomic_flush && result.min_write_buffer_number_to_merge > 1) {
    ROCKS_LOG_WARN(
        db_options.logger,
        "Currently, if atomic_flush is true, then triggering flush for any "
        "column family internally (non-manual flush) will trigger flushing "
        "all column families even if the number of memtables is smaller "
        "min_write_buffer_number_to_merge. Therefore, configuring "
        "min_write_buffer_number_to_merge > 1 is not compatible and should "
        "be satinized to 1. Not doing so will lead to data loss and "
        "inconsistent state across multiple column families when WAL is "
        "disabled, which is a common setting for atomic flush");

    result.min_write_buffer_number_to_merge = 1;
  }

  if (result.num_levels < 1) {
    result.num_levels = 1;
  }
  if (result.compaction_style == kCompactionStyleLevel &&
      result.num_levels < 2) {
    result.num_levels = 2;
  }

  if (result.compaction_style == kCompactionStyleUniversal &&
      db_options.allow_ingest_behind && result.num_levels < 3) {
    result.num_levels = 3;
  }

  if (result.max_write_buffer_number < 2) {
    result.max_write_buffer_number = 2;
  }

  if (result.max_local_write_buffer_number < 2) {
    result.max_local_write_buffer_number = 2;
  }
  // fall back max_write_buffer_number_to_maintain if
  // max_write_buffer_size_to_maintain is not set
  if (result.max_write_buffer_size_to_maintain < 0) {
    result.max_write_buffer_size_to_maintain =
        result.max_write_buffer_number *
        static_cast<int64_t>(result.write_buffer_size);
  } else if (result.max_write_buffer_size_to_maintain == 0 &&
             result.max_write_buffer_number_to_maintain < 0) {
    result.max_write_buffer_number_to_maintain = result.max_write_buffer_number;
  }
  // bloom filter size shouldn't exceed 1/4 of memtable size.
  if (result.memtable_prefix_bloom_size_ratio > 0.25) {
    result.memtable_prefix_bloom_size_ratio = 0.25;
  } else if (result.memtable_prefix_bloom_size_ratio < 0) {
    result.memtable_prefix_bloom_size_ratio = 0;
  }

  if (!result.prefix_extractor) {
    assert(result.memtable_factory);
    Slice name = result.memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (result.compaction_style == kCompactionStyleFIFO) {
    // since we delete level0 files in FIFO compaction when there are too many
    // of them, these options don't really mean anything
    result.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    result.level0_stop_writes_trigger = std::numeric_limits<int>::max();
  }

  if (result.max_bytes_for_level_multiplier <= 0) {
    result.max_bytes_for_level_multiplier = 1;
  }

  if (result.level0_file_num_compaction_trigger == 0) {
    ROCKS_LOG_WARN(db_options.logger,
                   "level0_file_num_compaction_trigger cannot be 0");
    result.level0_file_num_compaction_trigger = 1;
  }

  if (result.level0_stop_writes_trigger <
          result.level0_slowdown_writes_trigger ||
      result.level0_slowdown_writes_trigger <
          result.level0_file_num_compaction_trigger) {
    ROCKS_LOG_WARN(db_options.logger,
                   "This condition must be satisfied: "
                   "level0_stop_writes_trigger(%d) >= "
                   "level0_slowdown_writes_trigger(%d) >= "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
    if (result.level0_slowdown_writes_trigger <
        result.level0_file_num_compaction_trigger) {
      result.level0_slowdown_writes_trigger =
          result.level0_file_num_compaction_trigger;
    }
    if (result.level0_stop_writes_trigger <
        result.level0_slowdown_writes_trigger) {
      result.level0_stop_writes_trigger = result.level0_slowdown_writes_trigger;
    }
    ROCKS_LOG_WARN(db_options.logger,
                   "Adjust the value to "
                   "level0_stop_writes_trigger(%d)"
                   "level0_slowdown_writes_trigger(%d)"
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
  }

  if (result.soft_pending_compaction_bytes_limit == 0) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  } else if (result.hard_pending_compaction_bytes_limit > 0 &&
             result.soft_pending_compaction_bytes_limit >
                 result.hard_pending_compaction_bytes_limit) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  }

  // When the DB is stopped, it's possible that there are some .trash files that
  // were not deleted yet, when we open the DB we will find these .trash files
  // and schedule them to be deleted (or delete immediately if SstFileManager
  // was not used)
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options.sst_file_manager.get());
  for (size_t i = 0; i < result.cf_paths.size(); i++) {
    DeleteScheduler::CleanupDirectory(db_options.env, sfm,
                                      result.cf_paths[i].path)
        .PermitUncheckedError();
  }

  if (result.cf_paths.empty()) {
    result.cf_paths = db_options.db_paths;
  }
  if (db_options.server_remote_flush != 0) {
    result.server_use_remote_flush = true;
  }

  if (result.level_compaction_dynamic_level_bytes) {
    if (result.compaction_style != kCompactionStyleLevel) {
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "level_compaction_dynamic_level_bytes only makes sense"
                     "for level-based compaction");
      result.level_compaction_dynamic_level_bytes = false;
    } else if (result.cf_paths.size() > 1U) {
      // we don't yet know how to make both of this feature and multiple
      // DB path work.
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "multiple cf_paths/db_paths and"
                     "level_compaction_dynamic_level_bytes"
                     "can't be used together");
      result.level_compaction_dynamic_level_bytes = false;
    }
  }

  if (result.max_compaction_bytes == 0) {
    result.max_compaction_bytes = result.target_file_size_base * 25;
  }

  bool is_block_based_table = (result.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));

  const uint64_t kAdjustedTtl = 30 * 24 * 60 * 60;
  if (result.ttl == kDefaultTtl) {
    if (is_block_based_table &&
        result.compaction_style != kCompactionStyleFIFO) {
      result.ttl = kAdjustedTtl;
    } else {
      result.ttl = 0;
    }
  }

  const uint64_t kAdjustedPeriodicCompSecs = 30 * 24 * 60 * 60;

  // Turn on periodic compactions and set them to occur once every 30 days if
  // compaction filters are used and periodic_compaction_seconds is set to the
  // default value.
  if (result.compaction_style != kCompactionStyleFIFO) {
    if ((result.compaction_filter != nullptr ||
         result.compaction_filter_factory != nullptr) &&
        result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else {
    // result.compaction_style == kCompactionStyleFIFO
    if (result.ttl == 0) {
      if (is_block_based_table) {
        if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
          result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
        }
        result.ttl = result.periodic_compaction_seconds;
      }
    } else if (result.periodic_compaction_seconds != 0) {
      result.ttl = std::min(result.ttl, result.periodic_compaction_seconds);
    }
  }

  // TTL compactions would work similar to Periodic Compactions in Universal in
  // most of the cases. So, if ttl is set, execute the periodic compaction
  // codepath.
  if (result.compaction_style == kCompactionStyleUniversal && result.ttl != 0) {
    if (result.periodic_compaction_seconds != 0) {
      result.periodic_compaction_seconds =
          std::min(result.ttl, result.periodic_compaction_seconds);
    } else {
      result.periodic_compaction_seconds = result.ttl;
    }
  }

  if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
    result.periodic_compaction_seconds = 0;
  }

  return result;
}

int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;

SuperVersion::~SuperVersion() {
  LOG("SuperVersion::~SuperVersion");
  for (auto td : to_delete) {
    delete td;
  }
  LOG("SuperVersion::~SuperVersion end");
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  // Since this SuperVersion object is being deleted,
  // decrement reference to the immutable MemtableList
  // this SV object was pointing to.
  imm->Unref(&to_delete);
  MemTable* m = mem->Unref();
  if (m != nullptr) {
    auto* memory_usage = current->cfd()->imm()->current_memory_usage();
    assert(*memory_usage >= m->ApproximateMemoryUsage());
    *memory_usage -= m->ApproximateMemoryUsage();
    LOG("MemTableListVersion::UnrefMemTable to_delete Add M: ", m->GetID(), ' ',
        std::hex, reinterpret_cast<void*>(m), std::dec);
    to_delete.push_back(m);
  }
  current->Unref();
  cfd->UnrefAndTryDelete();
}

void SuperVersion::Init(ColumnFamilyData* new_cfd, MemTable* new_mem,
                        MemTableListVersion* new_imm, Version* new_current) {
  cfd = new_cfd;
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  cfd->Ref();
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);
}

namespace {
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exits or a ThreadLocalPtr gets
  // destroyed. When the former happens, the thread shouldn't see kSVInUse.
  // When the latter happens, only super_version_ holds a reference
  // to ColumnFamilyData, so no further queries are possible.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  bool was_last_ref __attribute__((__unused__));
  was_last_ref = sv->Unref();
  // Thread-local SuperVersions can't outlive ColumnFamilyData::super_version_.
  // This is important because we can't do SuperVersion cleanup here.
  // That would require locking DB mutex, which would deadlock because
  // SuperVersionUnrefHandle is called with locked ThreadLocalPtr mutex.
  assert(!was_last_ref);
}
}  // anonymous namespace

std::vector<std::string> ColumnFamilyData::GetDbPaths() const {
  std::vector<std::string> paths;
  paths.reserve(ioptions_.cf_paths.size());
  for (const DbPath& db_path : ioptions_.cf_paths) {
    paths.emplace_back(db_path.path);
  }
  return paths;
}

const uint32_t ColumnFamilyData::kDummyColumnFamilyDataId =
    std::numeric_limits<uint32_t>::max();

ColumnFamilyData::ColumnFamilyData(
    uint32_t id, const std::string& name, Version* _dummy_versions,
    Cache* _table_cache, WriteBufferManager* write_buffer_manager,
    const ColumnFamilyOptions& cf_options, const ImmutableDBOptions& db_options,
    const FileOptions* file_options, ColumnFamilySet* column_family_set,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id)
    : id_(id),
      name_(name),
      dummy_versions_(_dummy_versions),
      current_(nullptr),
      refs_(0),
      initialized_(false),
      dropped_(false),
      internal_comparator_(cf_options.comparator),
      initial_cf_options_(SanitizeOptions(db_options, cf_options)),
      ioptions_(db_options, initial_cf_options_),
      mutable_cf_options_(initial_cf_options_),
      is_delete_range_supported_(
          cf_options.table_factory->IsDeleteRangeSupported()),
      write_buffer_manager_(write_buffer_manager),
      mem_(nullptr),
      imm_(ioptions_.min_write_buffer_number_to_merge,
           ioptions_.max_write_buffer_number_to_maintain,
           initial_cf_options_.max_local_write_buffer_number,
           ioptions_.max_write_buffer_size_to_maintain),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle)),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      column_family_set_(column_family_set),
      queued_for_flush_(false),
      queued_for_compaction_(false),
      prev_compaction_needed_bytes_(0),
      allow_2pc_(db_options.allow_2pc),
      last_memtable_id_(0),
      db_paths_registered_(false),
      mempurge_used_(false),
      next_epoch_number_(1),
      meta_conn_(nullptr),
      trans_mem_accumulated_id(0),
      imm_que(new moodycamel::BlockingConcurrentQueue<std::pair<
                  std::pair<MemTable*, RDMANode::rdma_connection*>,
                  std::pair<bool,
                            std::chrono::high_resolution_clock::time_point>>>),
      memtable_ip_port(new std::pair<std::string, size_t>) {
  LOG("CHECK : ", "initial_cf_options_:",
      initial_cf_options_.server_use_remote_flush == true ? "true" : "false");
  if (id_ != kDummyColumnFamilyDataId) {
    // TODO(cc): RegisterDbPaths can be expensive, considering moving it
    // outside of this constructor which might be called with db mutex held.
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->RegisterDbPaths(GetDbPaths());
    if (s.ok()) {
      db_paths_registered_ = true;
    } else {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to register data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
  Ref();

  // Convert user defined table properties collector factories to internal ones.
  GetIntTblPropCollectorFactory(ioptions_, &int_tbl_prop_collector_factories_);

  // if _dummy_versions is nullptr, then this is a dummy column family.
  if (_dummy_versions != nullptr) {
    internal_stats_.reset(
        new InternalStats(ioptions_.num_levels, ioptions_.clock, this));
    table_cache_.reset(new TableCache(ioptions_, file_options, _table_cache,
                                      block_cache_tracer, io_tracer,
                                      db_session_id));
    blob_file_cache_.reset(
        new BlobFileCache(_table_cache, ioptions(), soptions(), id_,
                          internal_stats_->GetBlobFileReadHist(), io_tracer));
    blob_source_.reset(new BlobSource(ioptions(), db_id, db_session_id,
                                      blob_file_cache_.get()));

    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      compaction_picker_.reset(
          new UniversalCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleFIFO) {
      compaction_picker_.reset(
          new FIFOCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleNone) {
      compaction_picker_.reset(
          new NullCompactionPicker(ioptions_, &internal_comparator_));
      ROCKS_LOG_WARN(ioptions_.logger,
                     "Column family %s does not use any background compaction. "
                     "Compactions can only be done via CompactFiles\n",
                     GetName().c_str());
    } else {
      ROCKS_LOG_ERROR(ioptions_.logger,
                      "Unable to recognize the specified compaction style %d. "
                      "Column family %s will use kCompactionStyleLevel.\n",
                      ioptions_.compaction_style, GetName().c_str());
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    }

    if (column_family_set_->NumberOfColumnFamilies() < 10) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "--------------- Options for column family [%s]:\n",
                     name.c_str());
      initial_cf_options_.Dump(ioptions_.logger);
    } else {
      ROCKS_LOG_INFO(ioptions_.logger, "\t(skipping printing options)\n");
    }
  }

  RecalculateWriteStallConditions(mutable_cf_options_);

  if (cf_options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName()) &&
      cf_options.table_factory->GetOptions<BlockBasedTableOptions>()) {
    const BlockBasedTableOptions* bbto =
        cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
    const auto& options_overrides = bbto->cache_usage_options.options_overrides;
    const auto file_metadata_charged =
        options_overrides.at(CacheEntryRole::kFileMetadata).charged;
    if (bbto->block_cache &&
        file_metadata_charged == CacheEntryRoleOptions::Decision::kEnabled) {
      // TODO(hx235): Add a `ConcurrentCacheReservationManager` at DB scope
      // responsible for reservation of `ObsoleteFileInfo` so that we can keep
      // this `file_metadata_cache_res_mgr_` nonconcurrent
      file_metadata_cache_res_mgr_.reset(new ConcurrentCacheReservationManager(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              bbto->block_cache)));
    }
  }

  std::string memnode_ip = "10.10.1.3";
  if (db_options.server_remote_flush ||
      initial_cf_options_.max_local_write_buffer_number <
          initial_cf_options_.max_write_buffer_number) {
    if (!init_cf_level_rdma_client(memnode_ip, 9091).ok()) {
      LOG_CERR("rdma client INIT Failed");
    }
  }
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData() {
  LOG("ColumnFamilyData: ", GetID(), "destructing");
  assert(refs_.load(std::memory_order_relaxed) == 0);
  // remove from linked list
  auto prev = prev_;
  auto next = next_;
  prev->next_ = next;
  next->prev_ = prev;

  if (!dropped_ && column_family_set_ != nullptr) {
    // If it's dropped, it's already removed from column family set
    // If column_family_set_ == nullptr, this is dummy CFD and not in
    // ColumnFamilySet
    column_family_set_->RemoveColumnFamily(this);
  }

  if (current_ != nullptr) {
    current_->Unref();
  }

  // It would be wrong if this ColumnFamilyData is in flush_queue_ or
  // compaction_queue_ and we destroyed it
  assert(!queued_for_flush_);
  assert(!queued_for_compaction_);
  assert(super_version_ == nullptr);
  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->Next() == dummy_versions_);
    bool deleted __attribute__((__unused__));
    deleted = dummy_versions_->Unref();
    assert(deleted);
  }

  if (mem_ != nullptr) {
    LOG("cfd Unref Memtable");
    delete mem_->Unref();
  }
  autovector<MemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }

  if (db_paths_registered_) {
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->UnregisterDbPaths(GetDbPaths());
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to unregister data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }

  should_drop.store(true);
  if (!memtable_thread.empty() && memtable_thread[0]->joinable()) {
    for (auto thr : memtable_thread) {
      thr->join();
      delete thr;
    }
    memtable_thread.clear();
  }

  if (GetID() != kDummyColumnFamilyDataId &&
      (ioptions_.server_remote_flush ||
       initial_cf_options_.max_local_write_buffer_number <
           initial_cf_options_.max_write_buffer_number)) {
    for (size_t i = 0; i < 2; i++) {
      RDMANode::rdma_connection* conn = nullptr;
      delegated_read_conns_.wait_dequeue_timed(conn, std::chrono::seconds(2));
      if (conn) {
        cflevel_read_client_->disconnect_request(conn);
        LOG_CERR("delegated read conn disconnect for cfd: ", GetID());
      } else {
        LOG_CERR("delegated read conn disconnect timeout");
        i--;
        continue;
      }
    }
  }
  if (cflevel_read_client_) delete cflevel_read_client_;

  while (!memtable_conn_.empty()) {
    memtable_conn_mtx_.lock();
    auto conn = memtable_conn_.front();
    memtable_conn_.pop();
    cflevel_client_->disconnect_request(conn);
    memtable_conn_mtx_.unlock();
  }

  if (gc_thread_) {
    gc_thread_->join();
    delete gc_thread_;
    gc_thread_ = nullptr;
  }
  if (gc_conn_) {
    cflevel_client_->disconnect_request(gc_conn_);
  }
  if (meta_conn_) {
    cflevel_client_->disconnect_request(meta_conn_);
  }
  if (reginfo_) delete reginfo_;
  if (imm_que != nullptr) {
    delete imm_que;
    imm_que = nullptr;
  }
  if (memtable_ip_port) delete memtable_ip_port;
  if (cflevel_client_) delete cflevel_client_;
}

Status ColumnFamilyData::flush_imm_trans() {
  MemTable* imm_to_trans = nullptr;
  assert(false);
  // while (!should_drop) {
  //   assert(imm_que);
  //   imm_que_mtx->lock();
  //   if (imm_que->empty()) {
  //     imm_que_mtx->unlock();
  //     break;
  //   } else {
  //     imm_to_trans = imm_que->front();
  //     imm_que->pop();
  //     imm_que_mtx->unlock();
  //   }
  //   Status s = imm_to_trans->SendToRemote(
  //       cflevel_client_, memtable_conn_, reginfo_->imm_meta_remote_offset,
  //       reginfo_->imm_meta_local_offset, reginfo_->imm_data_remote_offset,
  //       reginfo_->imm_data_local_offset, *memtable_ip_port, id_);
  //   if (!s.ok()) {
  //     fprintf(stderr,
  //             "immutable memtable sent remote failed, reschedule task\n");
  //     std::lock_guard<std::mutex> lck(*imm_que_mtx);
  //     imm_que->push(imm_to_trans);
  //   }
  // }
  return Status::OK();
}

Status ColumnFamilyData::background_schedule_imm_trans() {
  if (!memtable_thread.empty() && memtable_thread[0]->joinable()) {
    fprintf(stderr, "memtable thread already created\n");
    return Status::Aborted();
  }
  memtable_thread.resize(4);
  for (int i = 0; i < 4; i++) {
    memtable_thread[i] = new std::thread([this, i]() {
      while (!should_drop.load()) {
        std::pair<
            std::pair<MemTable*, RDMANode::rdma_connection*>,
            std::pair<bool, std::chrono::high_resolution_clock::time_point>>
            imm_to_trans = {{nullptr, nullptr}, {false, {}}};
        assert(imm_que);
        imm_que->wait_dequeue_timed(imm_to_trans, std::chrono::seconds(5));
        if (imm_to_trans.first.first == nullptr && should_drop.load())
          break;
        else if (imm_to_trans.first.first == nullptr && !should_drop)
          continue;
        std::chrono::high_resolution_clock::time_point t1 =
            std::chrono::high_resolution_clock::now();
        LOG_CERR("Pop ImmMemTable ", imm_to_trans.first.first->GetID(),
                 " from imm_que, takes ",
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     t1 - imm_to_trans.second.second)
                     .count(),
                 " ms");
        auto reg_ = reginfo_;
        Status s = imm_to_trans.first.first->SendToRemote(
            cflevel_client_, imm_to_trans.first.second,
            reginfo_->index_mp.at(imm_to_trans.first.second).first,
            reginfo_->index_mp.at(imm_to_trans.first.second).second,
            *memtable_ip_port, id_, &gc_queue_, imm_to_trans.second.first);
        if (!s.ok()) {
          LOG_CERR("immutable memtable ", imm_to_trans.first.first->GetID(),
                   " sent remote failed, reschedule task");
          // std::lock_guard<std::mutex> lck(*imm_que_mtx);
          imm_que->enqueue(imm_to_trans);
        } else {
          // note: be able to delete local memtable copy.
          uint64_t new_id = imm_to_trans.first.first->GetID();
          uint64_t old = trans_mem_accumulated_id.load();
          while (new_id > old &&
                 !trans_mem_accumulated_id.compare_exchange_weak(old, new_id)) {
          }
          delete imm_to_trans.first.first->Unref();
          {
            std::lock_guard<std::mutex> lck(memtable_conn_mtx_);
            memtable_conn_.push(imm_to_trans.first.second);
          }
          std::chrono::high_resolution_clock::time_point t2 =
              std::chrono::high_resolution_clock::now();
          LOG_CERR(
              "Sending ImmMemTable ", imm_to_trans.first.first->GetID(),
              " to remote finished, takes ",
              std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1)
                  .count(),
              " ms");
        }
      }
    });
  }
  return Status::OK();
}

void ColumnFamilyData::free_remote() {
  if (internal_stats_ != nullptr) {
    internal_stats_.reset();
  }
  free(current_);
  const_cast<ImmutableOptions*>(&ioptions_)->~ImmutableOptions();
  const_cast<ColumnFamilyOptions*>(&initial_cf_options_)
      ->~ColumnFamilyOptions();
  const_cast<MutableCFOptions*>(&mutable_cf_options_)->~MutableCFOptions();
}

void ColumnFamilyData::PackRemote(TransferService* node) const {
  LOG("ColumnFamilyData::PackRemote");
  assert(internal_stats_ != nullptr);
  // internal_stats_->PackRemote(node);
  LOG("ColumnFamilyData::PackRemote done.");
}
void ColumnFamilyData::UnPackRemote(TransferService* node) {
  LOG("ColumnFamilyData::UnPackRemote");
  // internal_stats_->UnPackRemote(node);
  LOG("ColumnFamilyData::UnPackRemote done.");
}

void ColumnFamilyData::DoubleCheck(TransferService* node) const {
  LOG_CERR("DoubleCheck ColumnFamilyData::current_ ptr=",
           reinterpret_cast<void*>(current_));
  current_->DoubleCheck(node);
  LOG_CERR("DoubleCheck ColumnFamilyData::internal_stats_ ptr=",
           (internal_stats_ == nullptr ? "nullptr" : "non-nullptr"));
  internal_stats_->DoubleCheck(node);
}

void ColumnFamilyData::PackLocal(TransferService* node,
                                 InstrumentedMutex* db_mutex) const {
  assert(internal_comparator_.user_comparator() ==
         initial_cf_options_.comparator);
  initial_cf_options_.PackLocal(node);
  ioptions_.PackLocal(node);
  mutable_cf_options_.PackLocal(node);
  // int_tbl_prop_collector_factories_.PackLocal(sockfd);
  size_t int_tbl_prop_collector_factories_size =
      int_tbl_prop_collector_factories_.size();
  node->send(
      reinterpret_cast<const void*>(&int_tbl_prop_collector_factories_size),
      sizeof(size_t));
  LOG("server check int_tbl_prop_collector_factories_: ");
  for (auto& factory : int_tbl_prop_collector_factories_) {
    factory->PackLocal(node);
  }
  LOG("server check int_tbl_prop_collector_factories_ done.");

  size_t ret_val = name_.size();
  node->send(reinterpret_cast<const void*>(&ret_val), sizeof(size_t));
  node->send(reinterpret_cast<const void*>(name_.data()), name_.size());

  db_mutex->Lock();
  current_->PackLocal(node);
  assert(current_->cfd() == this);
  node->send(reinterpret_cast<const void*>(this), sizeof(ColumnFamilyData));
}

void* ColumnFamilyData::UnPackLocal(TransferService* node) {
  void* mem = malloc(sizeof(ColumnFamilyData));
  auto* worker_initial_cf_options_ = reinterpret_cast<ColumnFamilyOptions*>(
      ColumnFamilyOptions::UnPackLocal(node));
  auto* worker_ioptions_ = reinterpret_cast<ImmutableOptions*>(
      ImmutableOptions::UnPackLocal(node, *worker_initial_cf_options_));
  auto* worker_mutable_cf_options_ =
      reinterpret_cast<MutableCFOptions*>(MutableCFOptions::UnPackLocal(node));
  size_t int_tbl_prop_collector_factories_size = 0;
  node->receive(&int_tbl_prop_collector_factories_size, sizeof(size_t));
  std::vector<IntTblPropCollectorFactory*> temp_factories;
  for (size_t i = 0; i < int_tbl_prop_collector_factories_size; i++) {
    auto* int_tbl_prop_factory = reinterpret_cast<IntTblPropCollectorFactory*>(
        IntTblPropCollectorPackFactory::UnPackLocal(node));
    temp_factories.emplace_back(int_tbl_prop_factory);
  }
  size_t ret_val = 0;
  node->receive(&ret_val, sizeof(size_t));
  std::string worker_name_;
  worker_name_.resize(ret_val);
  node->receive(worker_name_.data(), ret_val);

  auto* worker_current_ =
      reinterpret_cast<Version*>(Version::UnPackLocal(node, mem));
  node->receive(mem, sizeof(ColumnFamilyData));
  auto* worker_cfd_ = reinterpret_cast<ColumnFamilyData*>(mem);
  new (const_cast<ColumnFamilyOptions*>(&worker_cfd_->initial_cf_options_))
      ColumnFamilyOptions(*worker_initial_cf_options_);
  new (const_cast<ImmutableOptions*>(&worker_cfd_->ioptions_))
      ImmutableOptions(*worker_ioptions_);
  delete reinterpret_cast<ImmutableOptions*>(worker_ioptions_);
  new (&worker_cfd_->mutable_cf_options_)
      MutableCFOptions(*worker_mutable_cf_options_);
  delete worker_mutable_cf_options_;
  LOG("retrieve Comparator:");
  auto worker_internal_comparator_ =
      new InternalKeyComparator(worker_cfd_->initial_cf_options_.comparator);
  new (&worker_cfd_->name_) std::string(worker_name_);
  memcpy(const_cast<void*>(
             reinterpret_cast<const void*>(&worker_cfd_->internal_comparator_)),
         reinterpret_cast<void*>(worker_internal_comparator_),
         sizeof(InternalKeyComparator));
  delete worker_internal_comparator_;
  delete reinterpret_cast<ColumnFamilyOptions*>(worker_initial_cf_options_);
  new (&worker_cfd_->int_tbl_prop_collector_factories_)
      std::vector<std::unique_ptr<IntTblPropCollectorPackFactory>>();
  worker_cfd_->int_tbl_prop_collector_factories_.resize(temp_factories.size());
  for (auto factory : temp_factories) {
    worker_cfd_->int_tbl_prop_collector_factories_.emplace_back(
        std::unique_ptr<IntTblPropCollectorFactory>(factory));
  }
  new (&worker_cfd_->internal_stats_)
      std::unique_ptr<InternalStats>(std::make_unique<InternalStats>(
          worker_cfd_->ioptions_.num_levels, worker_cfd_->ioptions_.clock,
          worker_cfd_));
  worker_cfd_->current_ = worker_current_;
  return mem;
}

bool ColumnFamilyData::UnrefAndTryDelete() {
  int old_refs = refs_.fetch_sub(1);
  assert(old_refs > 0);

  if (old_refs == 1) {
    assert(super_version_ == nullptr);
    delete this;
    return true;
  }

  if (old_refs == 2 && super_version_ != nullptr) {
    // Only the super_version_ holds me
    SuperVersion* sv = super_version_;
    super_version_ = nullptr;

    // Release SuperVersion references kept in ThreadLocalPtr.
    local_sv_.reset();

    if (sv->Unref()) {
      // Note: sv will delete this ColumnFamilyData during Cleanup()
      assert(sv->cfd == this);
      sv->Cleanup();
      delete sv;
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::SetDropped() {
  // can't drop default CF
  assert(id_ != 0);
  dropped_ = true;
  write_controller_token_.reset();

  // remove from column_family_set
  column_family_set_->RemoveColumnFamily(this);
}

ColumnFamilyOptions ColumnFamilyData::GetLatestCFOptions() const {
  return BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
}

uint64_t ColumnFamilyData::OldestLogToKeep() {
  auto current_log = GetLogNumber();

  if (allow_2pc_) {
    auto imm_prep_log = imm()->PrecomputeMinLogContainingPrepSection();
    auto mem_prep_log = mem()->GetMinLogContainingPrepSection();

    if (imm_prep_log > 0 && imm_prep_log < current_log) {
      current_log = imm_prep_log;
    }

    if (mem_prep_log > 0 && mem_prep_log < current_log) {
      current_log = mem_prep_log;
    }
  }

  return current_log;
}

const double kIncSlowdownRatio = 0.8;
const double kDecSlowdownRatio = 1 / kIncSlowdownRatio;
const double kNearStopSlowdownRatio = 0.6;
const double kDelayRecoverSlowdownRatio = 1.4;

namespace {
// If penalize_stop is true, we further reduce slowdown rate.
std::unique_ptr<WriteControllerToken> SetupDelay(
    WriteController* write_controller, uint64_t compaction_needed_bytes,
    uint64_t prev_compaction_need_bytes, bool penalize_stop,
    bool auto_compactions_disabled) {
  const uint64_t kMinWriteRate = 16 * 1024u;  // Minimum write rate 16KB/s.

  uint64_t max_write_rate = write_controller->max_delayed_write_rate();
  uint64_t write_rate = write_controller->delayed_write_rate();

  if (auto_compactions_disabled) {
    // When auto compaction is disabled, always use the value user gave.
    write_rate = max_write_rate;
  } else if (write_controller->NeedsDelay() && max_write_rate > kMinWriteRate) {
    // If user gives rate less than kMinWriteRate, don't adjust it.
    //
    // If already delayed, need to adjust based on previous compaction
    // debt. When there are two or more column families require delay, we
    // always increase or reduce write rate based on information for one
    // single column family. It is likely to be OK but we can improve if
    // there is a problem. Ignore compaction_needed_bytes = 0 case
    // because compaction_needed_bytes is only available in level-based
    // compaction
    //
    // If the compaction debt stays the same as previously, we also
    // further slow down. It usually means a mem table is full. It's
    // mainly for the case where both of flush and compaction are much
    // slower than the speed we insert to mem tables, so we need to
    // actively slow down before we get feedback signal from compaction
    // and flushes to avoid the full stop because of hitting the max
    // write buffer number.
    //
    // If DB just falled into the stop condition, we need to further
    // reduce the write rate to avoid the stop condition.
    if (penalize_stop) {
      // Penalize the near stop or stop condition by more aggressive
      // slowdown. This is to provide the long term slowdown increase
      // signal. The penalty is more than the reward of recovering to the
      // normal condition.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kNearStopSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > 0 &&
               prev_compaction_need_bytes <= compaction_needed_bytes) {
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kIncSlowdownRatio);
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }
    } else if (prev_compaction_need_bytes > compaction_needed_bytes) {
      // We are speeding up by ratio of kSlowdownRatio when we have paid
      // compaction debt. But we'll never speed up to faster than the
      // write rate given by users.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kDecSlowdownRatio);
      if (write_rate > max_write_rate) {
        write_rate = max_write_rate;
      }
    }
  }
  return write_controller->GetDelayToken(write_rate);
}

int GetL0ThresholdSpeedupCompaction(int level0_file_num_compaction_trigger,
                                    int level0_slowdown_writes_trigger) {
  // SanitizeOptions() ensures it.
  assert(level0_file_num_compaction_trigger <= level0_slowdown_writes_trigger);

  if (level0_file_num_compaction_trigger < 0) {
    return std::numeric_limits<int>::max();
  }

  const int64_t twice_level0_trigger =
      static_cast<int64_t>(level0_file_num_compaction_trigger) * 2;

  const int64_t one_fourth_trigger_slowdown =
      static_cast<int64_t>(level0_file_num_compaction_trigger) +
      ((level0_slowdown_writes_trigger - level0_file_num_compaction_trigger) /
       4);

  assert(twice_level0_trigger >= 0);
  assert(one_fourth_trigger_slowdown >= 0);

  // 1/4 of the way between L0 compaction trigger threshold and slowdown
  // condition.
  // Or twice as compaction trigger, if it is smaller.
  int64_t res = std::min(twice_level0_trigger, one_fourth_trigger_slowdown);
  if (res >= std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::max();
  } else {
    // res fits in int
    return static_cast<int>(res);
  }
}
}  // anonymous namespace

std::pair<WriteStallCondition, WriteStallCause>
ColumnFamilyData::GetWriteStallConditionAndCause(
    int num_unflushed_memtables, int num_l0_files,
    uint64_t num_compaction_needed_bytes,
    const MutableCFOptions& mutable_cf_options,
    const ImmutableCFOptions& immutable_cf_options) {
  if (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number) {
    return {WriteStallCondition::kStopped, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             num_l0_files >= mutable_cf_options.level0_stop_writes_trigger) {
    return {WriteStallCondition::kStopped, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.hard_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kStopped,
            WriteStallCause::kPendingCompactionBytes};
  } else if (mutable_cf_options.max_write_buffer_number > 3 &&
             num_unflushed_memtables >=
                 mutable_cf_options.max_write_buffer_number - 1 &&
             num_unflushed_memtables - 1 >=
                 immutable_cf_options.min_write_buffer_number_to_merge) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.level0_slowdown_writes_trigger >= 0 &&
             num_l0_files >=
                 mutable_cf_options.level0_slowdown_writes_trigger) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.soft_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kDelayed,
            WriteStallCause::kPendingCompactionBytes};
  }
  return {WriteStallCondition::kNormal, WriteStallCause::kNone};
}

WriteStallCondition ColumnFamilyData::RecalculateWriteStallConditions(
    const MutableCFOptions& mutable_cf_options) {
  auto write_stall_condition = WriteStallCondition::kNormal;
  if (current_ != nullptr) {
    auto* vstorage = current_->storage_info();
    auto write_controller = column_family_set_->write_controller_;
    uint64_t compaction_needed_bytes =
        vstorage->estimated_compaction_needed_bytes();

    auto write_stall_condition_and_cause = GetWriteStallConditionAndCause(
        imm()->NumNotFlushed(), vstorage->l0_delay_trigger_count(),
        vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
        *ioptions());
    write_stall_condition = write_stall_condition_and_cause.first;
    auto write_stall_cause = write_stall_condition_and_cause.second;

    bool was_stopped = write_controller->IsStopped();
    bool needed_delay = write_controller->NeedsDelay();

    if (write_stall_condition == WriteStallCondition::kStopped &&
        write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d",
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number);
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_STOPS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stopping writes because we have %d level-0 files",
                     name_.c_str(), vstorage->l0_delay_trigger_count());
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because of estimated pending compaction "
          "bytes %" PRIu64,
          name_.c_str(), compaction_needed_bytes);
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kMemtableLimit) {
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d "
          "rate %" PRIu64,
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number,
          write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      // L0 is the last two files from stopping.
      bool near_stop = vstorage->l0_delay_trigger_count() >=
                       mutable_cf_options.level0_stop_writes_trigger - 2;
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_DELAYS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stalling writes because we have %d level-0 files "
                     "rate %" PRIu64,
                     name_.c_str(), vstorage->l0_delay_trigger_count(),
                     write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      // If the distance to hard limit is less than 1/4 of the gap
      // between soft and hard bytes limit, we think it is near stop and
      // speed up the slowdown.
      bool near_stop =
          mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
          (compaction_needed_bytes -
           mutable_cf_options.soft_pending_compaction_bytes_limit) >
              3 *
                  (mutable_cf_options.hard_pending_compaction_bytes_limit -
                   mutable_cf_options.soft_pending_compaction_bytes_limit) /
                  4;

      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because of estimated pending compaction "
          "bytes %" PRIu64 " rate %" PRIu64,
          name_.c_str(), vstorage->estimated_compaction_needed_bytes(),
          write_controller->delayed_write_rate());
    } else {
      assert(write_stall_condition == WriteStallCondition::kNormal);
      if (vstorage->l0_delay_trigger_count() >=
          GetL0ThresholdSpeedupCompaction(
              mutable_cf_options.level0_file_num_compaction_trigger,
              mutable_cf_options.level0_slowdown_writes_trigger)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(ioptions_.logger,
                       "[%s] Increasing compaction threads because we "
                       "have %d level-0 "
                       "files ",
                       name_.c_str(), vstorage->l0_delay_trigger_count());
      } else if (vstorage->estimated_compaction_needed_bytes() >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit / 4) {
        // Increase compaction threads if bytes needed for compaction
        // exceeds 1/4 of threshold for slowing down. If soft pending
        // compaction byte limit is not set, always speed up compaction.
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        if (mutable_cf_options.soft_pending_compaction_bytes_limit > 0) {
          ROCKS_LOG_INFO(ioptions_.logger,
                         "[%s] Increasing compaction threads because of "
                         "estimated pending "
                         "compaction "
                         "bytes %" PRIu64,
                         name_.c_str(),
                         vstorage->estimated_compaction_needed_bytes());
        }
      } else {
        write_controller_token_.reset();
      }
      // If the DB recovers from delay conditions, we reward with
      // reducing double the slowdown ratio. This is to balance the long
      // term slowdown increase signal.
      if (needed_delay) {
        uint64_t write_rate = write_controller->delayed_write_rate();
        write_controller->set_delayed_write_rate(static_cast<uint64_t>(
            static_cast<double>(write_rate) * kDelayRecoverSlowdownRatio));
        // Set the low pri limit to be 1/4 the delayed write rate.
        // Note we don't reset this value even after delay condition is
        // relased. Low-pri rate will continue to apply if there is a
        // compaction pressure.
        write_controller->low_pri_rate_limiter()->SetBytesPerSecond(write_rate /
                                                                    4);
      }
    }
    prev_compaction_needed_bytes_ = compaction_needed_bytes;
  }
  return write_stall_condition;
}

const FileOptions* ColumnFamilyData::soptions() const {
  return &(column_family_set_->file_options_);
}

void ColumnFamilyData::SetCurrent(Version* current_version) {
  current_ = current_version;
}

uint64_t ColumnFamilyData::GetNumLiveVersions() const {
  return VersionSet::GetNumLiveVersions(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalSstFilesSize() const {
  return VersionSet::GetTotalSstFilesSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalBlobFileSize() const {
  return VersionSet::GetTotalBlobFileSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetLiveSstFilesSize() const {
  return current_->GetSstFilesSize();
}

MemTable* ColumnFamilyData::ConstructNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  assert(!memtable_conn_.empty() ||
         !initial_cf_options_.server_use_remote_flush);
  RDMANode::rdma_connection* conn_ = nullptr;
  if (initial_cf_options_.server_use_remote_flush ||
      initial_cf_options_.max_write_buffer_number >
          initial_cf_options_.max_local_write_buffer_number) {
    std::lock_guard<std::mutex> memconn_lock(memtable_conn_mtx_);
    conn_ = memtable_conn_.front();
    memtable_conn_.pop();
  }
  // fetch a non-nullptr connection from memtable_conn_ atomically
  auto* memtable_ = new MemTable(internal_comparator_, ioptions_,
                                 mutable_cf_options, write_buffer_manager_,
                                 earliest_seq, id_, cflevel_client_, conn_);
  LOG("ColumnFamilyData::ConstructNewMemtable Alloc memtable finish: "
      "ptr =",
      static_cast<void*>(memtable_), ' ', memtable_->GetID());
  return memtable_;
}

void ColumnFamilyData::CreateNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  MemTable* ptr = ConstructNewMemtable(mutable_cf_options, earliest_seq);
  SetMemtable(ptr);
  mem_->Ref();
}

bool ColumnFamilyData::NeedsCompaction() const {
  return !mutable_cf_options_.disable_auto_compactions &&
         compaction_picker_->NeedsCompaction(current_->storage_info());
}

Compaction* ColumnFamilyData::PickCompaction(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options, LogBuffer* log_buffer) {
  auto* result = compaction_picker_->PickCompaction(
      GetName(), mutable_options, mutable_db_options, current_->storage_info(),
      log_buffer);
  if (result != nullptr) {
    result->SetInputVersion(current_);
  }
  return result;
}

bool ColumnFamilyData::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  return compaction_picker_->RangeOverlapWithCompaction(
      smallest_user_key, largest_user_key, level);
}

Status ColumnFamilyData::RangesOverlapWithMemtables(
    const autovector<Range>& ranges, SuperVersion* super_version,
    bool allow_data_in_errors, bool* overlap) {
  assert(overlap != nullptr);
  *overlap = false;
  // Create an InternalIterator over all unflushed memtables
  Arena arena;
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  MergeIteratorBuilder merge_iter_builder(&internal_comparator_, &arena);
  merge_iter_builder.AddIterator(
      super_version->mem->NewIterator(read_opts, &arena));
  super_version->imm->AddIterators(read_opts, &merge_iter_builder,
                                   false /* add_range_tombstone_iter */);
  ScopedArenaIterator memtable_iter(merge_iter_builder.Finish());

  auto read_seq = super_version->current->version_set()->LastSequence();
  ReadRangeDelAggregator range_del_agg(&internal_comparator_, read_seq);
  auto* active_range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      read_opts, read_seq, false /* immutable_memtable */);
  range_del_agg.AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator>(active_range_del_iter));
  Status status;
  status = super_version->imm->AddRangeTombstoneIterators(
      read_opts, nullptr /* arena */, &range_del_agg);
  // AddRangeTombstoneIterators always return Status::OK.
  assert(status.ok());

  for (size_t i = 0; i < ranges.size() && status.ok() && !*overlap; ++i) {
    auto* vstorage = super_version->current->storage_info();
    auto* ucmp = vstorage->InternalComparator()->user_comparator();
    InternalKey range_start(ranges[i].start, kMaxSequenceNumber,
                            kValueTypeForSeek);
    memtable_iter->Seek(range_start.Encode());
    status = memtable_iter->status();
    ParsedInternalKey seek_result;

    if (status.ok() && memtable_iter->Valid()) {
      status = ParseInternalKey(memtable_iter->key(), &seek_result,
                                allow_data_in_errors);
    }

    if (status.ok()) {
      if (memtable_iter->Valid() &&
          ucmp->Compare(seek_result.user_key, ranges[i].limit) <= 0) {
        *overlap = true;
      } else if (range_del_agg.IsRangeOverlapped(ranges[i].start,
                                                 ranges[i].limit)) {
        *overlap = true;
      }
    }
  }
  return status;
}

const int ColumnFamilyData::kCompactAllLevels = -1;
const int ColumnFamilyData::kCompactToBaseLevel = -2;

Compaction* ColumnFamilyData::CompactRange(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, int input_level,
    int output_level, const CompactRangeOptions& compact_range_options,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts) {
  auto* result = compaction_picker_->CompactRange(
      GetName(), mutable_cf_options, mutable_db_options,
      current_->storage_info(), input_level, output_level,
      compact_range_options, begin, end, compaction_end, conflict,
      max_file_num_to_ignore, trim_ts);
  if (result != nullptr) {
    result->SetInputVersion(current_);
  }
  TEST_SYNC_POINT("ColumnFamilyData::CompactRange:Return");
  return result;
}

SuperVersion* ColumnFamilyData::GetReferencedSuperVersion(DBImpl* db) {
  SuperVersion* sv = GetThreadLocalSuperVersion(db);
  sv->Ref();
  if (!ReturnThreadLocalSuperVersion(sv)) {
    // This Unref() corresponds to the Ref() in
    // GetThreadLocalSuperVersion() when the thread-local pointer was
    // populated. So, the Ref() earlier in this function still prevents
    // the returned SuperVersion* from being deleted out from under the
    // caller.
    sv->Unref();
  }
  return sv;
}

SuperVersion* ColumnFamilyData::GetThreadLocalSuperVersion(DBImpl* db) {
  // The SuperVersion is cached in thread local storage to avoid
  // acquiring mutex when SuperVersion does not change since the last
  // use. When a new SuperVersion is installed, the compaction or flush
  // thread cleans up cached SuperVersion in all existing thread local
  // storage. To avoid acquiring mutex for this operation, we use atomic
  // Swap() on the thread local pointer to guarantee exclusive access. If
  // the thread local pointer is being used while a new SuperVersion is
  // installed, the cached SuperVersion can become stale. In that case,
  // the background thread would have swapped in kSVObsolete. We re-check
  // the value at when returning SuperVersion back to thread local, with
  // an atomic compare and swap. The superversion will need to be
  // released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete ||
      sv->version_number != super_version_number_.load()) {
    RecordTick(ioptions_.stats, NUMBER_SUPERVERSION_ACQUIRES);
    SuperVersion* sv_to_delete = nullptr;

    if (sv && sv->Unref()) {
      RecordTick(ioptions_.stats, NUMBER_SUPERVERSION_CLEANUPS);
      db->mutex()->Lock();
      // NOTE: underlying resources held by superversion (sst files)
      // might not be released until the next background job.
      sv->Cleanup();
      if (db->immutable_db_options().avoid_unnecessary_blocking_io) {
        db->AddSuperVersionsToFreeQueue(sv);
        db->SchedulePurge();
      } else {
        sv_to_delete = sv;
      }
    } else {
      db->mutex()->Lock();
    }
    sv = super_version_->Ref();
    db->mutex()->Unlock();

    delete sv_to_delete;
  }
  assert(sv != nullptr);
  return sv;
}

bool ColumnFamilyData::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call
    // (after thread local Swap() at the beginning and before
    // CompareAndSwap()). This means the SuperVersion it holds is
    // obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

void ColumnFamilyData::InstallSuperVersion(SuperVersionContext* sv_context,
                                           InstrumentedMutex* db_mutex) {
  db_mutex->AssertHeld();
  return InstallSuperVersion(sv_context, mutable_cf_options_);
}

void ColumnFamilyData::InstallSuperVersion(
    SuperVersionContext* sv_context,
    const MutableCFOptions& mutable_cf_options) {
  SuperVersion* new_superversion = sv_context->new_superversion.release();
  new_superversion->mutable_cf_options = mutable_cf_options;
  new_superversion->Init(this, mem_, imm_.current(), current_);
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  ++super_version_number_;
  super_version_->version_number = super_version_number_;
  if (old_superversion == nullptr || old_superversion->current != current() ||
      old_superversion->mem != mem_ ||
      old_superversion->imm != imm_.current()) {
    // Should not recalculate slow down condition if nothing has changed,
    // since currently RecalculateWriteStallConditions() treats it as
    // further slowing down is needed.
    super_version_->write_stall_condition =
        RecalculateWriteStallConditions(mutable_cf_options);
  } else {
    super_version_->write_stall_condition =
        old_superversion->write_stall_condition;
  }
  if (old_superversion != nullptr) {
    // Reset SuperVersions cached in thread local storage.
    // This should be done before old_superversion->Unref(). That's to
    // ensure that local_sv_ never holds the last reference to
    // SuperVersion, since it has no means to safely do SuperVersion
    // cleanup.
    ResetThreadLocalSuperVersions();

    if (old_superversion->mutable_cf_options.write_buffer_size !=
        mutable_cf_options.write_buffer_size) {
      mem_->UpdateWriteBufferSize(mutable_cf_options.write_buffer_size);
    }
    if (old_superversion->write_stall_condition !=
        new_superversion->write_stall_condition) {
      sv_context->PushWriteStallNotification(
          old_superversion->write_stall_condition,
          new_superversion->write_stall_condition, GetName(), ioptions());
    }
    if (old_superversion->Unref()) {
      old_superversion->Cleanup();
      sv_context->superversions_to_free.push_back(old_superversion);
    }
  }
}

void ColumnFamilyData::ResetThreadLocalSuperVersions() {
  autovector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    bool was_last_ref __attribute__((__unused__));
    was_last_ref = sv->Unref();
    // sv couldn't have been the last reference because
    // ResetThreadLocalSuperVersions() is called before
    // unref'ing super_version_.
    assert(!was_last_ref);
  }
}

Status ColumnFamilyData::ValidateOptions(
    const DBOptions& db_options, const ColumnFamilyOptions& cf_options) {
  Status s;
  s = CheckCompressionSupported(cf_options);
  if (s.ok() && db_options.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(cf_options);
  }
  if (s.ok() && db_options.unordered_write &&
      cf_options.max_successive_merges != 0) {
    s = Status::InvalidArgument(
        "max_successive_merges > 0 is incompatible with "
        "unordered_write");
  }
  if (s.ok()) {
    s = CheckCFPathsSupported(db_options, cf_options);
  }
  if (!s.ok()) {
    return s;
  }

  if (cf_options.ttl > 0 && cf_options.ttl != kDefaultTtl) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
    }
  }

  if (cf_options.periodic_compaction_seconds > 0 &&
      cf_options.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
    }
  }

  if (cf_options.enable_blob_garbage_collection) {
    if (cf_options.blob_garbage_collection_age_cutoff < 0.0 ||
        cf_options.blob_garbage_collection_age_cutoff > 1.0) {
      return Status::InvalidArgument(
          "The age cutoff for blob garbage collection should be in the "
          "range "
          "[0.0, 1.0].");
    }
    if (cf_options.blob_garbage_collection_force_threshold < 0.0 ||
        cf_options.blob_garbage_collection_force_threshold > 1.0) {
      return Status::InvalidArgument(
          "The garbage ratio threshold for forcing blob garbage "
          "collection "
          "should be in the range [0.0, 1.0].");
    }
  }

  if (cf_options.compaction_style == kCompactionStyleFIFO &&
      db_options.max_open_files != -1 && cf_options.ttl > 0) {
    return Status::NotSupported(
        "FIFO compaction only supported with max_open_files = -1.");
  }

  std::vector<uint32_t> supported{0, 1, 2, 4, 8};
  if (std::find(supported.begin(), supported.end(),
                cf_options.memtable_protection_bytes_per_key) ==
      supported.end()) {
    return Status::NotSupported(
        "Memtable per key-value checksum protection only supports 0, 1, "
        "2, 4 "
        "or 8 bytes per key.");
  }
  return s;
}

Status ColumnFamilyData::SetOptions(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& options_map) {
  ColumnFamilyOptions cf_opts =
      BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
  ConfigOptions config_opts;
  config_opts.mutable_options_only = true;
  Status s = GetColumnFamilyOptionsFromMap(config_opts, cf_opts, options_map,
                                           &cf_opts);
  if (s.ok()) {
    s = ValidateOptions(db_opts, cf_opts);
  }
  if (s.ok()) {
    mutable_cf_options_ = MutableCFOptions(cf_opts);
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  }
  return s;
}

// REQUIRES: DB mutex held
Env::WriteLifeTimeHint ColumnFamilyData::CalculateSSTWriteHint(int level) {
  if (initial_cf_options_.compaction_style != kCompactionStyleLevel) {
    return Env::WLTH_NOT_SET;
  }
  if (level == 0) {
    return Env::WLTH_MEDIUM;
  }
  LOG("[error] unchecked branch");
  int base_level = current_->storage_info()->base_level();

  // L1: medium, L2: long, ...
  if (level - base_level >= 2) {
    return Env::WLTH_EXTREME;
  } else if (level < base_level) {
    // There is no restriction which prevents level passed in to be
    // smaller than base_level.
    return Env::WLTH_MEDIUM;
  }
  return static_cast<Env::WriteLifeTimeHint>(
      level - base_level + static_cast<int>(Env::WLTH_MEDIUM));
}

Status ColumnFamilyData::AddDirectories(
    std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs) {
  Status s;
  assert(created_dirs != nullptr);
  assert(data_dirs_.empty());
  for (auto& p : ioptions_.cf_paths) {
    auto existing_dir = created_dirs->find(p.path);

    if (existing_dir == created_dirs->end()) {
      std::unique_ptr<FSDirectory> path_directory;
      s = DBImpl::CreateAndNewDirectory(ioptions_.fs.get(), p.path,
                                        &path_directory);
      if (!s.ok()) {
        return s;
      }
      assert(path_directory != nullptr);
      data_dirs_.emplace_back(path_directory.release());
      (*created_dirs)[p.path] = data_dirs_.back();
    } else {
      data_dirs_.emplace_back(existing_dir->second);
    }
  }
  assert(data_dirs_.size() == ioptions_.cf_paths.size());
  return s;
}

FSDirectory* ColumnFamilyData::GetDataDir(size_t path_id) const {
  if (data_dirs_.empty()) {
    return nullptr;
  }

  assert(path_id < data_dirs_.size());
  return data_dirs_[path_id].get();
}

void ColumnFamilyData::RecoverEpochNumbers() {
  assert(current_);
  auto* vstorage = current_->storage_info();
  assert(vstorage);
  vstorage->RecoverEpochNumbers(this);
}

Status ColumnFamilyData::init_cf_level_rdma_client(std::string& ip, int port) {
  if (cflevel_client_ != nullptr ||
      ColumnFamilyData::kDummyColumnFamilyDataId == GetID()) {
    LOG_CERR("already init cfd or dummy cfd: ", GetID());
    return Status::OK();
  } else {
    LOG_CERR("init cfd: ", GetID());
  }
  Status s = Status::OK();
  cflevel_client_ = new RDMAClient();
  reginfo_ = new struct built_memreg_info;
  memtable_ip_port->first = ip;
  memtable_ip_port->second = port;

  cflevel_read_client_ = new RDMAReadClient();

  size_t block_size_ =
      Arena::OptimizeBlockSize((mutable_cf_options_.write_buffer_size + 10240));
  // memory for each column family
  size_t maintain_mr_size =
      (100 /*memtable index*/ + (block_size_ << 3) /*memtable meta and data*/ +
       1000 /*read request*/) *
      initial_cf_options_.max_write_buffer_number;
  size_t maintain_rr_size = (cflevel_read_client_->config.max_recv_wr +
                             cflevel_read_client_->config.max_recv_wr + 100) *
                            (sizeof(imm_read_req_v2) + sizeof(imm_read_ret));
  cflevel_client_->resources_create(maintain_mr_size);
  cflevel_client_->rdma_mem_.init(maintain_mr_size);
  cflevel_read_client_->resources_create(maintain_rr_size);
  cflevel_read_client_->rdma_mem_.init(maintain_rr_size);

  for (size_t i = 0; i < 2; i++) {
    auto conn = cflevel_read_client_->sock_connect(ip, port);
    if (conn == nullptr) {
      fprintf(stderr, "delegated_read_conns_ connect failed\n");
      s = Status::IOError("delegated_read_conns_ connect failed");
      return s;
    }
    ASSERT_RW(cflevel_read_client_->register_client_in_get_service_request(
        conn, true));
    delegated_read_conns_.enqueue(conn);
    LOG_CERR("delegated_read_conns_ register_client_in_get_service_request: ",
             i);
  }

  meta_conn_ = cflevel_client_->sock_connect(ip, port);
  if (meta_conn_ == nullptr) {
    fprintf(stderr, "meta_conn_ connect failed\n");
    s = Status::IOError("meta_conn_ connect failed");
    return s;
  }

  gc_conn_ = cflevel_client_->sock_connect(ip, port);
  if (gc_conn_ == nullptr) {
    fprintf(stderr, "gc_conn_ connect failed\n");
    s = Status::IOError("gc_conn_ connect failed");
    return s;
  }
  if (gc_thread_ == nullptr) {
    gc_thread_ = new std::thread([this]() {
      while (!gc_queue_.empty()) {
        gc_queue_.pop();
      }

      while (!should_drop.load()) {
        while (!gc_queue_.empty()) {
          auto req = gc_queue_.front();
          gc_queue_.pop();
          while (Env::Default()->NowMicros() - req.second <= 1000000) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
          char req_type = 7;
          ASSERT_RW(writen(gc_conn_->sock, &req_type, sizeof(char)) ==
                    sizeof(char));
          ASSERT_RW(writen(gc_conn_->sock, &req.first, sizeof(uint64_t)) ==
                    sizeof(uint64_t));
          char ret = 0;
          ASSERT_RW(readn(gc_conn_->sock, &ret, sizeof(char)) == sizeof(char));
          assert(ret == 1 || ret == 2);
        }
      }
    });
  }
  // allocate mem for meta
  char req_type = 9;
  ASSERT_RW(writen(meta_conn_.load()->sock, reinterpret_cast<void*>(&req_type),
                   sizeof(char)) == sizeof(char));
  auto meta_offset = cflevel_client_->rdma_mem_.allocate(98304);
  auto remote_meta_reg =
      cflevel_client_->allocate_mem_request(meta_conn_, 98304);
  reginfo_->rf_meta_local_offset = meta_offset;
  reginfo_->rf_meta_remote_offset.first = remote_meta_reg.first;
  reginfo_->rf_meta_remote_offset.second = remote_meta_reg.second;

  for (int i = 0; i < initial_cf_options_.max_write_buffer_number; i++) {
    auto conn_ = cflevel_client_->sock_connect(ip, port);
    if (conn_ == nullptr) {
      fprintf(stderr, "memtable_conn_ connect failed\n");
      s = Status::IOError("memtable_conn_ connect failed");
      return s;
    }
    auto local_index_offset = cflevel_client_->rdma_mem_.allocate(93);
    char req_type = 1;
    ASSERT_RW(writen(conn_->sock, reinterpret_cast<void*>(&req_type),
                     sizeof(char)) == sizeof(char));
    auto reg = cflevel_client_->allocate_mem_request(
        conn_, 93);  // reusable index buffer
    reginfo_->index_mp.insert({conn_, {local_index_offset, reg}});
    memtable_conn_.push(conn_);
  }

  // auto memtable_index_offset =
  //     cflevel_client_->rdma_mem_.allocate(93);  // imm index metadata
  // auto memtable_meta_offset =
  //     cflevel_client_->rdma_mem_.allocate(block_size_);  // imm table
  //     metadata
  // std::vector<int> memtable_data_offset;
  // for (int i = 0; i < 4 /*sep*/; i++) {
  //   memtable_data_offset.push_back(
  //       cflevel_client_->rdma_mem_.allocate(block_size_));  // imm table data
  // }

  // auto remote_memindex_reg =
  //     cflevel_client_->allocate_mem_request(memtable_conn_, 93);
  // // reginfo_->imm_meta_remote_offset.first = remote_memmeta_reg.first;
  // // reginfo_->imm_meta_remote_offset.second = remote_memmeta_reg.second;
  // reginfo_->imm_index_meta_local_offset = memtable_index_offset;
  // reginfo_->imm_index_meta_remote_offset.first = remote_memindex_reg.first;
  // reginfo_->imm_index_meta_remote_offset.second = remote_memindex_reg.second;
  // auto remote_memmeta_reg =
  //     cflevel_client_->allocate_mem_request(memtable_conn_, block_size_);
  // reginfo_->imm_meta_data_local_offset = memtable_meta_offset;
  // reginfo_->imm_meta_data_remote_offset.first = remote_memmeta_reg.first;
  // reginfo_->imm_meta_data_remote_offset.second = remote_memmeta_reg.second;

  // for (int i = 0; i < 4 /*sep*/; i++) {
  //   auto remote_memdata_reg =
  //       cflevel_client_->allocate_mem_request(memtable_conn_, block_size_);
  //   reginfo_->imm_shard_data_local_offset.push_back(memtable_data_offset[i]);
  //   reginfo_->imm_shard_data_remote_offset.push_back(remote_memdata_reg);
  // }
  // fprintf(stderr,
  //         "init_cf_level_rdma_client, rmem-meta: %ld %ld %d rmem-data %ld %ld
  //         "
  //         "%d\n",
  //         reginfo_->imm_meta_remote_offset.first,
  //         reginfo_->imm_meta_remote_offset.second, memtable_meta_offset,
  //         reginfo_->imm_data_remote_offset.first,
  //         reginfo_->imm_data_remote_offset.second, memtable_offset);
  if (s.ok()) s = background_schedule_imm_trans();
  return s;
}

ColumnFamilySet::ColumnFamilySet(const std::string& dbname,
                                 const ImmutableDBOptions* db_options,
                                 const FileOptions& file_options,
                                 Cache* table_cache,
                                 WriteBufferManager* _write_buffer_manager,
                                 WriteController* _write_controller,
                                 BlockCacheTracer* const block_cache_tracer,
                                 const std::shared_ptr<IOTracer>& io_tracer,
                                 const std::string& db_id,
                                 const std::string& db_session_id)
    : max_column_family_(0),
      file_options_(file_options),
      dummy_cfd_(new ColumnFamilyData(
          ColumnFamilyData::kDummyColumnFamilyDataId, "", nullptr, nullptr,
          nullptr, ColumnFamilyOptions(), *db_options, &file_options_, nullptr,
          block_cache_tracer, io_tracer, db_id, db_session_id)),
      default_cfd_cache_(nullptr),
      db_name_(dbname),
      db_options_(db_options),
      table_cache_(table_cache),
      write_buffer_manager_(_write_buffer_manager),
      write_controller_(_write_controller),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_id_(db_id),
      db_session_id_(db_session_id) {
  // initialize linked list
  if (db_options->server_remote_flush != 0) {
    LOG("ColumnFamilySet: check dummy_cfd_ remote flush: true");
    assert(dummy_cfd_->initial_cf_options().server_use_remote_flush == true);
  } else {
    LOG("ColumnFamilySet: check dummy_cfd_ remote flush: false");
    assert(dummy_cfd_->initial_cf_options().server_use_remote_flush == false);
  }
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

ColumnFamilySet::~ColumnFamilySet() {
  LOG("~ColumnFamilySet: delete column_family_data_");
  while (column_family_data_.size() > 0) {
    // cfd destructor will delete itself from column_family_data_
    auto cfd = column_family_data_.begin()->second;
    bool last_ref __attribute__((__unused__));
    last_ref = cfd->UnrefAndTryDelete();
    assert(last_ref);
  }
  LOG("~ColumnFamilySet: delete dummy_cfd_");
  bool dummy_last_ref __attribute__((__unused__));
  dummy_last_ref = dummy_cfd_->UnrefAndTryDelete();
  assert(dummy_last_ref);
  LOG("~ColumnFamilySet: finish");
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  assert(default_cfd_cache_ != nullptr);
  return default_cfd_cache_;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(
    const std::string& name) const {
  auto cfd_iter = column_families_.find(name);
  if (cfd_iter != column_families_.end()) {
    auto cfd = GetColumnFamily(cfd_iter->second);
    assert(cfd != nullptr);
    return cfd;
  } else {
    return nullptr;
  }
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

uint32_t ColumnFamilySet::GetMaxColumnFamily() { return max_column_family_; }

void ColumnFamilySet::UpdateMaxColumnFamily(uint32_t new_max_column_family) {
  max_column_family_ = std::max(new_max_column_family, max_column_family_);
}

size_t ColumnFamilySet::NumberOfColumnFamilies() const {
  return column_families_.size();
}

// under a DB mutex AND write thread
ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options) {
  assert(column_families_.find(name) == column_families_.end());
  LOG("CreateColumnFamily: new cfd,check db_options: remote flush:",
      db_options_->server_remote_flush ? "true" : "false");

  ColumnFamilyData* new_cfd = nullptr;
  new_cfd = new ColumnFamilyData(id, name, dummy_versions, table_cache_,
                                 write_buffer_manager_, options, *db_options_,
                                 &file_options_, this, block_cache_tracer_,
                                 io_tracer_, db_id_, db_session_id_);
  column_families_.insert({name, id});
  column_family_data_.insert({id, new_cfd});
  max_column_family_ = std::max(max_column_family_, id);
  // add to linked list
  new_cfd->next_ = dummy_cfd_;
  auto prev = dummy_cfd_->prev_;
  new_cfd->prev_ = prev;
  prev->next_ = new_cfd;
  dummy_cfd_->prev_ = new_cfd;
  if (id == 0) {
    default_cfd_cache_ = new_cfd;
  }
  LOG("CreateColumnFamily: new cfd finish");
  return new_cfd;
}

// under a DB mutex AND from a write thread
void ColumnFamilySet::RemoveColumnFamily(ColumnFamilyData* cfd) {
  auto cfd_iter = column_family_data_.find(cfd->GetID());
  assert(cfd_iter != column_family_data_.end());
  column_family_data_.erase(cfd_iter);
  column_families_.erase(cfd->GetName());
}

// under a DB mutex OR from a write thread
bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  if (column_family_id == 0) {
    // optimization for common case
    current_ = column_family_set_->GetDefault();
  } else {
    current_ = column_family_set_->GetColumnFamily(column_family_id);
  }
  handle_.SetCFD(current_);
  return current_ != nullptr;
}

uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  assert(current_ != nullptr);
  return current_->GetLogNumber();
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  assert(current_ != nullptr);
  return current_->mem();
}

ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  assert(current_ != nullptr);
  return &handle_;
}

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family) {
  uint32_t column_family_id = 0;
  if (column_family != nullptr) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    column_family_id = cfh->GetID();
  }
  return column_family_id;
}

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family) {
  if (column_family != nullptr) {
    return column_family->GetComparator();
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
