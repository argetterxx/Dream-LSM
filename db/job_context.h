//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class MemTable;
struct SuperVersion;

struct SuperVersionContext {
  struct WriteStallNotification {
    WriteStallInfo write_stall_info;
    const ImmutableOptions* immutable_options;
  };

  autovector<SuperVersion*> superversions_to_free;
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
  autovector<WriteStallNotification> write_stall_notifications;
#endif
  std::unique_ptr<SuperVersion>
      new_superversion;  // if nullptr no new superversion

  explicit SuperVersionContext(bool create_superversion = false)
      : new_superversion(create_superversion ? new SuperVersion() : nullptr) {}

  explicit SuperVersionContext(SuperVersionContext&& other) noexcept
      : superversions_to_free(std::move(other.superversions_to_free)),
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
        write_stall_notifications(std::move(other.write_stall_notifications)),
#endif
        new_superversion(std::move(other.new_superversion)) {
  }
  // No copies
  SuperVersionContext(const SuperVersionContext& other) = delete;
  void operator=(const SuperVersionContext& other) = delete;

  void NewSuperVersion() {
    new_superversion = std::unique_ptr<SuperVersion>(new SuperVersion());
  }

  inline bool HaveSomethingToDelete() const {
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
    return !superversions_to_free.empty() || !write_stall_notifications.empty();
#else
    return !superversions_to_free.empty();
#endif
  }

  void PushWriteStallNotification(WriteStallCondition old_cond,
                                  WriteStallCondition new_cond,
                                  const std::string& name,
                                  const ImmutableOptions* ioptions) {
#if !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
    WriteStallNotification notif;
    notif.write_stall_info.cf_name = name;
    notif.write_stall_info.condition.prev = old_cond;
    notif.write_stall_info.condition.cur = new_cond;
    notif.immutable_options = ioptions;
    write_stall_notifications.push_back(notif);
#else
    (void)old_cond;
    (void)new_cond;
    (void)name;
    (void)ioptions;
#endif  // !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
  }

  void Clean() {
#if !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
    // notify listeners on changed write stall conditions
    for (auto& notif : write_stall_notifications) {
      for (auto& listener : notif.immutable_options->listeners) {
        listener->OnStallConditionsChanged(notif.write_stall_info);
      }
    }
    write_stall_notifications.clear();
#endif
    // free superversions
    for (auto s : superversions_to_free) {
      delete s;
    }
    superversions_to_free.clear();
    LOG("SuperVersionContext::Clean() done 2");
  }

  ~SuperVersionContext() {
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
    assert(write_stall_notifications.empty());
#endif
    assert(superversions_to_free.empty());
  }
};

struct JobContext {
 public:
  void PackLocal(TransferService* node) const;
  static void* UnPackLocal(TransferService* node);

 public:
  inline bool HaveSomethingToDelete() const {
    return !(full_scan_candidate_files.empty() && sst_delete_files.empty() &&
             blob_delete_files.empty() && log_delete_files.empty() &&
             manifest_delete_files.empty());
  }

  inline bool HaveSomethingToClean() const {
    bool sv_have_sth = false;
    for (const auto& sv_ctx : superversion_contexts) {
      if (sv_ctx.HaveSomethingToDelete()) {
        sv_have_sth = true;
        break;
      }
    }
    return memtables_to_free.size() > 0 || logs_to_free.size() > 0 ||
           job_snapshot != nullptr || sv_have_sth;
  }

  SequenceNumber GetJobSnapshotSequence() const {
    if (job_snapshot) {
      assert(job_snapshot->snapshot());
      return job_snapshot->snapshot()->GetSequenceNumber();
    }
    return kMaxSequenceNumber;
  }

  // Structure to store information for candidate files to delete.
  struct CandidateFileInfo {
    std::string file_name;
    std::string file_path;
    CandidateFileInfo(std::string name, std::string path)
        : file_name(std::move(name)), file_path(std::move(path)) {}
    bool operator==(const CandidateFileInfo& other) const {
      return file_name == other.file_name && file_path == other.file_path;
    }
  };

  // Unique job id
  int job_id;

  // a list of all files that we'll consider deleting
  // (every once in a while this is filled up with all files
  // in the DB directory)
  // (filled only if we're doing full scan)
  std::vector<CandidateFileInfo> full_scan_candidate_files;

  // the list of all live sst files that cannot be deleted
  std::vector<uint64_t> sst_live;

  // the list of sst files that we need to delete
  std::vector<ObsoleteFileInfo> sst_delete_files;

  // the list of all live blob files that cannot be deleted
  std::vector<uint64_t> blob_live;

  // the list of blob files that we need to delete
  std::vector<ObsoleteBlobFileInfo> blob_delete_files;

  // a list of log files that we need to delete
  std::vector<uint64_t> log_delete_files;

  // a list of log files that we need to preserve during full purge since they
  // will be reused later
  std::vector<uint64_t> log_recycle_files;

  // a list of manifest files that we need to delete
  std::vector<std::string> manifest_delete_files;

  // a list of memtables to be free
  autovector<MemTable*> memtables_to_free;

  // contexts for installing superversions for multiple column families
  std::vector<SuperVersionContext> superversion_contexts;

  autovector<log::Writer*> logs_to_free;

  // the current manifest_file_number, log_number and prev_log_number
  // that corresponds to the set of files in 'live'.
  uint64_t manifest_file_number;
  uint64_t pending_manifest_file_number;
  uint64_t log_number;
  uint64_t prev_log_number;

  uint64_t min_pending_output = 0;
  uint64_t prev_total_log_size = 0;
  size_t num_alive_log_files = 0;
  uint64_t size_log_to_delete = 0;

  // Snapshot taken before flush/compaction job.
  std::unique_ptr<ManagedSnapshot> job_snapshot;

  explicit JobContext(int _job_id, bool create_superversion = false) {
    job_id = _job_id;
    manifest_file_number = 0;
    pending_manifest_file_number = 0;
    log_number = 0;
    prev_log_number = 0;
    superversion_contexts.emplace_back(
        SuperVersionContext(create_superversion));
  }

  // For non-empty JobContext Clean() has to be called at least once before
  // before destruction (see asserts in ~JobContext()). Should be called with
  // unlocked DB mutex. Destructor doesn't call Clean() to avoid accidentally
  // doing potentially slow Clean() with locked DB mutex.
  void Clean() {
    // free superversions
    LOG("JobContext::Clean()");
    for (auto& sv_context : superversion_contexts) {
      sv_context.Clean();
    }
    // free pending memtables
    for (auto m : memtables_to_free) {
      delete m;
    }
    for (auto l : logs_to_free) {
      delete l;
    }

    memtables_to_free.clear();
    logs_to_free.clear();
    job_snapshot.reset();
    LOG("JobContext::Clean() done 4");
  }

  ~JobContext() {
    assert(memtables_to_free.size() == 0);
    assert(logs_to_free.size() == 0);
  }
};

inline void JobContext::PackLocal(TransferService* node) const {
  LOG("JobContext::PackLocal");
  node->send(reinterpret_cast<const void*>(this), sizeof(JobContext));
  assert(job_snapshot == nullptr);
}

inline void* JobContext::UnPackLocal(TransferService* node) {
  size_t empty = 0;
  node->receive(&empty, sizeof(size_t));
  if (empty == 0) {
    LOG("JobContext::UnPackLocal empty");
    return nullptr;
  }
  LOG("JobContext::UnPackLocal");
  void* mem = malloc(sizeof(JobContext));
  node->receive(mem, sizeof(JobContext));
  return mem;
}

}  // namespace ROCKSDB_NAMESPACE
