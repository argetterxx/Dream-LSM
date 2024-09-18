// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/logger.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

enum class SnapshotCheckerResult : int {
  kInSnapshot = 0,
  kNotInSnapshot = 1,
  // In case snapshot is released and the checker has no clue whether
  // the given sequence is visible to the snapshot.
  kSnapshotReleased = 2,
};

// Callback class that control GC of duplicate keys in flush/compaction.
class SnapshotChecker {
 public:
  virtual void PackLocal([[maybe_unused]] TransferService* node) const {
    LOG("should not use default SnapshotChecker::PackLocal");
    assert(false);
  }

 public:
  virtual ~SnapshotChecker() {}
  virtual SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber sequence, SequenceNumber snapshot_sequence) const = 0;
};

class DisableGCSnapshotChecker : public SnapshotChecker {
 public:
  void PackLocal(TransferService* node) const override {
    size_t msg = 0;
    msg += (0x2);
    node->send(&msg, sizeof(msg));
  }

 public:
  virtual ~DisableGCSnapshotChecker() {}
  virtual SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber /*sequence*/,
      SequenceNumber /*snapshot_sequence*/) const override {
    // By returning kNotInSnapshot, we prevent all the values from being GCed
    return SnapshotCheckerResult::kNotInSnapshot;
  }
  static DisableGCSnapshotChecker* Instance();

 protected:
  explicit DisableGCSnapshotChecker() {}
};

class WritePreparedTxnDB;

// Callback class created by WritePreparedTxnDB to check if a key
// is visible by a snapshot.
class WritePreparedSnapshotChecker : public SnapshotChecker {
 public:
  explicit WritePreparedSnapshotChecker(WritePreparedTxnDB* txn_db);
  virtual ~WritePreparedSnapshotChecker() {}

  virtual SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber sequence, SequenceNumber snapshot_sequence) const override;

 private:
  const WritePreparedTxnDB* const txn_db_;
};

class SnapshotCheckerFactory {
 public:
  static void* UnPackLocal(TransferService* node);

 public:
  SnapshotCheckerFactory& operator=(const SnapshotCheckerFactory&) = delete;
  SnapshotCheckerFactory(const SnapshotCheckerFactory&) = delete;
  SnapshotCheckerFactory(SnapshotCheckerFactory&&) = delete;

 private:
  SnapshotCheckerFactory() = default;
  ~SnapshotCheckerFactory() = default;
};

inline void* SnapshotCheckerFactory::UnPackLocal(TransferService* node) {
  size_t msg = 0;
  node->receive(&msg, sizeof(msg));
  if (msg == 0xff) {
    return nullptr;
  }
  msg = 0;
  node->receive(&msg, sizeof(msg));
  if (msg == 0x02) {
    return DisableGCSnapshotChecker::Instance();
  } else {
    LOG("SnapshotCheckerFactory::UnPackLocal: invalid msg:", msg);
    assert(false);
  }
}

}  // namespace ROCKSDB_NAMESPACE
