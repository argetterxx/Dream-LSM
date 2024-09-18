//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <atomic>
#include <memory>

#include "rocksdb/remote_flush_service.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// A factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries or the ratio of tombstone
// entries in the whole file >= the specified deletion ratio.
class CompactOnDeletionCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  void PackLocal(TransferService* node) const override {
    size_t msg_len = sizeof(size_t) + sizeof(size_t) * 2 + sizeof(double);
    char* msg = reinterpret_cast<char*>(malloc(msg_len));
    *reinterpret_cast<size_t*>(msg) = 2;
    *reinterpret_cast<size_t*>(msg + sizeof(size_t)) =
        sliding_window_size_.load();
    *reinterpret_cast<size_t*>(msg + sizeof(size_t) * 2) =
        deletion_trigger_.load();
    *reinterpret_cast<double*>(msg + sizeof(size_t) * 2 + sizeof(size_t)) =
        deletion_ratio_.load();
    node->send(msg, msg_len);
  }

 public:
  // A factory of a table property collector that marks a SST
  // file as need-compaction when it observe at least "D" deletion
  // entries in any "N" consecutive entries, or the ratio of tombstone
  // entries >= deletion_ratio.
  //
  // @param sliding_window_size "N"
  // @param deletion_trigger "D"
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  CompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                    size_t deletion_trigger,
                                    double deletion_ratio);

  ~CompactOnDeletionCollectorFactory() {}

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  // Change the value of sliding_window_size "N"
  // Setting it to 0 disables the delete triggered compaction
  void SetWindowSize(size_t sliding_window_size) {
    sliding_window_size_.store(sliding_window_size);
  }
  size_t GetWindowSize() const { return sliding_window_size_.load(); }

  // Change the value of deletion_trigger "D"
  void SetDeletionTrigger(size_t deletion_trigger) {
    deletion_trigger_.store(deletion_trigger);
  }

  size_t GetDeletionTrigger() const { return deletion_trigger_.load(); }
  // Change deletion ratio.
  // @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
  //     based on deletion ratio.
  void SetDeletionRatio(double deletion_ratio) {
    deletion_ratio_.store(deletion_ratio);
  }

  double GetDeletionRatio() const { return deletion_ratio_.load(); }
  static const char* kClassName() { return "CompactOnDeletionCollector"; }
  const char* Name() const override { return kClassName(); }

  std::string ToString() const override;

 private:
  std::atomic<size_t> sliding_window_size_;
  std::atomic<size_t> deletion_trigger_;
  std::atomic<double> deletion_ratio_;
};

// Creates a factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entries, or the ratio of tombstone
// entries >= deletion_ratio.
//
// @param sliding_window_size "N". Note that this number will be
//     round up to the smallest multiple of 128 that is no less
//     than the specified size.
// @param deletion_trigger "D".  Note that even when "N" is changed,
//     the specified number for "D" will not be changed.
// @param deletion_ratio, if <= 0 or > 1, disable triggering compaction
//     based on deletion ratio. Disabled by default.
extern std::shared_ptr<CompactOnDeletionCollectorFactory>
NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                     size_t deletion_trigger,
                                     double deletion_ratio = 0);
}  // namespace ROCKSDB_NAMESPACE
