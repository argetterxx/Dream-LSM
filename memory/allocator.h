//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Abstract interface for allocating memory in blocks. This memory is freed
// when the allocator object is destroyed. See the Arena class for more info.

#pragma once
#include <cassert>
#include <cerrno>
#include <cstddef>

#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

class Logger;

class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                Logger* logger = nullptr) = 0;

  virtual size_t BlockSize() const = 0;
  virtual const char* name() const { return "should not use this allocator"; };
};

class BasicArena : public Allocator {
 public:
  virtual void PackLocal(TransferService* node) const = 0;
  virtual void TESTContinuous() const { assert(false); }
  virtual ~BasicArena() {}
  virtual Status SendToRemote() const { assert(false); }
  virtual void get_remote_page_info(uint64_t* info) const { assert(false); }

 public:
  virtual size_t ApproximateMemoryUsage() const = 0;
  virtual size_t MemoryAllocatedBytes() const = 0;
  virtual size_t AllocatedAndUnused() const = 0;
  virtual size_t RawDataUsage() const = 0;
  virtual size_t IrregularBlockNum() const = 0;
  virtual bool IsInInlineBlock() const = 0;
  virtual const char* name() const override {
    return "should not use this allocator";
  };
  virtual const void* MemBegin() const { assert(false); }
};
class AllocTracker {
 public:
  explicit AllocTracker(WriteBufferManager* write_buffer_manager);
  // No copying allowed
  AllocTracker(const AllocTracker&) = delete;
  void operator=(const AllocTracker&) = delete;

  ~AllocTracker();
  void Allocate(size_t bytes);
  // Call when we're finished allocating memory so we can free it from
  // the write buffer's limit.
  void DoneAllocating();

  void FreeMem();

  bool is_freed() const { return write_buffer_manager_ == nullptr || freed_; }

 private:
  WriteBufferManager* write_buffer_manager_;
  std::atomic<size_t> bytes_allocated_;
  bool done_allocating_;
  bool freed_;
};

}  // namespace ROCKSDB_NAMESPACE
