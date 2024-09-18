//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/arena.h"

#include <sys/socket.h>

#include <algorithm>
#include <cstdint>

#include "db/tcprw.h"
#include "logging/logging.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/remote_flush_service.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

void Arena::PackLocal(TransferService* node) const {
  int msg = 2;
  node->send(&msg, sizeof(int));
}

void* Arena::UnPackLocal(TransferService*) {
  void* arena = reinterpret_cast<void*>(new Arena());
  return arena;
}

size_t Arena::OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

Arena::Arena(size_t block_size, AllocTracker* tracker, size_t huge_page_size,
             RDMAClient* client, RDMANode::rdma_connection* conn)
    : kBlockSize(OptimizeBlockSize(block_size)),
      tracker_(tracker),
      client_(client),
      conn_(conn) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  TEST_SYNC_POINT_CALLBACK("Arena::Arena:0", const_cast<size_t*>(&kBlockSize));
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
  if (MemMapping::kHugePageSupported) {
    hugetlb_size_ = huge_page_size;
    if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
      hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
    }
  }
  if (tracker_ != nullptr) {
    tracker_->Allocate(kInlineSize);
  }
  size_t size = kBlockSize;
  char* block_head = AllocateNewBlock(size);
  alloc_bytes_remaining_ = size;
  aligned_alloc_ptr_ = block_head;
  unaligned_alloc_ptr_ = block_head + size;
  mem_begin_ = block_head;
}

Arena::~Arena() {
  if (tracker_ != nullptr) {
    assert(tracker_->is_freed());
    tracker_->FreeMem();
  }
  if (client_ != nullptr && conn_ != nullptr) {
    auto offset = mem_begin_ - client_->get_buf();
    client_->rdma_mem_.free(offset);
    auto ret = blocks_[0].release();
    assert(ret == mem_begin_ && blocks_.size() == 1);
    blocks_.clear();
  }
}

char* Arena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateNewBlock(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes) {
  MemMapping mm = MemMapping::AllocateHuge(bytes);
  auto addr = static_cast<char*>(mm.Get());
  if (addr) {
    huge_blocks_.push_back(std::move(mm));
    blocks_memory_ += bytes;
    if (tracker_ != nullptr) {
      tracker_->Allocate(bytes);
    }
  }
  return addr;
}

char* Arena::AllocateAligned(size_t bytes, size_t huge_page_size,
                             Logger* logger) {
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0 &&
      huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TLB table.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     errnoStr(errno).c_str());
      // fail back to malloc
    } else {
      return addr;
    }
  }

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  // NOTE: std::make_unique zero-initializes the block so is not appropriate
  // here
  char* block = nullptr;
  if (client_ != nullptr && conn_ != nullptr && blocks_.empty()) {
    auto offset = client_->rdma_mem_.allocate(block_bytes);
    if (offset == -1)
      LOG_CERR("Failed to allocate memory from RDMA client mempool");
    auto remote_reg = client_->allocate_mem_request(conn_, block_bytes);
    remote_reg_mem = {remote_reg.first, remote_reg.second - remote_reg.first};
    block = client_->get_buf() + offset;
    // block = new char[block_bytes];
  } else {
    block = new char[block_bytes];
    if (conn_ != nullptr && client_ != nullptr) {
      LOG_CERR("Arena Allocate New Builtin Block!!!:: ", conn_->sock,
               "AllocateNewBlock: ", block_bytes,
               " CHECK::", alloc_bytes_remaining_, ' ', kBlockSize, ' ',
               blocks_.size());
    }
  }
  blocks_.push_back(std::unique_ptr<char[]>(block));

  size_t allocated_size = block_bytes;

  blocks_memory_ += allocated_size;
  if (tracker_ != nullptr) {
    tracker_->Allocate(allocated_size);
  }
  return block;
}

Status Arena::SendToRemote() const {
  Status s;
  if (client_ != nullptr && conn_ != nullptr) {
    std::chrono::high_resolution_clock::time_point start_time =
        std::chrono::high_resolution_clock::now();
    client_->rdma_write(conn_, BlockSize(), mem_begin_ - client_->get_buf(),
                        remote_reg_mem.first);
    ASSERT_RW(client_->poll_completion(conn_) == 0);
    std::chrono::high_resolution_clock::time_point end_time =
        std::chrono::high_resolution_clock::now();
    LOG_CERR("Arena SendToRemote: ",
             std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                   start_time)
                 .count(),
             " us, size = ", BlockSize());
  }
  return s;
}

void Arena::get_remote_page_info(uint64_t* info) const {
  if (client_ != nullptr && conn_ != nullptr) {
    info[0] = remote_reg_mem.first;
    info[1] = remote_reg_mem.second;
  }
}

}  // namespace ROCKSDB_NAMESPACE
