
#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
#include "port/lang.h"
#include "port/likely.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/macro.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "util/core_local.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

class TransConcurrentArena : public BasicArena {
 public:
  void PackLocal(TransferService *node) const override {
    int msg = 3;
    node->send(&msg, sizeof(msg));
  }
  static void *UnPackLocal(TransferService *node) {
    void *arena = reinterpret_cast<void *>(
        new ConcurrentArena());  // nolonger need allocate support
    return arena;
  }

 public:
  const char *name() const override { return "TransConcurrentArena"; }
  inline void *begin() const { return const_cast<void *>(begin_address); }
  inline void *end() const { return now_ptr; }
  inline void TESTContinuous() const override {
    LOG_CERR("begin address: ", begin_address, ' ',
             "now_addr: ", reinterpret_cast<void *>(now_ptr), ' ',
             "memory_allocated_bytes_: ", memory_allocated_bytes_, ' ',
             "max_allocated_bytes_: ", max_allocated_bytes_, ' ',
             "arena_allocated_and_unused_: ",
             arena_allocated_and_unused_.load(std::memory_order_relaxed));
  }
  explicit TransConcurrentArena(size_t max_memtable_size);
  ~TransConcurrentArena() override { free(const_cast<void *>(begin_address)); }
  char *Allocate(size_t bytes) override { return AllocateImpl(bytes); }
  char *AllocateAligned(size_t bytes,
                        [[maybe_unused]] size_t huge_page_size = 0,
                        [[maybe_unused]] Logger *logger = nullptr) override {
    size_t rounded_up = ((bytes - 1) | (sizeof(void *) - 1)) + 1;
    assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void *) &&
           (rounded_up % sizeof(void *)) == 0);
    return AllocateImpl(rounded_up);
  }

  // // todo: later we can support storing offset instead of pointer
  // using Offset = uint32_t;
  // Offset AllocateV2(size_t bytes) { return AllocateImpl(bytes); }
  // Offset AllocateAlignedV2(size_t bytes) {
  //   size_t rounded_up = ((bytes - 1) | (sizeof(void *) - 1)) + 1;
  //   assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void *) &&
  //          (rounded_up % sizeof(void *)) == 0);
  //   return AllocateImpl(rounded_up);
  // }

  size_t ApproximateMemoryUsage() const override {
    return memory_allocated_bytes_;
  }
  size_t MemoryAllocatedBytes() const override {
    return memory_allocated_bytes_;
  }
  size_t AllocatedAndUnused() const override {
    return arena_allocated_and_unused_.load(std::memory_order_relaxed);
  }
  size_t IrregularBlockNum() const override {
    assert(false);
    return 0;
  }
  size_t BlockSize() const override {
    assert(false);
    return 0;
  }
  bool IsInInlineBlock() const override {
    assert(false);
    return true;
  }
  const size_t get_max_allocated_bytes() const { return max_allocated_bytes_; }

 private:
  const void *begin_address = nullptr;
  char *now_ptr = nullptr;
  std::atomic<size_t> arena_allocated_and_unused_;
  size_t memory_allocated_bytes_;
  size_t max_allocated_bytes_;
  mutable SpinMutex arena_mutex_;

  char *AllocateImpl(size_t bytes) {
    std::lock_guard<SpinMutex> lock(arena_mutex_);
    if (arena_allocated_and_unused_.load(std::memory_order_relaxed) >= bytes) {
      arena_allocated_and_unused_.fetch_sub(bytes, std::memory_order_relaxed);
      memory_allocated_bytes_ += bytes;
      char *old = now_ptr;
      now_ptr += bytes;
      return static_cast<char *>(old);
    } else {
      assert(false);
    }
  }

  TransConcurrentArena(const TransConcurrentArena &) = delete;
  TransConcurrentArena &operator=(const TransConcurrentArena &) = delete;
};
}  // namespace ROCKSDB_NAMESPACE