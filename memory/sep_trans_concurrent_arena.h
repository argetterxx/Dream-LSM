
// #pragma once
// #include <atomic>
// #include <cassert>
// #include <cstddef>
// #include <cstdint>
// #include <cstdlib>
// #include <memory>
// #include <mutex>
// #include <utility>

// #include "memory/allocator.h"
// #include "memory/arena.h"
// #include "memory/concurrent_arena.h"
// #include "memory/prefix_sep.h"
// #include "port/lang.h"
// #include "port/likely.h"
// #include "rocksdb/logger.hpp"
// #include "rocksdb/macro.hpp"
// #include "rocksdb/remote_flush_service.h"
// #include "rocksdb/remote_transfer_service.h"
// #include "util/core_local.h"
// #include "util/mutexlock.h"
// #include "util/thread_local.h"

// namespace ROCKSDB_NAMESPACE {
// class Logger;

// class SepTransConcurrentArena : public BasicArena {
//  public:
//   void PackLocal(TransferService *node) const override {
//     int msg = 4;
//     node->send(&msg, sizeof(msg));
//   }
//   static void *UnPackLocal(TransferService *node) {
//     void *arena = reinterpret_cast<void *>(
//         new ConcurrentArena());  // nolonger need allocate support
//     return arena;
//   }

//  public:
//   const char *name() const override { return "SepTransConcurrentArena"; }
//   inline void *meta_begin() const { return const_cast<void
//   *>(meta_ptr.first); } inline void *meta_end() const { return
//   const_cast<void *>(meta_ptr.second); } inline void *kv_begin(int sep) const
//   {
//     return const_cast<void *>(kv_ptrs[sep].first);
//   }
//   inline void *kv_end(int sep) const {
//     return const_cast<void *>(kv_ptrs[sep].second);
//   }
//   inline void TESTContinuous() const override {
//     // LOG_CERR("begin address: ", begin_address, ' ',
//     //          "now_addr: ", reinterpret_cast<void *>(now_ptr), ' ',
//     //          "memory_allocated_bytes_: ", memory_allocated_bytes_, ' ',
//     //          "max_allocated_bytes_: ", max_allocated_bytes_, ' ',
//     //          "arena_allocated_and_unused_: ",
//     //          arena_allocated_and_unused_.load(std::memory_order_relaxed));
//   }
//   explicit SepTransConcurrentArena(size_t max_memtable_size, int sep = 4);
//   ~SepTransConcurrentArena() override {
//     free(meta_ptr.first);
//     for (auto &kv : kv_ptrs) free(kv.first);
//   }
//   char *Allocate(size_t bytes) override { return AllocateMetaImpl(bytes); }
//   char *AllocateAligned(size_t bytes,
//                         [[maybe_unused]] size_t huge_page_size = 0,
//                         [[maybe_unused]] Logger *logger = nullptr) override {
//     size_t rounded_up = ((bytes - 1) | (sizeof(void *) - 1)) + 1;
//     assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void *) &&
//            (rounded_up % sizeof(void *)) == 0);
//     return AllocateMetaImpl(rounded_up);
//   }
//   char *AllocateKV(size_t bytes, const char *prefix) {
//     std::lock_guard<SpinMutex> lock(arena_mutex_);
//     if (arena_allocated_and_unused_.load(std::memory_order_relaxed) >= bytes)
//     {
//       arena_allocated_and_unused_.fetch_sub(bytes,
//       std::memory_order_relaxed); memory_allocated_bytes_ += bytes; int sep =
//           SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(prefix);
//       char *old = reinterpret_cast<char *>(kv_ptrs[sep].second);
//       kv_ptrs[sep].second = reinterpret_cast<void *>(
//           reinterpret_cast<char *>(kv_ptrs[sep].second) + bytes);
//       return static_cast<char *>(old);
//     } else {
//       assert(false);
//     }
//   }

//   size_t ApproximateMemoryUsage() const override {
//     return memory_allocated_bytes_;
//   }
//   size_t MemoryAllocatedBytes() const override {
//     return memory_allocated_bytes_;
//   }
//   size_t AllocatedAndUnused() const override {
//     return arena_allocated_and_unused_.load(std::memory_order_relaxed);
//   }
//   size_t IrregularBlockNum() const override {
//     assert(false);
//     return 0;
//   }
//   size_t BlockSize() const override {
//     assert(false);
//     return 0;
//   }
//   bool IsInInlineBlock() const override {
//     assert(false);
//     return true;
//   }
//   const size_t get_max_allocated_bytes() const { return max_allocated_bytes_;
//   }

//  private:
//   const int sep_ = 4;
//   std::pair<void *, void *> meta_ptr{nullptr, nullptr};
//   std::vector<std::pair</*const*/ void *, void *>> kv_ptrs;

//   std::atomic<size_t> arena_allocated_and_unused_;
//   size_t memory_allocated_bytes_;
//   size_t max_allocated_bytes_;
//   mutable SpinMutex arena_mutex_;

//   char *AllocateMetaImpl(size_t bytes) {
//     std::lock_guard<SpinMutex> lock(arena_mutex_);
//     if (arena_allocated_and_unused_.load(std::memory_order_relaxed) >= bytes)
//     {
//       arena_allocated_and_unused_.fetch_sub(bytes,
//       std::memory_order_relaxed); memory_allocated_bytes_ += bytes; char *old
//       = reinterpret_cast<char *>(meta_ptr.second); meta_ptr.second =
//       reinterpret_cast<void *>(
//           reinterpret_cast<char *>(meta_ptr.second) + bytes);
//       return static_cast<char *>(old);
//     } else {
//       assert(false);
//     }
//   }
//   SepTransConcurrentArena(const SepTransConcurrentArena &) = delete;
//   SepTransConcurrentArena &operator=(const SepTransConcurrentArena &) =
//   delete;
// };
// }  // namespace ROCKSDB_NAMESPACE