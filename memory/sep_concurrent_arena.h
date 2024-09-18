
#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <utility>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
#include "memory/prefix_sep.h"
#include "port/lang.h"
#include "port/likely.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "util/core_local.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

class SepConcurrentArena : public BasicArena {
 public:
  void PackLocal(TransferService *node) const override {
    int msg = 5;
    node->send(&msg, sizeof(msg));
  }
  static void *UnPackLocal(TransferService *node) {
    void *arena = reinterpret_cast<void *>(
        new ConcurrentArena());  // nolonger need allocate support
    return arena;
  }

 public:
  const char *name() const override { return "SepConcurrentArena"; }
  inline const void *meta_begin() const { return meta_arena_->MemBegin(); }
  inline const void *meta_end() const {
    return reinterpret_cast<const void *>(
        reinterpret_cast<const char *>(meta_arena_->MemBegin()) +
        meta_arena_->BlockSize());
  }
  inline const void *kv_begin(int sep) const {
    return kv_arena_[sep]->MemBegin();
  }
  inline const void *kv_end(int sep) const {
    return reinterpret_cast<const void *>(
        reinterpret_cast<const char *>(kv_arena_[sep]->MemBegin()) +
        kv_arena_[sep]->BlockSize());
  }

  explicit SepConcurrentArena(size_t max_memtable_size, int sep = 4,
                              RDMAClient *client = nullptr,
                              RDMANode::rdma_connection *conn = nullptr);
  ~SepConcurrentArena() override {
    delete meta_arena_;
    for (int i = 0; i < kv_arena_.size(); i++) {
      delete kv_arena_[i];
    }
  }
  Status SendToRemote() const override;
  void get_remote_page_info(uint64_t *info) const override;

  char *Allocate(size_t bytes) override { return meta_arena_->Allocate(bytes); }
  char *AllocateAligned(size_t bytes,
                        [[maybe_unused]] size_t huge_page_size = 0,
                        [[maybe_unused]] Logger *logger = nullptr) override {
    return meta_arena_->AllocateAligned(bytes);
  }
  char *AllocateKV(size_t bytes, const char *prefix) {
    return kv_arena_[SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
                         prefix)]
        ->Allocate(bytes);
  }
  size_t ApproximateMemoryUsage() const override {
    return meta_arena_->ApproximateMemoryUsage() +
           (kv_arena_[0]->ApproximateMemoryUsage() << 2);
  }
  size_t MemoryAllocatedBytes() const override {
    return meta_arena_->BlockSize();
  }
  size_t AllocatedAndUnused() const override {
    return BlockSize() - ApproximateMemoryUsage();
  }
  size_t IrregularBlockNum() const override { assert(false); }
  size_t BlockSize() const override { return blocksize_; }
  size_t RawBlockSize() const { return meta_arena_->BlockSize(); }
  bool IsInInlineBlock() const override { assert(false); }
  size_t RawDataUsage() const override {
    size_t ret = 0;
    for (auto &arena : kv_arena_) {
      ret += arena->RawDataUsage();
    }
    return meta_arena_->RawDataUsage() + ret;
  }

 private:
  const int sep_ = 4;
  ConcurrentArena *meta_arena_{nullptr};
  std::vector<ConcurrentArena *> kv_arena_;
  const size_t blocksize_ = 0;

  SepConcurrentArena(const SepConcurrentArena &) = delete;
  SepConcurrentArena &operator=(const SepConcurrentArena &) = delete;
};
}  // namespace ROCKSDB_NAMESPACE