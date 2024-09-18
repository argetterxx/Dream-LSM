#pragma once
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstddef>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
#include "memory/sep_concurrent_arena.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {
class BasicArenaFactory {
 public:
  static BasicArena* UnPackLocal(TransferService* node) {
    int msg = 0;
    node->receive(&msg, sizeof(int));
    if (msg == 1) {
      return reinterpret_cast<BasicArena*>(ConcurrentArena::UnPackLocal(node));
    } else if (msg == 2) {
      return reinterpret_cast<BasicArena*>(Arena::UnPackLocal(node));
    } else if (msg == 3) {
      return reinterpret_cast<BasicArena*>(
          SepConcurrentArena::UnPackLocal(node));
    } else {
      assert(false);
      return nullptr;
    }
  }

 private:
  BasicArenaFactory() = default;
  BasicArenaFactory(const BasicArenaFactory&) = delete;
  void operator=(const BasicArenaFactory&) = delete;
  ~BasicArenaFactory() = default;
};

}  // namespace ROCKSDB_NAMESPACE