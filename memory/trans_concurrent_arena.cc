
#pragma once
#include "memory/trans_concurrent_arena.h"

#include <thread>

#include "port/port.h"
#include "rocksdb/remote_flush_service.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
TransConcurrentArena::TransConcurrentArena(size_t max_memtable_size)
    : begin_address(malloc(max_memtable_size + 2500)) {
  int temp_compensate_size = 2500;  // exactly 2112 for now, others reserved
  now_ptr = reinterpret_cast<char*>(const_cast<void*>(begin_address));
  max_allocated_bytes_ = max_memtable_size + temp_compensate_size;
  memory_allocated_bytes_ = 0;
  arena_allocated_and_unused_.store(max_allocated_bytes_);
}

}  // namespace ROCKSDB_NAMESPACE