
// #pragma once
// #include "memory/sep_trans_concurrent_arena.h"

// #include <thread>

// #include "port/port.h"
// #include "rocksdb/remote_flush_service.h"
// #include "util/random.h"

// namespace ROCKSDB_NAMESPACE {
// SepTransConcurrentArena::SepTransConcurrentArena(size_t max_memtable_size,
//                                                  int sep)
//     : sep_(sep) {
//   int temp_compensate_size = 2500;  // exactly 2112 for now, others reserved
//   meta_ptr.first = malloc(max_memtable_size + temp_compensate_size);
//   meta_ptr.second = meta_ptr.first;
//   for (int i = 0; i < sep_; i++) {
//     void* ptr = malloc(max_memtable_size + temp_compensate_size);
//     kv_ptrs.emplace_back(std::make_pair(ptr, ptr));
//   }
//   max_allocated_bytes_ = max_memtable_size + temp_compensate_size;
//   memory_allocated_bytes_ = 0;
//   arena_allocated_and_unused_.store(max_allocated_bytes_);
// }

// }  // namespace ROCKSDB_NAMESPACE