#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "memory/concurrent_arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"

#ifdef ROCKSDB_ALL_TESTS_ENABLED
#include "test_util/testutil.h"
#endif  // ROCKSDB_ALL_TESTS_ENABLED

namespace ROCKSDB_NAMESPACE {
class MemTableRepPackFactory {
 public:
  static void* UnPackLocal(TransferService* node,
                           const MemTableRep::KeyComparator* compare = nullptr,
                           Allocator* alloc = nullptr,
                           const SliceTransform* transform = nullptr,
                           MemTableRep* rep = nullptr);
  MemTableRepPackFactory(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(MemTableRepPackFactory&&) = delete;

 private:
  MemTableRepPackFactory() = default;
  ~MemTableRepPackFactory() = default;
};

inline void* MemTableRepPackFactory::UnPackLocal(
    TransferService* node, const MemTableRep::KeyComparator* compare,
    Allocator* allocator, const SliceTransform* transform, MemTableRep* rep) {
  int64_t msg = -1;
  node->receive(&msg, sizeof(int64_t));
  int64_t type = msg & 0xff;
  [[maybe_unused]] int64_t info = msg >> 8;
  if (type == 1 /*SkipListRep:: range_del_table_*/) {
    MemTableRep* local_skiplistrep = SkipListFactory().CreateMemTableRep(
        *compare, allocator, transform, nullptr);
    int64_t head_offset_ = 0;
    node->receive(&head_offset_, sizeof(int64_t));
    local_skiplistrep->set_local_begin(const_cast<void*>(
        reinterpret_cast<const void*>(rep->local_begin().first)));
    local_skiplistrep->set_remote_begin(rep->remote_begin().first);
    local_skiplistrep->set_head_offset(head_offset_);
    for (int i = 0; i < 4 /*sep*/; i++) {
      local_skiplistrep->set_shard_local_begin(
          i, rep->get_shard_local_begin(i).first);
      local_skiplistrep->set_shard_remote_begin(
          i, rep->get_shard_remote_begin(i).first);
    }
    int32_t max_height = 1;
    node->receive(&max_height, sizeof(int32_t));
    local_skiplistrep->set_max_height(max_height);
    return local_skiplistrep;
  } else if (type == 2 /*Test SpecialMemTableRep*/) {
    LOG("MemTableRepPackFactory::UnPackLocal: SpecialMemTableRep: ", type, ' ',
        info);
    void* sub_memtable_ = UnPackLocal(node);
    [[maybe_unused]] auto memtable_ =
        reinterpret_cast<MemTableRep*>(sub_memtable_);
#ifdef ROCKSDB_ALL_TESTS_ENABLED
    int num_entries_flush = info;
    MemTableRepFactory* factory =
        test::NewSpecialSkipListFactory(num_entries_flush);
    MemTableRep* ret_memtable_ =
        factory->CreateExistMemTableWrapper(new ConcurrentArena(), memtable_);
    delete factory;
    return reinterpret_cast<void*>(ret_memtable_);
#else
    assert(false);
#endif  // ROCKSDB_ALL_TESTS_ENABLED
  } else {
    LOG_CERR("MemTableRepPackFactory::UnPackLocal error", type, ' ', info);
    assert(false);
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE