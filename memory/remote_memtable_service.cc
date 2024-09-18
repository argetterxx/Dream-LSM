#include "memory/remote_memtable_service.h"

#include <cassert>
#include <cstdint>
#include <mutex>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/sep_concurrent_arena.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {
void RemoteMemTable::register_remote_memTable(
    RemoteMemTable*& rmt, void* rdma_buf, void* index, uint64_t index_size,
    void* mem_meta, uint64_t mem_meta_size,
    std::pair<void*, uint64_t>* mem_data) {
  rmt = new RemoteMemTable();
  rmt->meta =
      reinterpret_cast<char*>(mem_meta) - reinterpret_cast<char*>(rdma_buf);
  rmt->meta_size = mem_meta_size;
  rmt->index =
      reinterpret_cast<char*>(index) - reinterpret_cast<char*>(rdma_buf);
  rmt->index_size = index_size;
  rmt->data.resize(4);
  for (int i = 0; i < 4; i++) {
    rmt->data[i].first = reinterpret_cast<char*>(mem_data[i].first) -
                         reinterpret_cast<char*>(rdma_buf);
    rmt->data[i].second = mem_data[i].second;
  }
}

void RemoteMemTable::rebuild_remote_memTable(
    RemoteMemTable* rmt, void* rdma_buf, void* index, uint64_t index_size,
    void* mem_meta, uint64_t mem_meta_size,
    std::pair<void*, uint64_t>* mem_data) {
  if (rmt == nullptr) {
    LOG_CERR("rebuild_remote_memTable rmt is nullptr");
    return;
  }
  uint64_t id_ = 0;
  int64_t head_offset_ = 0;
  int32_t max_height = 1;
  bool cmp_id = false;
  std::pair<int64_t, int64_t> transform_id = {0, 0};
  size_t lookahead_ = 0;
  void* skip_list_ptr_ = nullptr;
  void* meta_begin_ptr_ = nullptr;
  std::vector<void*> data_begin_ptr_;
  data_begin_ptr_.resize(4 /*sep*/);

  //   parse index
  char* idx_ptr = reinterpret_cast<char*>(index);
  id_ = *reinterpret_cast<uint64_t*>(idx_ptr);
  idx_ptr += sizeof(uint64_t);
  head_offset_ = *reinterpret_cast<int64_t*>(idx_ptr);
  idx_ptr += sizeof(int64_t);
  max_height = *reinterpret_cast<int32_t*>(idx_ptr);
  idx_ptr += sizeof(int32_t);
  cmp_id = *reinterpret_cast<bool*>(idx_ptr);
  idx_ptr += sizeof(bool);
  transform_id.first = *reinterpret_cast<int64_t*>(idx_ptr);
  idx_ptr += sizeof(int64_t);
  if (transform_id.first == 0) {
    transform_id.second = *reinterpret_cast<int64_t*>(idx_ptr);
    idx_ptr += sizeof(int64_t);
  }
  lookahead_ = *reinterpret_cast<size_t*>(idx_ptr);
  idx_ptr += sizeof(size_t);
  skip_list_ptr_ = *reinterpret_cast<void**>(idx_ptr);
  idx_ptr += sizeof(void*);
  meta_begin_ptr_ = *reinterpret_cast<void**>(idx_ptr);
  idx_ptr += sizeof(void*);
  for (int i = 0; i < 4 /*sep*/; i++) {
    data_begin_ptr_[i] = *reinterpret_cast<void**>(idx_ptr);
    idx_ptr += sizeof(void*);
  }

  // rebuild
  rmt->id = id_;

  LOG_CERR("rebuild rmem id:", id_, ' ', "head_offset:", head_offset_, ' ',
           "cmp_id:", cmp_id, ' ', "transform_id:", transform_id.first, ' ',
           "lookahead:", lookahead_, ' ', "skip_list_ptr:", skip_list_ptr_);

  auto* key_cmp = new MemTable::KeyComparator(
      (!cmp_id) ? (InternalKeyComparator(BytewiseComparator()))
                : InternalKeyComparator(ReverseBytewiseComparator()));
  rmt->key_cmp = key_cmp;
  auto* arena = new SepConcurrentArena((16 << 10));
  rmt->arena = arena;
  const SliceTransform* prefix_extractor = nullptr;
  if (transform_id.first == 0xff) {
    prefix_extractor = nullptr;
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 1)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewFixedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 2)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewCappedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 3)) {
    prefix_extractor = new InternalKeySliceTransform(NewNoopTransform());
  } else if ((transform_id.first & 0xff) == 1) {
    prefix_extractor = NewFixedPrefixTransform(transform_id.first >> 8);
  } else if ((transform_id.first & 0xff) == 2) {
    prefix_extractor = NewCappedPrefixTransform(transform_id.first >> 8);
  } else if ((transform_id.first & 0xff) == 3) {
    prefix_extractor = NewNoopTransform();
  } else {
    assert(false);
    prefix_extractor = nullptr;
  }
  rmt->prefix_extractor = const_cast<SliceTransform*>(prefix_extractor);

  MemTableRep* rmt_rep =
      SkipListFactory(lookahead_)
          .CreateMemTableRep(*key_cmp, arena, prefix_extractor, nullptr);
  //   rmt_rep->MemnodeRebuild(skip_list_ptr_);
  rmt_rep->set_local_begin(meta_begin_ptr_);
  rmt_rep->set_remote_begin(mem_meta);
  rmt_rep->set_head_offset(head_offset_);
  for (int i = 0; i < 4 /*sep*/; i++) {
    rmt_rep->set_shard_local_begin(i, data_begin_ptr_[i]);
    rmt_rep->set_shard_remote_begin(i, mem_data[i].first);
  }
  rmt_rep->set_max_height(max_height);
  rmt->memtable = rmt_rep;
}

Status RemoteMemTablePool::rebuild_remote_memtable(
    void* rdma_buf, uint64_t index, uint64_t index_size, uint64_t mem_meta,
    uint64_t mem_meta_size, uint64_t* mem_data) {
  Status s = Status::OK();
  void* index_ = reinterpret_cast<char*>(rdma_buf) + index;
  uint64_t id_ = *reinterpret_cast<uint64_t*>(index_);
  std::pair<void*, uint64_t> mem_data_[4];
  for (int i = 0; i < 4 /*sep*/; i++) {
    mem_data_[i].first = reinterpret_cast<char*>(rdma_buf) + mem_data[i * 2];
    mem_data_[i].second = mem_data[i * 2 + 1];
  }
  RemoteMemTable* rmt = nullptr;
  RemoteMemTable::register_remote_memTable(
      rmt, rdma_buf, reinterpret_cast<char*>(rdma_buf) + index, index_size,
      reinterpret_cast<char*>(rdma_buf) + mem_meta, mem_meta_size, mem_data_);
  {
    std::lock_guard<std::mutex> lck(mtx_);
    if (id2ptr_.find(id_) != id2ptr_.end()) {
      fprintf(stderr, "rebuild_remote_memtable id %lu already exists\n", id_);
      delete rmt;
      return Status::Expired();
    } else {
      id2ptr_[id_] = rmt;
    }
  }
  RemoteMemTable::rebuild_remote_memTable(
      rmt, rdma_buf, reinterpret_cast<char*>(rdma_buf) + index, index_size,
      reinterpret_cast<char*>(rdma_buf) + mem_meta, mem_meta_size, mem_data_);
  return s;
}

void RemoteMemTable::remote_get_v2(void* req_data_v2, void* ret_data, void* rdma_buf) {
  assert(memtable != nullptr);
  memtable->RGet_v2(&(key_cmp->comparator), req_data_v2, ret_data, rdma_buf);
}

}  // namespace ROCKSDB_NAMESPACE