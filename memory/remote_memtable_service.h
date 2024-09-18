#pragma once
#include <cstdint>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "memory/sep_concurrent_arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {
struct RemoteMemTable {
  uint64_t index{0};
  uint64_t index_size{0};
  uint64_t meta{0};
  uint64_t meta_size{0};
  std::vector<std::pair<uint64_t, uint64_t>> data;
  uint64_t id{0};
  MemTableRep* memtable{nullptr};
  // gc
  SepConcurrentArena* arena{nullptr};
  MemTable::KeyComparator* key_cmp{nullptr};
  SliceTransform* prefix_extractor{nullptr};
  static void rebuild_remote_memTable(RemoteMemTable* rmt, void* rdma_buf,
                                      void* index, uint64_t index_size,
                                      void* mem_meta, uint64_t mem_meta_size,
                                      std::pair<void*, uint64_t>* mem_data);
  static void register_remote_memTable(RemoteMemTable*& mem, void* rdma_buf,
                                       void* index, uint64_t index_size,
                                       void* mem_meta, uint64_t mem_meta_size,
                                       std::pair<void*, uint64_t>* mem_data);
  void remote_get_v2(void* req_data_v2, void* ret_data, void* rdma_buf);
};
class DBImpl;
class RemoteMemTablePool {
 public:
  Status rebuild_remote_memtable(void* rdma_buf, uint64_t index,
                                 uint64_t index_size, uint64_t mem_meta,
                                 uint64_t mem_meta_size, uint64_t* mem_data);

  inline Status delete_remote_memtable(uint64_t id, uint64_t* to_unpin) {
    Status s = Status::OK();
    std::lock_guard<std::mutex> lock(mtx_);
    if (id2ptr_.find(id) == id2ptr_.end()) {
      s = Status::NotFound("id not found");
    } else {
      RemoteMemTable* rmem = id2ptr_[id];
      delete rmem->memtable;
      delete rmem->arena;
      delete rmem->key_cmp;
      delete rmem->prefix_extractor;
      LOG_CERR("unpin rmem: ", rmem->id, ' ', id);
      to_unpin[0] = rmem->index;
      to_unpin[1] = rmem->index_size;
      to_unpin[2] = rmem->meta;
      to_unpin[3] = rmem->meta_size;
      for (int i = 0; i < rmem->data.size(); i++) {
        to_unpin[4 + i * 2] = rmem->data[i].first;
        to_unpin[5 + i * 2] = rmem->data[i].second;
      }
      id2ptr_.erase(id);
      delete rmem;
    }
    return s;
  }

  RemoteMemTable* get(uint64_t id) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = id2ptr_.find(id);
    if (it == id2ptr_.end()) {
      return nullptr;
    }
    return it->second;
  }

  void erase(uint64_t id) {
    std::lock_guard<std::mutex> lock(mtx_);
    id2ptr_.erase(id);
  }

 private:
  std::mutex mtx_;
  std::unordered_map<uint64_t, RemoteMemTable*> id2ptr_;
};
}  // namespace ROCKSDB_NAMESPACE