
#pragma once
#include "memory/sep_concurrent_arena.h"

#include <thread>

#include "db/tcprw.h"
#include "port/port.h"
#include "rocksdb/remote_flush_service.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
SepConcurrentArena::SepConcurrentArena(size_t max_memtable_size, int sep,
                                       RDMAClient *client,
                                       RDMANode::rdma_connection *conn)
    : sep_(sep), blocksize_(max_memtable_size) {
  int temp_compensate_size = 10240;  // exactly 2112 for now, others reserved
  if (client != nullptr && conn != nullptr) {
    char req_type = 5;
    ASSERT_RW(writen(conn->sock, reinterpret_cast<void *>(&req_type),
                     sizeof(char)) == sizeof(char));
  }
  size_t block_size =
      Arena::OptimizeBlockSize(max_memtable_size + temp_compensate_size);
  LOG_CERR("SepConcurrentArena::SepConcurrentArena:: ", max_memtable_size, ' ',
           Arena::OptimizeBlockSize(max_memtable_size + temp_compensate_size));
  meta_arena_ = new ConcurrentArena(block_size, nullptr, 0, client, conn);
  if (sep_ > 0) kv_arena_.resize(sep_);
  for (int i = 0; i < sep_; i++)
    kv_arena_[i] = new ConcurrentArena(block_size, nullptr, 0, client, conn);
}

Status SepConcurrentArena::SendToRemote() const {
  LOG_CERR("SepConcurrentArena::SendToRemote");
  Status s = meta_arena_->SendToRemote();
  if (!s.ok()) return s;
  for (int i = 0; i < sep_; i++) {
    s = kv_arena_[i]->SendToRemote();
    if (!s.ok()) return s;
  }
  LOG_CERR("SepConcurrentArena::SendToRemote Finish");
  return s;
}

void SepConcurrentArena::get_remote_page_info(uint64_t *info) const {
  meta_arena_->get_remote_page_info(info);
  for (int i = 0; i < sep_; i++) {
    kv_arena_[i]->get_remote_page_info(info + 2 + 2 * i);
  }
}

}  // namespace ROCKSDB_NAMESPACE
