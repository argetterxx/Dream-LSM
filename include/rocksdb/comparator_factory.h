#pragma once

#include <cassert>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class ComparatorFactory {
 public:
  static void* UnPackLocal(TransferService* node);

  ComparatorFactory(const ComparatorFactory&) = delete;
  void operator=(const ComparatorFactory&) = delete;

 private:
  ComparatorFactory() = default;
  ~ComparatorFactory() = default;
};

inline void* ComparatorFactory::UnPackLocal(TransferService* node) {
  uint8_t msg = 0;
  node->receive(&msg, sizeof(msg));
  if (msg == 0 /*BytewiseComparator*/) {
    LOG("ComparatorFactory::UnPackLocal: BytewiseComparator");
    const Comparator* local_ptr = BytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else if (msg == 1 /*ReverseBytewiseComparatorImpl*/) {
    LOG("ComparatorFactory::UnPackLocal: ReverseBytewiseComparatorImpl");
    const Comparator* local_ptr = ReverseBytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else {
    LOG("ComparatorFactory::UnPackLocal: error: ", type);
    assert(false);
    return nullptr;
  }

  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
