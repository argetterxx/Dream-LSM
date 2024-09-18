#pragma once
#include <cassert>
#include <cstdint>
#include <string>

#include "db/table_properties_collector.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"
#include "table/sst_file_writer_collectors.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

namespace ROCKSDB_NAMESPACE {

class TablePropertiesCollectorPackFactory {
 public:
  static void* UnPackLocal(TransferService* node);

 public:
  TablePropertiesCollectorPackFactory& operator=(
      const TablePropertiesCollectorPackFactory&) = delete;
  TablePropertiesCollectorPackFactory& operator=(
      TablePropertiesCollectorPackFactory&&) = delete;
  TablePropertiesCollectorPackFactory(
      const TablePropertiesCollectorPackFactory&) = delete;

 private:
  TablePropertiesCollectorPackFactory() = default;
  ~TablePropertiesCollectorPackFactory() = default;
};

class IntTblPropCollectorPackFactory {
 public:
  static void* UnPackLocal(TransferService* node);

 public:
  IntTblPropCollectorPackFactory& operator=(
      const IntTblPropCollectorPackFactory&) = delete;
  IntTblPropCollectorPackFactory& operator=(IntTblPropCollectorPackFactory&&) =
      delete;
  IntTblPropCollectorPackFactory(const IntTblPropCollectorPackFactory&) =
      delete;

 private:
  IntTblPropCollectorPackFactory() = default;
  ~IntTblPropCollectorPackFactory() = default;
};

inline void* TablePropertiesCollectorPackFactory::UnPackLocal(
    TransferService* node) {
  //   int64_t msg = 0;
  size_t msg_len = sizeof(size_t) + sizeof(size_t) * 2 + sizeof(double);
  char* msg = reinterpret_cast<char*>(malloc(msg_len));
  node->receive(msg, msg_len);
  size_t type = *reinterpret_cast<size_t*>(msg);
  if (type == 1) {
    LOG("TablePropertiesCollectorPackFactory::UnPackLocal: "
        "DbStressTablePropertiesCollectorFactory not compiled with "
        "ROCKSDB_TOOLS");
    assert(false);
  } else if (type == 2) {
    size_t sliding_window_size_ =
        *reinterpret_cast<size_t*>(msg + sizeof(size_t));
    size_t deletion_trigger_ =
        *reinterpret_cast<size_t*>(msg + sizeof(size_t) * 2);
    double deletion_ratio_ =
        *reinterpret_cast<double*>(msg + sizeof(size_t) * 2 + sizeof(size_t));
    return new CompactOnDeletionCollector(sliding_window_size_,
                                          deletion_trigger_, deletion_ratio_);
  } else {
    LOG("TablePropertiesCollectorPackFactory::UnPackLocal: unknown type: ",
        type);
    assert(false);
  }
  return nullptr;
}

inline void* IntTblPropCollectorPackFactory::UnPackLocal(
    TransferService* node) {
  int64_t msg = 0;
  node->receive(&msg, sizeof(msg));
  int64_t type = msg & 0xff;
  // int64_t info = msg >> 8;
  if (type == 1) {
    void* local_table_properties_collector_factory =
        TablePropertiesCollectorPackFactory::UnPackLocal(node);
    std::shared_ptr<TablePropertiesCollectorFactory> factory(
        reinterpret_cast<TablePropertiesCollectorFactory*>(
            local_table_properties_collector_factory));
    return new UserKeyTablePropertiesCollectorFactory(factory);
  } else if (type == 2) {
    size_t msg_len = sizeof(size_t) + sizeof(int32_t);
    char* sst_collector_msg = reinterpret_cast<char*>(malloc(msg_len));
    node->receive(sst_collector_msg, msg_len);
    int32_t version_ = *reinterpret_cast<int32_t*>(sst_collector_msg);
    size_t global_seqno_ =
        *reinterpret_cast<size_t*>(sst_collector_msg + sizeof(int32_t));
    return new SstFileWriterPropertiesCollectorFactory(version_, global_seqno_);
  } else {
    LOG("IntTblPropCollectorPackFactory::UnPackLocal: unknown type: ", type);
    assert(false);
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE