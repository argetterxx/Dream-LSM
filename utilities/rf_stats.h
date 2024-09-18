#pragma once

#include <sys/types.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <tuple>
#include <vector>

#include "port/port_posix.h"
#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/system_clock.h"
namespace ROCKSDB_NAMESPACE {

template <class T>
class Singleton {
 public:
  static T* GetInstance() {
    static T instance;
    return &instance;
  }
};

class rf_stats {
 public:
  static rf_stats* GetInstance() { return Singleton<rf_stats>::GetInstance(); }

  void Start(uint64_t tp) {
    std::call_once(start_flag, [this, tp]() { start_time = tp; });
  }
  void End(uint64_t tp) {
    std::call_once(end_flag, [this, tp]() { end_time = tp; });
  }
  void Reset() { Start(Env::Default()->GetSystemClock().get()->NowMicros()); }

  void ReportCompaction(uint64_t start, uint64_t end, uint64_t elapsed_time) {
    compaction_mtx.Lock();
    compaction_stats.emplace_back(start, end, elapsed_time);
    compaction_mtx.Unlock();
  }
  void ReportFlush(uint64_t start, uint64_t end, uint64_t elapsed_time) {
    flush_mtx.Lock();
    flush_stats.emplace_back(start, end, elapsed_time);
    flush_mtx.Unlock();
  }
  void Report() {
    if (start_time == 0 || end_time == 0) {
      printf("rf_stats: start_time or end_time is 0\n");
      return;
    }
    std::ofstream file("/tmp/flush_compaction_stats.log",
                       std::ios::app | std::ios::out);
    if (file.is_open()) {
      file << "Flush Operations:" << std::endl;
      for (auto& [start, end, elapsed_time] : flush_stats) {
        file << "flush " << start << " " << end << " " << elapsed_time
             << std::endl;
      }
      file << "Compaction Operations:" << std::endl;
      for (auto& [start, end, elapsed_time] : compaction_stats) {
        file << "compaction " << start << " " << end << " " << elapsed_time
             << std::endl;
      }
      file.flush();
      file.close();
    } else {
      std::cerr << "dump flush&compaction stats data failed" << std::endl;
    }
  }

  rf_stats() = default;
  ~rf_stats() = default;

  std::once_flag start_flag, end_flag;
  uint64_t start_time = 0;
  uint64_t end_time = 0;
  port::Mutex flush_mtx;
  port::Mutex compaction_mtx;
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> compaction_stats;
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> flush_stats;

 private:
};

}  // namespace ROCKSDB_NAMESPACE