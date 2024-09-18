
#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "memory/allocator.h"
namespace ROCKSDB_NAMESPACE {
template <int N>
class PrefixSep {
 public:
  PrefixSep() = default;
  ~PrefixSep() = default;
};

template <>
class PrefixSep<4> {
 private:
  const int sep_ = 4;

 public:
  PrefixSep() = default;
  ~PrefixSep() = default;
  const int get_sep(const char* prefix) const {
    // prefix is a 2-byte string from '00' to '99', sep them into 4 parts
    // int x = ((prefix[0] - '0') * 10 + (prefix[1] - '0'));
    // return x < 25 ? 0 : x < 50 ? 1 : x < 75 ? 2 : 3;
    // return int(prefix[0]) >> 6;
    // if (prefix[0] < '2') {
    //   return 0;
    // } else if (prefix[0] == '2') {
    //   return prefix[1] < '5' ? 0 : 1;
    // } else if (prefix[0] < '5') {
    //   return 1;
    // } else if (prefix[0] < '7') {
    //   return 2;
    // } else if (prefix[0] == '7') {
    //   return 2 + (prefix[1] < '5' ? 0 : 1);
    // } else {
    //   return 3;
    // }
    return uint8_t(prefix[0]) >> 6;
  }
};

}  // namespace ROCKSDB_NAMESPACE