#pragma once
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include "db/db_impl/db_impl.h"

namespace Concurrent {

template <typename Key, typename Value>
class concurrent_hash {
 public:
  concurrent_hash() = default;
  ~concurrent_hash() = default;
  inline std::optional<Value> find(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return std::nullopt;
    }
    return it->second;
  }
  inline bool insert(const Key& key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      return false;
    }
    map_.insert(std::make_pair(key, value));
    return true;
  }

 private:
  std::unordered_map<Key, Value> map_;
  std::shared_mutex mutex_;
};

template <typename Key>
class concurrent_unordered_set {
 public:
  concurrent_unordered_set() = default;
  ~concurrent_unordered_set() = default;
  inline bool find(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return false;
    }
    return true;
  }
  inline bool insert(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      return false;
    }
    map_.insert(key);
    return true;
  }

 private:
  std::unordered_set<Key> map_;
  std::shared_mutex mutex_;
};
}  // namespace Concurrent