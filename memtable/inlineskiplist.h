//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.
//
// InlineSkipList is derived from SkipList (skiplist.h), but it optimizes
// the memory layout by requiring that the key storage be allocated through
// the skip list instance.  For the common case of SkipList<const char*,
// Cmp> this saves 1 pointer per skip list node and gives better cache
// locality, at the expense of wasted padding from using AllocateAligned
// instead of Allocate for the keys.  The unused padding will be from
// 0 to sizeof(void*)-1 bytes, and the space savings are sizeof(void*)
// bytes, so despite the padding the space used is always less than
// SkipList<const char*, ..>.
//
// Thread safety -------------
//
// Writes via Insert require external synchronization, most likely a mutex.
// InsertConcurrently can be safely called concurrently with reads and
// with other concurrent inserts.  Reads require a guarantee that the
// InlineSkipList will not be destroyed while the read is in progress.
// Apart from that, reads progress without any internal locking or
// synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the InlineSkipList is
// destroyed.  This is trivially guaranteed by the code since we never
// delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the InlineSkipList.
// Only Insert() modifies the list, and it is careful to initialize a
// node and use release-stores to publish the nodes in one or more lists.
//
// ... prev vs. next pointer ordering ...
//

#pragma once
#include <assert.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "memory/allocator.h"
#include "memory/sep_concurrent_arena.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/macro.hpp"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

template <class Comparator>
class InlineSkipList {
 public:
  inline void TESTContinuous() const {
    if (offset == 0) {
      LOG_CERR("TESTContinue:: seems to be local version.");
    }
    LOG_CERR("Allocator name: ", allocator_->name(), ' ', offset, ' ',
             int64_t(local_mem_begin_), ' ', int64_t(remote_mem_begin_));
    for (int i = 0; i < 4; i++) {
      LOG_CERR("shard:", i, ' ', int64_t(local_shard_[i]), ' ',
               int64_t(remote_shard_[i]), ' ', shard_offset_[i]);
    }
    Node* node = head_->Next(0, offset);
    LOG_CERR("Head:: ", int64_t(head_), ' ', int64_t(node));
    std::function<std::string(const char*, const char*)> parse =
        [&](const char* raw_key, const char* meta_node) {
          // parse the key:[key_size(int64_t)|key|value_size(int64_t)|value]
          char* buf_ = const_cast<char*>(raw_key);
          char* node_fake_begin_ = const_cast<char*>(meta_node);
          char* node_end_ = node_fake_begin_ + sizeof(Node);
          uint32_t ikey_size = 0;
          auto p = GetVarint32Ptr(buf_, buf_ + 5, &ikey_size);
          if (p == nullptr) {
            LOG_CERR("ikey parse error");
          }
          int sep =
              SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(p);
          std::string ikey = Slice(p, ikey_size - 8).ToString(true);
          buf_ += VarintLength(ikey_size) + ikey_size;
          uint32_t ivalue_size = 0;
          p = GetVarint32Ptr(buf_, buf_ + 5, &ivalue_size);
          if (p == nullptr) {
            LOG_CERR("ivalue parse error");
          }
          std::string ivalue = Slice(p, ivalue_size).ToString();
          int64_t blocksize_ = Arena::OptimizeBlockSize((64 << 20) + 10240);
          assert(node_end_ <= local_mem_begin_ + offset + blocksize_ &&
                 node_fake_begin_ >= local_mem_begin_ + offset);
          assert(buf_ <= local_shard_[sep] + shard_offset_[sep] + blocksize_ &&
                 buf_ >= local_shard_[sep] + shard_offset_[sep]);
          return std::string(std::to_string(ikey_size) + "+" +
                             std::to_string(ivalue_size) + "+" + "--" +
                             std::to_string(sep));
        };
    LOG_CERR("Test Iteration");
    int cnt = 0;
    while (node != nullptr) {
      LOG_CERR(parse(shard_offset_[0] ? node->RKey(shard_offset_) : node->Key(),
                     const_cast<const char*>(reinterpret_cast<char*>(node))));
      cnt++;
      node = node->Next(0, offset);
    }
    LOG_CERR("InlineSkipList TESTContinuous Iteation finish:: ", cnt);
    // for (int i = 0; i < 4; i++) {
    //   LOG_CERR("Test SepIterator ", i);
    //   SepIterator sep_iter(this, i);
    //   sep_iter.SeekToFirst();
    //   cnt = 0;
    //   while (sep_iter.Valid()) {
    //     LOG_CERR(parse(sep_iter.key(),
    //                    const_cast<const
    //                    char*>(reinterpret_cast<char*>(node))));
    //     sep_iter.Next();
    //     cnt++;
    //   }
    //   LOG_CERR("InlineSkipList TESTContinuous SepIterator ", i,
    //            " finish:: ", cnt);
    // }
  }

 public:
  inline void set_local_begin(void* local) {
    local_mem_begin_ = const_cast<const char*>(reinterpret_cast<char*>(local));
  }
  inline std::pair<const char*, size_t> get_local_begin() const {
    return std::make_pair(
        local_mem_begin_,
        reinterpret_cast<SepConcurrentArena*>(allocator_)->RawBlockSize());
  }
  inline std::pair<void*, size_t> get_remote_begin() const {
    return std::make_pair(
        remote_mem_begin_,
        reinterpret_cast<SepConcurrentArena*>(allocator_)->RawBlockSize());
  }
  inline void set_remote_begin(void* remote) {
    assert(remote != nullptr);
    remote_mem_begin_ = reinterpret_cast<char*>(remote);
    offset = reinterpret_cast<char*>(remote) - local_mem_begin_;
  }
  inline void set_shard_remote_begin(int sep, void* remote) {
    assert(remote != nullptr);
    remote_shard_[sep] = reinterpret_cast<char*>(remote);
    shard_offset_[sep] =
        reinterpret_cast<const char*>(remote) - local_shard_[sep];
  }
  inline void set_shard_remote_begin_offset(int sep, int64_t remote_offset) {
    assert(local_shard_[sep] != nullptr);
    remote_shard_[sep] = const_cast<char*>(local_shard_[sep]);
    shard_offset_[sep] = remote_offset;
        
  }
  inline void set_local_buf_begin(char* local) {
    local_buf_begin_ = const_cast<const char*>(local);
  }
  inline void set_enable_one_side_read() {
    enable_multi_one_side_reads_ = true;
    LOG_CERR("set enable one side reads");
  }
  inline void set_rdma_conn(RDMANode::rdma_connection* conn) {
    conn_ = conn;
    LOG_CERR("set conns");
  }
  inline void set_rdma_client(RDMAClient* client){
    client_ = client;
    LOG_CERR("set clients");
  }
  inline void set_real_offset(int64_t offset) {
    offset_ = offset;
  }
  inline int64_t get_real_offset() {
    return offset_;
  }

  inline void set_shard_local_begin(int sep, void* local) {
    local_shard_[sep] = const_cast<const char*>(reinterpret_cast<char*>(local));
  }
  inline void* get_shard_local_begin(int sep) {
    return const_cast<void*>(reinterpret_cast<const void*>(local_shard_[sep]));
  }
  inline void* get_shard_remote_begin(int sep) {
    return const_cast<void*>(reinterpret_cast<const void*>(remote_shard_[sep]));
  }

  // update remote metadata
  inline int64_t get_head_offset() const {
    return int64_t(reinterpret_cast<char*>(head_) -
                   const_cast<char*>(local_mem_begin_) - offset);
  }
  inline void set_head_offset(int64_t offset) {
    assert(remote_mem_begin_ != nullptr);
    Node** head = const_cast<Node**>(&head_);
    *head = reinterpret_cast<Node*>(reinterpret_cast<char*>(remote_mem_begin_) +
                                    offset);
  }
  inline void set_max_height(int height) {
    max_height_.store(height, std::memory_order_acquire);
  }
  inline void get_max_height(int& height) const {
    height = max_height_.load(std::memory_order_acquire);
  }
  inline Status SendToRemote() const {
    return reinterpret_cast<BasicArena*>(allocator_)->SendToRemote();
  }
  inline void get_remote_page_info(uint64_t* info) const {
    return reinterpret_cast<BasicArena*>(allocator_)
        ->get_remote_page_info(info);
  }

 private:
  struct Node;
  struct Splice;

 public:
  using DecodedKey =
      typename std::remove_reference<Comparator>::type::DecodedType;

  static const uint16_t kMaxPossibleHeight = 32;

  // Create a new InlineSkipList object that will use "cmp" for comparing
  // keys, and will allocate memory using "*allocator".  Objects allocated
  // in the allocator must remain allocated for the lifetime of the
  // skiplist object.
  explicit InlineSkipList(Comparator cmp, Allocator* allocator,
                          int32_t max_height = 12,
                          int32_t branching_factor = 4);
  // No copying allowed
  InlineSkipList(const InlineSkipList&) = delete;
  InlineSkipList& operator=(const InlineSkipList&) = delete;

  // Allocates a key and a skip-list node, returning a pointer to the key
  // portion of the node.  This method is thread-safe if the allocator
  // is thread-safe.
  void AllocateKey(size_t key_size, char** ptr_buf, char** kv_buf,
                   const char* prefix);

  // Allocate a splice using allocator.
  Splice* AllocateSplice();

  // Allocate a splice on heap.
  Splice* AllocateSpliceOnHeap();

  // Inserts a key allocated by AllocateKey, after the actual key value
  // has been filled in.
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls to any of inserts.
  bool Insert(const char* key);

  // Inserts a key allocated by AllocateKey with a hint of last insert
  // position in the skip-list. If hint points to nullptr, a new hint will be
  // populated, which can be used in subsequent calls.
  //
  // It can be used to optimize the workload where there are multiple groups
  // of keys, and each key is likely to insert to a location close to the last
  // inserted key in the same group. One example is sequential inserts.
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls to any of inserts.
  bool InsertWithHint(const char* key, void** hint);

  // Like InsertConcurrently, but with a hint
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls that use same hint
  bool InsertWithHintConcurrently(const char* key, void** hint);

  // Like Insert, but external synchronization is not required.
  bool InsertConcurrently(const char* key);

  // Inserts a node into the skip list.  key must have been allocated by
  // AllocateKey and then filled in by the caller.  If UseCAS is true,
  // then external synchronization is not required, otherwise this method
  // may not be called concurrently with any other insertions.
  //
  // Regardless of whether UseCAS is true, the splice must be owned
  // exclusively by the current thread.  If allow_partial_splice_fix is
  // true, then the cost of insertion is amortized O(log D), where D is
  // the distance from the splice to the inserted key (measured as the
  // number of intervening nodes).  Note that this bound is very good for
  // sequential insertions!  If allow_partial_splice_fix is false then
  // the existing splice will be ignored unless the current key is being
  // inserted immediately after the splice.  allow_partial_splice_fix ==
  // false has worse running time for the non-sequential case O(log N),
  // but a better constant factor.
  template <bool UseCAS>
  bool Insert(const char* key, Splice* splice, bool allow_partial_splice_fix);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const;

  // Return estimated number of entries smaller than `key`.
  uint64_t EstimateCount(const char* key) const;

  // Validate correctness of the skip-list.
  void TEST_Validate() const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const InlineSkipList* list);

    // Change the underlying skiplist used for this iterator
    // This enables us not changing the iterator without deallocating
    // an old one and then allocating a new one
    void SetList(const InlineSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const;

    // unsigned char Freq();

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const char* target);

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const char* target);

    // Advance to a random entry in the list.
    void RandomSeek();

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const InlineSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

  class SepIterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit SepIterator(const InlineSkipList* list, int sep);

    // Change the underlying skiplist used for this iterator
    // This enables us not changing the iterator without deallocating
    // an old one and then allocating a new one
    void SetList(const InlineSkipList* list, int sep);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const InlineSkipList* list_;
    const Node* front_;
    const Node* last_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  const uint16_t kMaxHeight_;
  const uint16_t kBranching_;
  const uint32_t kScaledInverseBranching_;

  Allocator* const allocator_;  // Allocator used for allocations of nodes
  // Immutable after construction
  Comparator const compare_;
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // seq_splice_ is a Splice used for insertions in the non-concurrent
  // case.  It caches the prev and next found during the most recent
  // non-concurrent insertion.
  Splice* seq_splice_;
  // remote support
  const char* local_mem_begin_;
  char* remote_mem_begin_{nullptr};
  int64_t offset{0};
  // std::vector<std::<const char*, char*, int32_t>> remote_kv_;
  const char* local_shard_[4]{nullptr};
  char* remote_shard_[4]{nullptr};
  int64_t shard_offset_[4]{0};
  const char* local_buf_begin_;
  bool enable_multi_one_side_reads_ = false;
  RDMANode::rdma_connection* conn_{nullptr};
  RDMAClient* client_ = nullptr;
  int64_t offset_{0};

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  int RandomHeight();

  Node* AllocateNode(size_t key_size, int height, const char* prefix);

  bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  bool LessThan(const char* a, const char* b) const {
    return (compare_(a, b) < 0);
  }

  // Return true if key is greater than the data stored in "n".  Null n
  // is considered infinite.  n should not be head_.
  bool KeyIsAfterNode(const char* key, Node* n) const;
  bool KeyIsAfterNode(const DecodedKey& key, Node* n) const;

  // Returns the earliest node with a key >= key.
  // Return nullptr if there is no such node.
  Node* FindGreaterOrEqual(const char* key) const;
  Node* FindSepGreaterOrEqual(int sep) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // Fills prev[level] with pointer to previous node at "level" for every
  // level in [0..max_height_-1], if prev is non-null.
  Node* FindLessThan(const char* key, Node** prev = nullptr) const;
  Node* FindSepLessThan(int sep) const;

  // Return the latest node with a key < key on bottom_level. Start searching
  // from root node on the level below top_level.
  // Fills prev[level] with pointer to previous node at "level" for every
  // level in [bottom_level..top_level-1], if prev is non-null.
  Node* FindLessThan(const char* key, Node** prev, Node* root, int top_level,
                     int bottom_level) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Returns a random entry.
  Node* FindRandomEntry() const;

  // Traverses a single level of the list, setting *out_prev to the last
  // node before the key and *out_next to the first node after. Assumes
  // that the key is not present in the skip list. On entry, before should
  // point to a node that is before the key, and after should point to
  // a node that is after the key.  after should be nullptr if a good after
  // node isn't conveniently available.
  template <bool prefetch_before>
  void FindSpliceForLevel(const DecodedKey& key, Node* before, Node* after,
                          int level, Node** out_prev, Node** out_next);

  // Recomputes Splice levels from highest_level (inclusive) down to
  // lowest_level (inclusive).
  void RecomputeSpliceLevels(const DecodedKey& key, Splice* splice,
                             int recompute_level);
};

// Implementation details follow

template <class Comparator>
struct InlineSkipList<Comparator>::Splice {
  // The invariant of a Splice is that prev_[i+1].key <= prev_[i].key <
  // next_[i].key <= next_[i+1].key for all i.  That means that if a
  // key is bracketed by prev_[i] and next_[i] then it is bracketed by
  // all higher levels.  It is _not_ required that prev_[i]->Next(i) ==
  // next_[i] (it probably did at some point in the past, but intervening
  // or concurrent operations might have inserted nodes in between).
  int height_ = 0;
  Node** prev_;
  Node** next_;
};

// The Node data type is more of a pointer into custom-managed memory than
// a traditional C++ struct.  The key is stored in the bytes immediately
// after the struct, and the next_ pointers for nodes with height > 1 are
// stored immediately _before_ the struct.  This avoids the need to include
// any pointer or sizing data, which reduces per-node memory overheads.
template <class Comparator>
struct InlineSkipList<Comparator>::Node {
  // Stores the height of the node in the memory location normally used for
  // next_[0].  This is used for passing data from AllocateKey to Insert.
  void StashHeight(const int height) {
    assert(sizeof(int) <= sizeof(next_[0]));
    memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
  }

  // Retrieves the value passed to StashHeight.  Undefined after a call
  // to SetNext or NoBarrier_SetNext.
  int UnstashHeight() const {
    int rv;
    memcpy(&rv, &next_[0], sizeof(int));
    return rv;
  }

  const char* Key() const {
    return *reinterpret_cast<char**>(
        const_cast<char*>(reinterpret_cast<const char*>(&next_[1])));
  }
  const char* RKey(const int64_t* remote_offset) const {

      return (*reinterpret_cast<char**>(
               const_cast<char*>(reinterpret_cast<const char*>(&next_[1])))) +
           remote_offset[*reinterpret_cast<const uint8_t*>(
               (reinterpret_cast<const char*>(&next_[1])) + sizeof(char*))];

  }

  const char* RKey(const int64_t* remote_offset, RDMAClient* rdma_client, RDMANode::rdma_connection* conn, const char* local_buf_begin) const {
    // LOG_CERR("start jump");
      int64_t local_position = *reinterpret_cast<char**>(const_cast<char*>(reinterpret_cast<const char*>(&next_[1]))) - rdma_client->get_buf();
      // LOG_CERR("local position: ", local_position);
      int64_t remote_position = remote_offset[*reinterpret_cast<const uint8_t*>((reinterpret_cast<const char*>(&next_[1])) + sizeof(char*))];
      // LOG_CERR("remote position: ", remote_position);
      int64_t offset = local_position + remote_position;
      // LOG_CERR("final position: ", offset);
      // int64_t offset = (*reinterpret_cast<char**>(
      //          const_cast<char*>(reinterpret_cast<const char*>(&next_[1])))) - rdma_client->get_buf() + remote_offset[*reinterpret_cast<const uint8_t*>(
      //          (reinterpret_cast<const char*>(&next_[1])) + sizeof(char*))];
      // LOG_CERR("real key offset: ", offset);

      int64_t local_key_offset = rdma_client->rdma_mem_.allocate(16);
      char* key_offset = rdma_client->get_buf() + local_key_offset;
      rdma_client->rdma_read(conn, 16, local_key_offset, offset);

      ASSERT_RW(rdma_client->poll_completion(conn) == 0);
      // printf("Read string: %s\n", key_offset);
      return key_offset;
  }

  // Accessors/mutators for links.  Wrapped in methods so we can add
  // the appropriate barriers as necessary, and perform the necessary
  // addressing trickery for storing links below the Node in memory.
  Node* Next(int n, int64_t offset) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    Node* tmp;
    return ((offset == 0)
                ? ((&next_[0] - n)->load(std::memory_order_acquire))
                : (((tmp = reinterpret_cast<Node*>(reinterpret_cast<char*>(
                         (&next_[0] - n)->load(std::memory_order_acquire)))) ==
                            nullptr
                        ? nullptr
                        : reinterpret_cast<Node*>(reinterpret_cast<char*>(tmp) +
                                                  offset))));
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    (&next_[0] - n)->store(x, std::memory_order_release);
  }

  bool CASNext(int n, Node* expected, Node* x) {
    assert(n >= 0);
    return (&next_[0] - n)->compare_exchange_strong(expected, x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return (&next_[0] - n)->load(std::memory_order_relaxed);
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    (&next_[0] - n)->store(x, std::memory_order_relaxed);
  }

  // Insert node after prev on specific level.
  void InsertAfter(Node* prev, int level) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "this" in prev.
    NoBarrier_SetNext(level, prev->NoBarrier_Next(level));
    prev->SetNext(level, this);
  }

  // unsigned char frequency{0};

 private:
  // next_[0] is the lowest level link (level 0).  Higher levels are
  // stored _earlier_, so level 1 is at next_[-1].
  std::atomic<Node*> next_[1];
};

template <class Comparator>
inline InlineSkipList<Comparator>::Iterator::Iterator(
    const InlineSkipList* list) {
  SetList(list);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SetList(
    const InlineSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <class Comparator>
inline bool InlineSkipList<Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <class Comparator>
inline const char* InlineSkipList<Comparator>::Iterator::key() const {
  assert(Valid());
  // return list_->remote_shard_[0] == nullptr ? node_->Key()
  //                                           : node_->RKey(list_->shard_offset_);
  return list_->remote_shard_[0] == nullptr ? node_->Key()
                                          : (!list_->enable_multi_one_side_reads_
                                              ? node_->RKey(list_->shard_offset_) 
                                              : node_->RKey(list_->shard_offset_, list_->client_, list_->conn_, list_->local_buf_begin_));
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0, list_->offset);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->Key());
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Seek(const char* target) {
  node_ = list_->FindGreaterOrEqual(target);
  // node_->frequency ++;
}

// template <class Comparator>
// inline unsigned char InlineSkipList<Comparator>::Iterator::Freq() {
//   return node_->frequency;
// }

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekForPrev(
    const char* target) {
  Seek(target);
  if (!Valid()) {
    SeekToLast();
  }
  while (Valid() && list_->LessThan(target, key())) {
    Prev();
  }
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::RandomSeek() {
  node_ = list_->FindRandomEntry();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0, list_->offset);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline InlineSkipList<Comparator>::SepIterator::SepIterator(
    const InlineSkipList* list, int sep) {
  SetList(list, sep);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::SepIterator::SetList(
    const InlineSkipList* list, int sep) {
  list_ = list;
  node_ = nullptr;
  // Todo: check corner case
  front_ = list_->FindSepGreaterOrEqual(sep);
  last_ = list_->FindSepLessThan(sep + 1);
  if (front_ != list_->head_ && front_ != nullptr) {
    const DecodedKey pkey =
        list_->compare_.decode_key(front_->RKey(list_->shard_offset_));
    LOG_CERR("FindLess::Front:: ", sep, ' ', pkey.ToString(true), ' ',
             SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
                 pkey.data()));
  }
  if (last_ != list_->head_ && last_ != nullptr) {
    const DecodedKey pkey =
        list_->compare_.decode_key(last_->RKey(list_->shard_offset_));
    LOG_CERR("FindLess::Last:: ", sep, ' ', pkey.ToString(true), ' ',
             SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
                 pkey.data()));
  }
}

template <class Comparator>
inline bool InlineSkipList<Comparator>::SepIterator::Valid() const {
  return node_ != nullptr && node_ != last_;
}
template <class Comparator>
inline const char* InlineSkipList<Comparator>::SepIterator::key() const {
  assert(Valid());
  // return list_->shard_offset_[0] ? node_->RKey(list_->shard_offset_)
  //                                : node_->Key();
  return list_->remote_shard_[0] == nullptr ? node_->Key()
                                        : (!list_->enable_multi_one_side_reads_
                                            ? node_->RKey(list_->shard_offset_) 
                                            : node_->RKey(list_->shard_offset_, list_->client_, list_->conn_, list_->local_buf_begin_));
}

template <class Comparator>
inline void InlineSkipList<Comparator>::SepIterator::Next() {
  assert(Valid());
  if (node_ == last_)
    node_ = nullptr;
  else
    node_ = node_->Next(0, list_->offset);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::SepIterator::SeekToFirst() {
  node_ = const_cast<Node*>(front_);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::SepIterator::SeekToLast() {
  node_ = const_cast<Node*>(last_);
}

template <class Comparator>
int InlineSkipList<Comparator>::RandomHeight() {
  auto rnd = Random::GetTLSInstance();

  // Increase height with probability 1 in kBranching
  int height = 1;
  while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
         rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight_);
  assert(height <= kMaxPossibleHeight);
  return height;
}

template <class Comparator>
bool InlineSkipList<Comparator>::KeyIsAfterNode(const char* key,
                                                Node* n) const {
  // nullptr n is considered infinite
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
bool InlineSkipList<Comparator>::KeyIsAfterNode(const DecodedKey& key,
                                                Node* n) const {
  // nullptr n is considered infinite
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindGreaterOrEqual(const char* key) const {
  // Note: It looks like we could reduce duplication by implementing
  // this function as FindLessThan(key)->Next(0), but we wouldn't be able
  // to exit early on equality and the result wouldn't even be correct.
  // A concurrent insert might occur after FindLessThan(key) but before
  // we get a chance to call Next(0).
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  Node* last_bigger = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    Node* next = x->Next(level, offset);
    if (next != nullptr) {
      PREFETCH(next->Next(level, offset), 0, 1);
    }
    // Make sure the lists are sorted
    // assert(x == head_ || next == nullptr ||
    //        ((remote_shard_[0] == nullptr)
    //             ? KeyIsAfterNode(next->Key(), x)
    //             : KeyIsAfterNode(next->RKey(shard_offset_), x)));
    assert(x == head_ || next == nullptr ||
       ((remote_shard_[0] == nullptr)
            ? KeyIsAfterNode(next->Key(), x)
            : (!enable_multi_one_side_reads_
               ? KeyIsAfterNode(next->RKey(shard_offset_), x)
               : KeyIsAfterNode(next->RKey(shard_offset_, client_, conn_, local_buf_begin_), x))));
    // Make sure we haven't overshot during our search
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    int cmp = (next == nullptr || next == last_bigger)
                  ? 1
                  // : compare_((shard_offset_[0]) ? next->RKey(shard_offset_)
                  //                               : next->Key(),
                  //            key_decoded);
                      : compare_((shard_offset_[0] == 0)
                            ? next->Key()
                            : (!enable_multi_one_side_reads_
                               ? next->RKey(shard_offset_)
                               : next->RKey(shard_offset_, client_, conn_, local_buf_begin_)),
                         key_decoded);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      return next;
    } else if (cmp < 0) {
      // Keep searching in this list
      x = next;
    } else {
      // Switch to next list, reuse compare_() result
      last_bigger = next;
      level--;
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLessThan(const char* key, Node** prev) const {
  return FindLessThan(key, prev, head_, GetMaxHeight(), 0);
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLessThan(const char* key, Node** prev,
                                         Node* root, int top_level,
                                         int bottom_level) const {
  assert(top_level > bottom_level);
  int level = top_level - 1;
  Node* x = root;
  // KeyIsAfter(key, last_not_after) is definitely false
  Node* last_not_after = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    assert(x != nullptr);
    Node* next = x->Next(level, offset);
    if (next != nullptr) {
      PREFETCH(next->Next(level, offset), 0, 1);
    }
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    if (next != last_not_after && KeyIsAfterNode(key_decoded, next)) {
      // Keep searching in this list
      assert(next != nullptr);
      x = next;
    } else {
      if (prev != nullptr) {
        prev[level] = x;
      }
      if (level == bottom_level) {
        return x;
      } else {
        // Switch to next list, reuse KeyIsAfterNode() result
        last_not_after = next;
        level--;
      }
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindSepGreaterOrEqual(int sep) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level, offset);
    if (next == nullptr) {
      if (level == 0) {
        return nullptr;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      const char* ikey = compare_
                             .decode_key(
                              // remote_shard_[0] == nullptr
                              //                ? next->Key()
                              //                : next->RKey(shard_offset_)
                              remote_shard_[0] == nullptr
                                                ? next->Key()
                                                : (!enable_multi_one_side_reads_
                                                    ? next->RKey(shard_offset_)
                                                    : next->RKey(shard_offset_, client_, conn_, local_buf_begin_))
                                             )
                             .data();
      int next_sep =
          SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(ikey);
      if (next_sep >= sep && level == 0) {
        return next;
      } else if (next_sep < sep) {
        // Keep searching in this list
        x = next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindSepLessThan(int sep) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level, offset);
    if (next == nullptr) {
      if (level == 0) {
        // const DecodedKey ikey = compare_.decode_key(x->RKey(shard_offset_));
        // LOG_CERR("Return::FindSep:: ", sep, ' ', ikey.ToString(true), ' ',
        //          SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
        //              ikey.data()),
        //          ' ', x == head_ ? "true" : "false");
        return x == head_ ? nullptr : x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      const char* ikey = compare_
                             .decode_key(
                              // remote_shard_[0] == nullptr
                              //                ? next->Key()
                              //                : next->RKey(shard_offset_)
                              remote_shard_[0] == nullptr
                                                ? next->Key()
                                                : (!enable_multi_one_side_reads_
                                                    ? next->RKey(shard_offset_)
                                                    : next->RKey(shard_offset_, client_, conn_, local_buf_begin_))
                                             )
                             .data();
      int next_sep =
          SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(ikey);
      // const DecodedKey pkey = compare_.decode_key(next->RKey(shard_offset_));
      // LOG_CERR("FindLess:: ", sep, ' ', next_sep, ' ', level, ' ',
      //          pkey.ToString(true), ' ',
      //          SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
      //              pkey.data()));
      if (level == 0 && next_sep >= sep) {
        // const DecodedKey ikey = compare_.decode_key(x->RKey(shard_offset_));
        // LOG_CERR("Return::FindSep::1 ", sep, ' ', ikey.ToString(true), ' ',
        //          SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(
        //              ikey.data()));
        return x;
      } else if (next_sep < sep) {
        // Keep searching in this list
        x = next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level, offset);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindRandomEntry() const {
  // TODO(bjlemaire): consider adding PREFETCH calls.
  Node *x = head_, *scan_node = nullptr, *limit_node = nullptr;

  // We start at the max level.
  // FOr each level, we look at all the nodes at the level, and
  // we randomly pick one of them. Then decrement the level
  // and reiterate the process.
  // eg: assume GetMaxHeight()=5, and there are #100 elements (nodes).
  // level 4 nodes: lvl_nodes={#1, #15, #67, #84}. Randomly pick #15.
  // We will consider all the nodes between #15 (inclusive) and #67
  // (exclusive). #67 is called 'limit_node' here.
  // level 3 nodes: lvl_nodes={#15, #21, #45, #51}. Randomly choose
  // #51. #67 remains 'limit_node'.
  // [...]
  // level 0 nodes: lvl_nodes={#56,#57,#58,#59}. Randomly pick $57.
  // Return Node #57.
  std::vector<Node*> lvl_nodes;
  Random* rnd = Random::GetTLSInstance();
  int level = GetMaxHeight() - 1;

  while (level >= 0) {
    lvl_nodes.clear();
    scan_node = x;
    while (scan_node != limit_node) {
      lvl_nodes.push_back(scan_node);
      scan_node = scan_node->Next(level, offset);
    }
    uint32_t rnd_idx = rnd->Next() % lvl_nodes.size();
    x = lvl_nodes[rnd_idx];
    if (rnd_idx + 1 < lvl_nodes.size()) {
      limit_node = lvl_nodes[rnd_idx + 1];
    }
    level--;
  }
  // There is a special case where x could still be the head_
  // (note that the head_ contains no key).
  return x == head_ && head_ != nullptr ? head_->Next(0, offset) : x;
}

template <class Comparator>
uint64_t InlineSkipList<Comparator>::EstimateCount(const char* key) const {
  uint64_t count = 0;

  Node* x = head_;
  int level = GetMaxHeight() - 1;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    assert(x == head_ || compare_(x->Key(), key_decoded) < 0);
    Node* next = x->Next(level, offset);
    if (next != nullptr) {
      PREFETCH(next->Next(level, offset), 0, 1);
    }
    if (next == nullptr || compare_(next->Key(), key_decoded) >= 0) {
      if (level == 0) {
        return count;
      } else {
        // Switch to next list
        count *= kBranching_;
        level--;
      }
    } else {
      x = next;
      count++;
    }
  }
}

template <class Comparator>
InlineSkipList<Comparator>::InlineSkipList(const Comparator cmp,
                                           Allocator* allocator,
                                           int32_t max_height,
                                           int32_t branching_factor)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      kBranching_(static_cast<uint16_t>(branching_factor)),
      kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
      allocator_(allocator),
      compare_(cmp),
      head_(AllocateNode(0, max_height, nullptr)),
      max_height_(1),
      seq_splice_(AllocateSplice()),
      local_mem_begin_(nullptr) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);

  for (int i = 0; i < kMaxHeight_; ++i) {
    head_->SetNext(i, nullptr);
  }
  local_mem_begin_ = reinterpret_cast<const char*>(
      reinterpret_cast<SepConcurrentArena*>(allocator)->meta_begin());
  for (int i = 0; i < 4 /*sep*/; i++) {
    auto kv_begin_ =
        reinterpret_cast<SepConcurrentArena*>(allocator)->kv_begin(i);
    local_shard_[i] = reinterpret_cast<const char*>(kv_begin_);
    remote_shard_[i] = nullptr;
    shard_offset_[i] = 0;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::AllocateKey(size_t key_size, char** ptr_buf,
                                             char** kv_buf,
                                             const char* prefix) {
  Node* node_buf_ = AllocateNode(key_size, RandomHeight(), prefix);
  const char* kv_buf_ = node_buf_->Key();
  char* ptr_buf_ =
      reinterpret_cast<char*>(node_buf_) + sizeof(std::atomic<Node*>);
  *ptr_buf = ptr_buf_;
  *kv_buf = const_cast<char*>(kv_buf_);
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::AllocateNode(size_t key_size, int height,
                                         const char* prefix_) {
  auto prefix = sizeof(std::atomic<Node*>) * (height - 1);

  // prefix is space for the height - 1 pointers that we store before
  // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
  // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
  // next_[0].  key_size is the bytes for the key, which comes just after
  // the Node.
  // Todo(separated-skiplist): maybe need to refine allocator->Allocate()
  // interface ; replace char* with Offset
  char* raw = allocator_->AllocateAligned(prefix + sizeof(std::atomic<Node*>) +
                                          sizeof(uint8_t) + sizeof(char**));
  char* raw_kv = key_size ? reinterpret_cast<SepConcurrentArena*>(allocator_)
                                ->AllocateKV(key_size, prefix_)
                          : nullptr;
  Node* x = reinterpret_cast<Node*>(raw + prefix);

  // Once we've linked the node into the skip list we don't actually need
  // to know its height, because we can implicitly use the fact that we
  // traversed into a node at level h to known that h is a valid level
  // for that node.  We need to convey the height to the Insert step,
  // however, so that it can perform the proper links.  Since we're not
  // using the pointers at the moment, StashHeight temporarily borrow
  // storage from next_[0] for that purpose.
  x->StashHeight(height);
  *reinterpret_cast<char**>(raw + prefix +
                            sizeof(std::atomic<Node*>) /*Node*/) = raw_kv;
  // Todo: check reinterpret
  *reinterpret_cast<uint8_t*>(
      raw + prefix + sizeof(std::atomic<Node*>) /*Node*/ + sizeof(char*)) =
      key_size
          ? SingletonV2::SingletonV2<PrefixSep<4>>::Instance().get_sep(prefix_)
          : 4;
  return x;
}

template <class Comparator>
typename InlineSkipList<Comparator>::Splice*
InlineSkipList<Comparator>::AllocateSplice() {
  // size of prev_ and next_
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = allocator_->AllocateAligned(sizeof(Splice) + array_size * 2);
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
typename InlineSkipList<Comparator>::Splice*
InlineSkipList<Comparator>::AllocateSpliceOnHeap() {
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = new char[sizeof(Splice) + array_size * 2];
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
bool InlineSkipList<Comparator>::Insert(const char* key) {
  return Insert<false>(key, seq_splice_, false);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertConcurrently(const char* key) {
  Node* prev[kMaxPossibleHeight];
  Node* next[kMaxPossibleHeight];
  Splice splice;
  splice.prev_ = prev;
  splice.next_ = next;
  return Insert<true>(key, &splice, false);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertWithHint(const char* key, void** hint) {
  assert(hint != nullptr);
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSplice();
    *hint = reinterpret_cast<void*>(splice);
  }
  return Insert<false>(key, splice, true);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertWithHintConcurrently(const char* key,
                                                            void** hint) {
  assert(hint != nullptr);
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSpliceOnHeap();
    *hint = reinterpret_cast<void*>(splice);
  }
  return Insert<true>(key, splice, true);
}

template <class Comparator>
template <bool prefetch_before>
void InlineSkipList<Comparator>::FindSpliceForLevel(const DecodedKey& key,
                                                    Node* before, Node* after,
                                                    int level, Node** out_prev,
                                                    Node** out_next) {
  while (true) {
    Node* next = before->Next(level, offset);
    if (next != nullptr) {
      PREFETCH(next->Next(level, offset), 0, 1);
    }
    if (prefetch_before == true) {
      if (next != nullptr && level > 0) {
        PREFETCH(next->Next(level - 1, offset), 0, 1);
      }
    }
    assert(before == head_ || next == nullptr ||
           KeyIsAfterNode(next->Key(), before));
    assert(before == head_ || KeyIsAfterNode(key, before));
    if (next == after || !KeyIsAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::RecomputeSpliceLevels(const DecodedKey& key,
                                                       Splice* splice,
                                                       int recompute_level) {
  assert(recompute_level > 0);
  assert(recompute_level <= splice->height_);
  for (int i = recompute_level - 1; i >= 0; --i) {
    FindSpliceForLevel<true>(key, splice->prev_[i + 1], splice->next_[i + 1], i,
                             &splice->prev_[i], &splice->next_[i]);
  }
}

template <class Comparator>
template <bool UseCAS>
bool InlineSkipList<Comparator>::Insert(const char* ptr, Splice* splice,
                                        bool allow_partial_splice_fix) {
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(ptr)) - 1;
  const char* key_ = x->Key();
  const DecodedKey key_decoded = compare_.decode_key(key_);
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  int max_height = max_height_.load(std::memory_order_relaxed);
  while (height > max_height) {
    if (max_height_.compare_exchange_weak(max_height, height)) {
      // successfully updated it
      max_height = height;
      break;
    }
    // else retry, possibly exiting the loop because somebody else
    // increased it
  }
  assert(max_height <= kMaxPossibleHeight);

  int recompute_height = 0;
  if (splice->height_ < max_height) {
    // Either splice has never been used or max_height has grown since
    // last use.  We could potentially fix it in the latter case, but
    // that is tricky.
    splice->prev_[max_height] = head_;
    splice->next_[max_height] = nullptr;
    splice->height_ = max_height;
    recompute_height = max_height;
  } else {
    // Splice is a valid proper-height splice that brackets some
    // key, but does it bracket this one?  We need to validate it and
    // recompute a portion of the splice (levels 0..recompute_height-1)
    // that is a superset of all levels that don't bracket the new key.
    // Several choices are reasonable, because we have to balance the work
    // saved against the extra comparisons required to validate the Splice.
    //
    // One strategy is just to recompute all of orig_splice_height if the
    // bottom level isn't bracketing.  This pessimistically assumes that
    // we will either get a perfect Splice hit (increasing sequential
    // inserts) or have no locality.
    //
    // Another strategy is to walk up the Splice's levels until we find
    // a level that brackets the key.  This strategy lets the Splice
    // hint help for other cases: it turns insertion from O(log N) into
    // O(log D), where D is the number of nodes in between the key that
    // produced the Splice and the current insert (insertion is aided
    // whether the new key is before or after the splice).  If you have
    // a way of using a prefix of the key to map directly to the closest
    // Splice out of O(sqrt(N)) Splices and we make it so that splices
    // can also be used as hints during read, then we end up with Oshman's
    // and Shavit's SkipTrie, which has O(log log N) lookup and insertion
    // (compare to O(log N) for skip list).
    //
    // We control the pessimistic strategy with allow_partial_splice_fix.
    // A good strategy is probably to be pessimistic for seq_splice_,
    // optimistic if the caller actually went to the work of providing
    // a Splice.
    while (recompute_height < max_height) {
      if (splice->prev_[recompute_height]->Next(recompute_height, offset) !=
          splice->next_[recompute_height]) {
        // splice isn't tight at this level, there must have been some inserts
        // to this
        // location that didn't update the splice.  We might only be a little
        // stale, but if
        // the splice is very stale it would be O(N) to fix it.  We haven't used
        // up any of
        // our budget of comparisons, so always move up even if we are
        // pessimistic about
        // our chances of success.
        ++recompute_height;
      } else if (splice->prev_[recompute_height] != head_ &&
                 !KeyIsAfterNode(key_decoded,
                                 splice->prev_[recompute_height])) {
        // key is from before splice
        if (allow_partial_splice_fix) {
          // skip all levels with the same node without more comparisons
          Node* bad = splice->prev_[recompute_height];
          while (splice->prev_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          // we're pessimistic, recompute everything
          recompute_height = max_height;
        }
      } else if (KeyIsAfterNode(key_decoded, splice->next_[recompute_height])) {
        // key is from after splice
        if (allow_partial_splice_fix) {
          Node* bad = splice->next_[recompute_height];
          while (splice->next_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          recompute_height = max_height;
        }
      } else {
        // this level brackets the key, we won!
        break;
      }
    }
  }
  assert(recompute_height <= max_height);
  if (recompute_height > 0) {
    RecomputeSpliceLevels(key_decoded, splice, recompute_height);
  }

  bool splice_is_valid = true;
  if (UseCAS) {
    for (int i = 0; i < height; ++i) {
      while (true) {
        // Checking for duplicate keys on the level 0 is sufficient
        if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                     compare_(x->Key(), splice->next_[i]->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                     compare_(splice->prev_[i]->Key(), x->Key()) >= 0)) {
          // duplicate key
          return false;
        }
        assert(splice->next_[i] == nullptr ||
               compare_(x->Key(), splice->next_[i]->Key()) < 0);
        assert(splice->prev_[i] == head_ ||
               compare_(splice->prev_[i]->Key(), x->Key()) < 0);
        x->NoBarrier_SetNext(i, splice->next_[i]);
        if (splice->prev_[i]->CASNext(i, splice->next_[i], x)) {
          // success
          break;
        }
        // CAS failed, we need to recompute prev and next. It is unlikely
        // to be helpful to try to use a different level as we redo the
        // search, because it should be unlikely that lots of nodes have
        // been inserted between prev[i] and next[i]. No point in using
        // next[i] as the after hint, because we know it is stale.
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);

        // Since we've narrowed the bracket for level i, we might have
        // violated the Splice constraint between i and i-1.  Make sure
        // we recompute the whole thing next time.
        if (i > 0) {
          splice_is_valid = false;
        }
      }
    }
  } else {
    for (int i = 0; i < height; ++i) {
      if (i >= recompute_height &&
          splice->prev_[i]->Next(i, offset) != splice->next_[i]) {
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);
      }
      // Checking for duplicate keys on the level 0 is sufficient
      if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                   compare_(x->Key(), splice->next_[i]->Key()) >= 0)) {
        // duplicate key
        return false;
      }
      if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                   compare_(splice->prev_[i]->Key(), x->Key()) >= 0)) {
        // duplicate key
        return false;
      }
      assert(splice->next_[i] == nullptr ||
             compare_(x->Key(), splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), x->Key()) < 0);
      assert(splice->prev_[i]->Next(i, offset) == splice->next_[i]);
      x->NoBarrier_SetNext(i, splice->next_[i]);
      splice->prev_[i]->SetNext(i, x);
    }
  }
  if (splice_is_valid) {
    for (int i = 0; i < height; ++i) {
      splice->prev_[i] = x;
    }
    assert(splice->prev_[splice->height_] == head_);
    assert(splice->next_[splice->height_] == nullptr);
    for (int i = 0; i < splice->height_; ++i) {
      assert(splice->next_[i] == nullptr ||
             compare_(key_, splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), key_) <= 0);
      assert(splice->prev_[i + 1] == splice->prev_[i] ||
             splice->prev_[i + 1] == head_ ||
             compare_(splice->prev_[i + 1]->Key(), splice->prev_[i]->Key()) <
                 0);
      assert(splice->next_[i + 1] == splice->next_[i] ||
             splice->next_[i + 1] == nullptr ||
             compare_(splice->next_[i]->Key(), splice->next_[i + 1]->Key()) <
                 0);
    }
  } else {
    splice->height_ = 0;
  }
  return true;
}

template <class Comparator>
bool InlineSkipList<Comparator>::Contains(const char* key) const {
  Node* x = FindGreaterOrEqual(key);
  if (x != nullptr && Equal(key, x->Key())) {
    return true;
  } else {
    return false;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::TEST_Validate() const {
  // Interate over all levels at the same time, and verify nodes appear in
  // the right order, and nodes appear in upper level also appear in lower
  // levels.
  Node* nodes[kMaxPossibleHeight];
  int max_height = GetMaxHeight();
  assert(max_height > 0);
  for (int i = 0; i < max_height; i++) {
    nodes[i] = head_;
  }
  while (nodes[0] != nullptr) {
    Node* l0_next = nodes[0]->Next(0, offset);
    if (l0_next == nullptr) {
      break;
    }
    assert(nodes[0] == head_ || compare_(nodes[0]->Key(), l0_next->Key()) < 0);
    nodes[0] = l0_next;

    int i = 1;
    while (i < max_height) {
      Node* next = nodes[i]->Next(i, offset);
      if (next == nullptr) {
        break;
      }
      auto cmp = compare_(nodes[0]->Key(), next->Key());
      assert(cmp <= 0);
      if (cmp == 0) {
        assert(next == nodes[0]);
        nodes[i] = next;
      } else {
        break;
      }
      i++;
    }
  }
  for (int i = 1; i < max_height; i++) {
    assert(nodes[i] != nullptr && nodes[i]->Next(i, offset) == nullptr);
  }
}

}  // namespace ROCKSDB_NAMESPACE
