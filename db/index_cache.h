#pragma once
// #include <unordered_map>
// #include <string>
// #include <memory>
// #include <random>
// #include <algorithm>


// class EntryCache {
// public:
//     EntryCache(std::shared_ptr<std::string> key, int64_t offset, int64_t freq)
//         : key_(std::move(key)), offset_(offset), freq_(freq) {}

//     ~EntryCache() = default;

//     // Retrieve the key of the entry
//     std::shared_ptr<std::string> getKey() const {
//         return key_;
//     }

//     // Retrieve the offset stored in the entry
//     int64_t getOffset() const {
//         return offset_;
//     }

//     // Set the offset for the entry
//     void setOffset(int64_t offset) {
//         offset_ = offset;
//     }

//     // Retrieve the frequency of the entry
//     int64_t getFreq() const {
//         return freq_;
//     }

//     void increaseFreq() {
//         freq_++;
//     }

//     // Set the frequency for the entry
//     void setFreq(int64_t freq) {
//         freq_ = freq;
//     }

// private:
//     std::shared_ptr<std::string> key_;  // Pointer to the key string to save memory
//     int64_t offset_;
//     int64_t freq_;
// };

// class MemCache {
// public:
//     MemCache() = default;
//     ~MemCache() = default;

//     // Add an entry to the cache
//     void addEntry(std::string& key, std::shared_ptr<EntryCache> entry) {
//         cache_[key] = entry;
//     }

//     // Retrieve an entry from the cache
//     std::shared_ptr<EntryCache> getEntry(const std::string& key) {
//         auto it = cache_.find(key);
//         if (it != cache_.end()) {
//             return it->second;
//         }
//         return nullptr; // Return nullptr if key is not found
//     }

//     std::unordered_map<std::string, std::shared_ptr<EntryCache>> getCache() {
//         return cache_;
//     }

//     // Remove an entry from the cache
//     void removeEntry(std::string& key) {
//         cache_.erase(key);
//     }

//     // Check if the cache contains a specific key
//     bool containsKey(const std::string& key) const {
//         return cache_.find(key) != cache_.end();
//     }

//     // Get the number of entries in the cache
//     size_t size() const {
//         return cache_.size();
//     }

// private:
//     std::unordered_map<std::string, std::shared_ptr<EntryCache>> cache_;
// };

// class IndexCache {
// public:
//     // Constructor
//     IndexCache() : totalSize_(0) {}

//     // Destructor
//     ~IndexCache() = default;

//     // Add an entry to the cache
//     void addMem(const uint64_t id, std::shared_ptr<MemCache> mem) {
//         if (cache_.find(id) == cache_.end()) {
//             totalSize_ += mem->size();  // Update total size when a new MemCache is added
//         } else {
//             totalSize_ = totalSize_ - cache_[id]->size() + mem->size();  // Adjust total size if replacing an existing MemCache
//         }
//         cache_[id] = mem;
//     }

//     // Retrieve an entry from the cache
//     std::shared_ptr<MemCache> getMem(const uint64_t id) {
//         auto it = cache_.find(id);
//         if (it != cache_.end()) {
//             return it->second;
//         }
//         return nullptr; // Return nullptr if key is not found
//     }

//     // Remove an entry from the cache
//     void removeMem(const uint64_t id) {
//         auto it = cache_.find(id);
//         if (it != cache_.end()) {
//             totalSize_ -= it->second->size();  // Decrease total size when a MemCache is removed
//             cache_.erase(it);
//         }
//     }

//     // Check if the cache contains a specific key
//     bool containsMem(const uint64_t id) const {
//         return cache_.find(id) != cache_.end();
//     }

//     // Get the total size of all MemCache entries
//     size_t size() const {
//         return totalSize_;
//     }

//     // Evict entries based on frequency
//     void evict(size_t numbers) {
//         if (cache_.size() < 1) return; // Ensure there is at least 1 MemCache to evict from

//         std::random_device rd;
//         std::mt19937 gen(rd());
//         std::uniform_int_distribution<> mem_dis(0, cache_.size() - 1);

//         for (size_t i = 0; i < numbers; ++i) {
//             auto it1 = std::next(cache_.begin(), mem_dis(gen));
//             auto it2 = std::next(cache_.begin(), mem_dis(gen));

//             // Ensure the MemCache has at least one entry to evict
//             if (it1->second->size() > 0 && it2->second->size() > 0) {
//                 auto entry1 = getRandomEntry(it1->second, gen);
//                 auto entry2 = getRandomEntry(it2->second, gen);

//                 // Compare the frequencies and remove the one with the lower frequency
//                 if (entry1->getFreq() < entry2->getFreq()) {
//                     it1->second->removeEntry(*(entry1->getKey()));
//                     totalSize_ -= 1;  // Decrease total size by 1 when an entry is removed
//                 } else {
//                     it2->second->removeEntry(*(entry2->getKey()));
//                     totalSize_ -= 1;  // Decrease total size by 1 when an entry is removed
//                 }
//             }
//         }
//     }

// private:
//     std::unordered_map<uint64_t, std::shared_ptr<MemCache>> cache_;
//     size_t totalSize_;  // Store the total size of all MemCache entries

//     // Helper function to get a random entry from a MemCache
//     std::shared_ptr<EntryCache> getRandomEntry(std::shared_ptr<MemCache> memCache, std::mt19937& gen) {
//         std::uniform_int_distribution<> entry_dis(0, memCache->size() - 1);
//         auto it = std::next(memCache->getCache().begin(), entry_dis(gen));
//         return it->second;
//     }
// };
#include <iostream>
#include <unordered_map>
#include <vector>
#include <mutex>

class Entry {
public:
    std::string key;
    int64_t value;
    // unsigned char frequency; // 4-bit frequency counter (0-15)

    Entry(std::string k, int64_t v) : key(k), value(v) {}
};

class IndexCache {
public:
    IndexCache(int capacity, unsigned char freqThreshold)
        : capacity_(capacity), freqThreshold_(freqThreshold), size_(0) {
        head_ = new Node("", 0); // Dummy head
        tail_ = new Node("", 0); // Dummy tail
        head_->next = tail_;
        tail_->prev = head_;
    }

    ~IndexCache() {
        std::lock_guard<std::mutex> lock(mutex_);
        Node* node = head_;
        while (node) {
            Node* next = node->next;
            delete node;
            node = next;
        }
    }

    // Add entries to the cache based on their frequency
    void addEntry(Entry& entry) {
        std::lock_guard<std::mutex> lock(mutex_);
        insert(entry.key, entry.value);
    }

    int64_t get(std::string key) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (map_.find(key) == map_.end()) {
            return -1; // Key not found
        }
        Node* node = map_[key];
        // incrementFrequency(node); // Update frequency on access
        moveToFront(node);
        return node->value;
    }

    unsigned char getFreqThreshold() {
        return freqThreshold_;
    }

private:
    struct Node {
        std::string key;
        int64_t value;
        // unsigned char frequency; // 4-bit frequency counter (0-15)
        Node* prev;
        Node* next;
        Node(std::string k, int64_t v) : key(k), value(v), prev(nullptr), next(nullptr) {}
    };

    int capacity_;
    unsigned char freqThreshold_;
    int size_;
    Node* head_;
    Node* tail_;
    std::unordered_map<std::string, Node*> map_;
    std::mutex mutex_;

    void insert(std::string key, int64_t value) {
        if (map_.find(key) != map_.end()) {
            Node* node = map_[key];
            node->value = value;
            // node->frequency = frequency; // Update frequency
            moveToFront(node);
        } else {
            if (size_ == capacity_) {
                Node* tail = removeTail();
                map_.erase(tail->key);
                delete tail;
                --size_;
            }
            Node* newNode = new Node(key, value);
            // newNode->frequency = frequency; // Set initial frequency
            addToFront(newNode);
            map_[key] = newNode;
            ++size_;
        }
    }

    void addToFront(Node* node) {
        node->next = head_->next;
        node->prev = head_;
        head_->next->prev = node;
        head_->next = node;
    }

    void moveToFront(Node* node) {
        removeNode(node);
        addToFront(node);
    }

    void removeNode(Node* node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    Node* removeTail() {
        Node* node = tail_->prev;
        removeNode(node);
        return node;
    }
};


