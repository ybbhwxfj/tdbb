#pragma once

#include <vector>
#include <list>
#include <utility>
#include <mutex>
#include <functional>
#include <boost/ptr_container/ptr_vector.hpp>

template<class KEY, class VALUE, class KEY_HASH, class KEY_EQUAL>
class bucket {
private:
  KEY_HASH key_hash_;
  KEY_EQUAL key_equal_;

public:
  bucket() {
  }

  bool find_or_insert(KEY item,
                      std::function<void(VALUE &value)> fn_if_exist,
                      std::function<std::pair<KEY, VALUE>()> fn_if_absent
  ) {
    std::lock_guard l(mutex_);
    auto iter = table_.find(item);
    if (iter != table_.end()) {
      fn_if_exist(iter->second);
      return true;
    } else {
      auto pair = table_.insert(fn_if_absent());
      return pair.second;
    }
  }

  bool remove(KEY item,
              std::function<void(VALUE value)> fn_if_exist
  ) {
    std::lock_guard l(mutex_);
    auto i = table_.find(item);
    if (i != table_.end()) {
      if (fn_if_exist) {
        fn_if_exist(i->second);
      }
      table_.erase(i);
      return true;
    } else {
      return false;
    }
  }

  std::pair<VALUE, bool> find(KEY item) {
    std::lock_guard l(mutex_);
    auto i = table_.find(item);
    if (i != table_.end()) {
      return std::make_pair(i->second, true);
    } else {
      return std::make_pair(VALUE(), false);
    }
  }

  bool find(KEY item,
            std::function<void(VALUE value)> fn_if_exist
  ) {
    std::lock_guard l(mutex_);
    auto i = table_.find(item);
    if (i != table_.end()) {
      fn_if_exist(i->second);
      return true;
    } else {
      return false;
    }
  }

  bool insert(KEY item,
              VALUE &value
  ) {
    std::lock_guard l(mutex_);
    auto pair = table_.insert(std::make_pair(item, value));
    return pair.second;
  }

  bool insert(KEY item,
              std::function<std::pair<KEY, VALUE>()> fn_if_absent
  ) {
    std::lock_guard l(mutex_);
    if (!table_.contains(item)) {
      auto pair = table_.insert(fn_if_absent());
      return pair.second;
    } else {
      return true;
    }
  }

  void traverse(std::function<void(KEY, VALUE)> fn_key_value) {
    std::scoped_lock l(mutex_);
    for (auto kv: table_) {
      fn_key_value(kv.first, kv.second);
    }
  }
private:
  std::mutex mutex_;
  std::unordered_map<KEY, VALUE, KEY_HASH, KEY_EQUAL> table_;
};

template<class KEY, class VALUE,
    class KEY_HASH = std::hash<KEY>,
    class KEY_EQUAL = std::equal_to<KEY>>
class hash_table {
private:
  KEY_HASH key_hash_;
  KEY_EQUAL key_equal_;
  typedef bucket<KEY, VALUE, KEY_HASH, KEY_EQUAL> hash_bucket;
  boost::ptr_vector<hash_bucket> buckets_;
private:
  void init(size_t size) {
    for (size_t i = 0; i < size; i++) {
      buckets_.push_back(std::unique_ptr<hash_bucket>(new hash_bucket()));
    }
  }
public:
  hash_table() {
    init(1024);
  }

  hash_table(size_t size) {
    init(size);
  }

  ~hash_table() {

  }
  // find if exist
  // insert if absent
  bool find_or_insert(KEY item,
                      std::function<void(VALUE &value)> fn_if_exist,
                      std::function<std::pair<KEY, VALUE>()> fn_if_absent
  ) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];

    return b.find_or_insert(item, fn_if_exist, fn_if_absent);
  }

  std::pair<VALUE, bool> find(KEY item) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];
    return b.find(item);
  }

  bool find(KEY item,
            std::function<void(VALUE value)> fn_if_exist
  ) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];
    return b.find(item, fn_if_exist);
  }

  bool insert(KEY item,
              std::function<std::pair<KEY, VALUE>()> fn_if_absent
  ) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];

    return b.insert(item, fn_if_absent);
  }

  bool insert(KEY item,
              VALUE &value
  ) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];

    return b.insert(item, value);
  }

  bool remove(KEY item, std::function<void(VALUE value)> fn_if_exist) {
    size_t size = buckets_.size();
    size_t hash = key_hash_(item);
    hash_bucket &b = buckets_[hash % size];

    return b.remove(item, fn_if_exist);
  }

  void traverse(std::function<void(KEY, VALUE)> fn_key_value) {
    for (hash_bucket &b: buckets_) {
      b.traverse(fn_key_value);
    }
  }

};