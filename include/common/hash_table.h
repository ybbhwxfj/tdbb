#pragma once

#include <boost/functional/hash.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <functional>
#include <list>
#include <mutex>
#include <tbb/concurrent_hash_map.h>
#include <utility>
#include <vector>

template<class KEY> struct MyHashCompare {

  inline static size_t hash(const KEY &x) { return boost::hash_value(x); }

  //! True if keys are equal
  inline static bool equal(const KEY &x, const KEY &y) { return x==y; }
};

template<class KEY, class VALUE> class concurrent_hash_table {
private:
  typedef tbb::concurrent_hash_map<KEY, VALUE> tbb_hash_map;
  tbb_hash_map hash_map_;

private:
public:
  concurrent_hash_table() : hash_map_(128) {}

  concurrent_hash_table(size_t size) : hash_map_(size) {}

  ~concurrent_hash_table() {}

  // find if exist
  // insert if absent
  std::pair<VALUE, bool>
  find_or_insert(KEY item, std::function<void(VALUE &value)> fn_if_exist,
                 std::function<VALUE()> fn_if_absent) {
    typename tbb_hash_map::accessor accessor;
    auto is_new = this->hash_map_.insert(accessor, item);
    if (is_new) {
      VALUE value = fn_if_absent();
      accessor->second = value;
      return std::make_pair(value, false);
    } else {
      VALUE value = accessor->second;
      fn_if_exist(accessor->second);
      return std::make_pair(value, true);
    }
  }

  std::pair<VALUE, bool> find(KEY item) {
    typename tbb_hash_map::const_accessor accessor;
    bool found = this->hash_map_.find(accessor, item);
    if (found) {
      return std::make_pair(accessor->second, true);
    } else {
      return std::make_pair(VALUE(), false);
    }
  }

  bool find(KEY item, std::function<void(VALUE value)> fn_if_exist) {
    typename tbb_hash_map::const_accessor accessor;
    bool found = this->hash_map_.find(accessor, item);
    if (found) {
      fn_if_exist(accessor->second);
    } else {
      return false;
    }
  }

  bool insert(KEY item, std::function<VALUE()> fn_if_absent) {
    typename tbb_hash_map::accessor accessor;
    auto is_new = this->hash_map_.insert(accessor, item);
    if (is_new) {
      accessor->second = fn_if_absent();
      return true;
    } else {
      return false;
    }
  }

  bool insert(KEY item, VALUE &value) {
    typename tbb_hash_map::accessor accessor;
    auto is_new = this->hash_map_.insert(accessor, item);
    if (is_new) {
      accessor->second = value;
      return true;
    } else {
      return false;
    }
  }

  bool remove(KEY item, std::function<void(VALUE value)> fn_if_exist) {
    typename tbb_hash_map::const_accessor accessor;
    bool found = this->hash_map_.find(accessor, item);
    if (found) {
      if (fn_if_exist) {
        fn_if_exist(accessor->second);
      }
      return true;
    } else {
      return false;
    }
  }

  void traverse(std::function<void(KEY, VALUE)> fn_key_value) {
    for (auto i = this->hash_map_.begin(); i!=this->hash_map_.end(); ++i) {
      fn_key_value(i->first, i->second);
    }
  }
};

template<class KEY, class VALUE> class hash_table {
private:
  typedef std::unordered_map<KEY, VALUE> std_hash_map;
  std_hash_map hash_map_;

private:
public:
  hash_table() : hash_map_(128) {}

  hash_table(size_t size) : hash_map_(size) {}

  ~hash_table() {}

  // find if exist
  // insert if absent
  std::pair<VALUE, bool>
  find_or_insert(KEY item, std::function<void(VALUE &value)> fn_if_exist,
                 std::function<VALUE()> fn_if_absent) {

    typename std_hash_map::iterator iter = hash_map_.find(item);
    if (iter!=hash_map_.end()) {
      VALUE value = fn_if_absent();
      hash_map_.insert(std::make_pair(item, value));
      return std::make_pair(value, false);
    } else {
      VALUE value = iter->second;
      fn_if_exist(iter->second);
      return std::make_pair(value, true);
    }
  }

  std::pair<VALUE, bool> find(KEY item) {
    typename std_hash_map::iterator iter = this->hash_map_.find(item);
    if (iter==hash_map_.end()) {
      return std::make_pair(iter->second, true);
    } else {
      return std::make_pair(VALUE(), false);
    }
  }

  bool find(KEY item, std::function<void(VALUE value)> fn_if_exist) {
    typename std_hash_map::iterator iter = hash_map_.find(item);
    if (iter!=hash_map_.end()) {
      fn_if_exist(iter->second);
    } else {
      return false;
    }
  }

  bool insert(KEY item, std::function<VALUE()> fn_if_absent) {
    std::pair<typename std_hash_map::iterator, bool> pair =
        hash_map_.insert(std::make_pair(item, fn_if_absent));
    return pair.second;
  }

  bool insert(KEY item, VALUE &value) {
    std::pair<typename std_hash_map::iterator, bool> pair =
        hash_map_.insert(std::make_pair(item, value));
    return pair.second;
  }

  bool remove(KEY item, std::function<void(VALUE value)> fn_if_exist) {
    typename std_hash_map::iterator iter = hash_map_.find(item);
    if (iter!=hash_map_) {
      fn_if_exist(iter->second);
      hash_map_.erase(iter);
      return true;
    } else {
      return false;
    }
  }

  void traverse(std::function<void(KEY, VALUE)> fn_key_value) {
    for (auto i = hash_map_.begin(); i!=hash_map_.end(); ++i) {
      fn_key_value(i->first, i->second);
    }
  }
};