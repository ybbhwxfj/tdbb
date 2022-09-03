#pragma once

#include <vector>
#include <list>
#include <utility>
#include <mutex>
#include <functional>
#include <boost/ptr_container/ptr_vector.hpp>
#include <tbb/concurrent_hash_map.h>
#include <boost/functional/hash.hpp>

template<class KEY>
struct MyHashCompare {

  inline static size_t hash(const KEY &x) {
    return boost::hash_value(x);
  }

  //! True if keys are equal
  inline static bool equal(const KEY &x, const KEY &y) {
    return x == y;
  }
};

template<class KEY, class VALUE>
class hash_table {
 private:

  typedef tbb::concurrent_hash_map<KEY, VALUE, MyHashCompare<KEY>> tbb_hash_map;
  tbb_hash_map hash_map_;


 private:
 public:
  hash_table(): hash_map_(128) {

  }

  hash_table(size_t size):hash_map_(size) {

  }

  ~hash_table() {

  }

  // find if exist
  // insert if absent
  bool find_or_insert(KEY item,
                      std::function<void(VALUE &value)> fn_if_exist,
                      std::function<VALUE()> fn_if_absent
  ) {
    typename tbb_hash_map::accessor accessor;
    auto is_new = this->hash_map_.insert(accessor, item);
    if (is_new) {
      accessor->second = fn_if_absent();
      return false;
    } else {
      fn_if_exist(accessor->second);
      return true;
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

  bool find(KEY item,
            std::function<void(VALUE value)> fn_if_exist
  ) {
    typename tbb_hash_map::const_accessor accessor;
    bool found = this->hash_map_.find(accessor, item);
    if (found) {
      fn_if_exist(accessor->second);
    } else {
      return false;
    }
  }

  bool insert(KEY item,
              std::function<VALUE()> fn_if_absent
  ) {
    typename tbb_hash_map::accessor accessor;
    auto is_new = this->hash_map_.insert(accessor, item);
    if (is_new) {
      accessor->second = fn_if_absent();
      return true;
    } else {
      return false;
    }
  }

  bool insert(KEY item,
              VALUE &value
  ) {
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
    for (auto i = this->hash_map_.begin(); i != this->hash_map_.end(); ++i) {
      fn_key_value(i->first, i->second);
    }
  }
};