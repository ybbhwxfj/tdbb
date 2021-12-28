#include "concurrency/data_mgr.h"

#include <memory>

data_mgr::~data_mgr() = default;

void data_mgr::put(tuple_id_t key, const tuple_pb &tuple) {
  ptr<tuple_list> list;
  key_row_locks_.find_or_insert(
      key,
      [&list](ptr<tuple_list> &value) {
        list = value;
      },
      [&key, &list]() {
        ptr<tuple_list> s = std::make_shared<tuple_list>();
        list = s;
        return std::make_pair(key, s);
      });
  if (list) {
    std::scoped_lock l(list->mutex_);
    list->versions_.push_back(tuple);
  } else {
    BOOST_ASSERT(false);
  }
}

std::pair<tuple_pb, bool> data_mgr::get(tuple_id_t key) {
  std::pair<ptr<tuple_list>, bool> pair = key_row_locks_.find(key);
  if (pair.second) {
    std::scoped_lock l(pair.first->mutex_);
    if (!pair.first->versions_.empty()) {
      return std::make_pair(pair.first->versions_.back(), true);
    }
  }
  return std::make_pair(tuple_pb(), false);
}