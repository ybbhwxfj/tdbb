#include "concurrency/data_mgr.h"

#include <memory>

data_mgr::~data_mgr() = default;

void data_mgr::put(tuple_id_t key, tuple_pb &&tuple) {
  ptr<tuple_list> list;
  key_row_locks_.find_or_insert(
      key, [&list](ptr<tuple_list> &value) { list = value; },
      [&key, &list]() {
        ptr<tuple_list> s = std::make_shared<tuple_list>();
        list = s;
        return s;
      });
  if (list) {
    std::scoped_lock l(list->mutex_);
    list->versions_.push_back(tuple);
  } else {
    LOG(fatal) << "find tuple when put error";
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

void data_mgr::print() {
  key_row_locks_.traverse([](tuple_id_t key, ptr<tuple_list> v) {
    if (v) {
      LOG(info) << "    key:" << key << ", versions: " << v->versions_.size();
    }
  });
}