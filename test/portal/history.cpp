#include "history.h"
#include "common/wait_path.h"
#include "common/lock_mode.h"

void history::add_op(const tx_op &o) {
  std::scoped_lock l(mutex_);
  history_.push_back(o);
}

struct key {
  key(table_id_t tbl_id, tuple_id_t tpl_id) : table_id_(tbl_id), tuple_id_(tpl_id) {}
  table_id_t table_id_;
  tuple_id_t tuple_id_;
};

struct key_equal {
  bool operator()(const key &k1, const key &k2) const {
    return k1.tuple_id_ == k2.tuple_id_ && k1.table_id_ == k2.table_id_;
  }
};

struct key_hash {
  uint64_t operator()(const key &k) const {
    return (k.table_id_ + k.tuple_id_) * (k.table_id_ + k.tuple_id_ + 1) / 2 + k.table_id_;
  }
};

bool is_conflict(const tx_op &o1, const tx_op o2) {
  if (o1.xid_ == o2.xid_) {
    return false;
  } else {
    if (o1.cmd_ == TX_CMD_RM_READ && o2.cmd_ == TX_CMD_RM_READ) {
      return false;
    } else {
      return true;
    }
  }
}

bool history::is_serializable() {
  std::scoped_lock l(mutex_);
  wait_path path;
  tx_wait_set set;
  std::unordered_map<key, std::vector<tx_op>, key_hash, key_equal> table;
  std::set<xid_t> committed_;
  for (const tx_op o : history_) {
    if (o.cmd_ == TX_CMD_RM_COMMIT || o.cmd_ == TX_CMD_TM_COMMIT) {
      committed_.insert(o.xid_);
    }
  }
  erase_if(history_, [committed_](const tx_op &op) {
    // remove all non access operations and not committed tx_rm operations
    return (op.cmd_ != TX_CMD_RM_READ || op.cmd_ == TX_CMD_RM_WRITE)
        || not committed_.contains(op.xid_);
  });

  // left only committed read/write
  // insert into table
  for (const tx_op o : history_) {
    key k(o.table_id_, o.tuple_id_);
    table[k].push_back(o);
  }

  for (std::pair<const key, std::vector<tx_op>> &p : table) {
    tx_op *prev_op = nullptr;
    for (tx_op &o : p.second) {
      if (prev_op) {
        if (is_conflict(o, *prev_op)) {
          ptr<tx_wait> w(new tx_wait(prev_op->xid_));
          w->out().insert(o.xid_);
          set.add(w);
        }
      }
      prev_op = &o;
    }
  }
  path.merge_dependency_set(set);
  auto find_circle = [](const std::vector<xid_t> &circle) {
    BOOST_LOG_TRIVIAL(info) << "find non-serializable";
    for (auto x : circle) {
      BOOST_LOG_TRIVIAL(info) << "    --> " << x;
    }
  };
  bool found_circle = path.detect_circle(find_circle);
  return !found_circle;
}