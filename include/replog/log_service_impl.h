#pragma once

#include <fstream>
#include "common/config.h"
#include "common/ptr.hpp"

#include "proto/proto.h"
#include "rocksdb/db.h"
#include "common/slice.h"
#include "common/hash_table.h"
#include "replog/ptr_log_vec.h"
#include "replog/log_service.h"
#include "network/net_service.h"
#include <functional>

class tx_log_index_cmp : public rocksdb::Comparator {
 public:
  tx_log_index_cmp() {

  }

  virtual ~tx_log_index_cmp() {

  }

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    BOOST_ASSERT(a.size() == sizeof(tx_log_index) || b.size() == sizeof(tx_log_index));
    tx_log_index *ap = (tx_log_index *) a.data();
    tx_log_index *bp = (tx_log_index *) b.data();
    return ap->compare(*bp);
  }

  bool Equal(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    BOOST_ASSERT(a.size() == sizeof(tx_log_index) || b.size() == sizeof(tx_log_index));
    tx_log_index *ap = (tx_log_index *) a.data();
    tx_log_index *bp = (tx_log_index *) b.data();
    return ap->equal(*bp);
  }

  const char *Name() const override {
    return "tx_log_index_cmp";
  }

  void FindShortestSeparator(std::string *, const rocksdb::Slice &) const override {

  }

  void FindShortSuccessor(std::string *) const override {

  }

};

class log_service_impl : public log_service {
 private:
  config conf_;
  node_id_t dsb_node_id_;
  uint64_t cno_;
  ptr<net_service> service_;
  rocksdb::DB *logs_;
  tx_log_index_cmp cmp_;
  std::vector<xid_t> commit_xid_;
  std::vector<xid_t> abort_xid_;
  std::mutex mutex_;
  boost::asio::io_context::strand log_strand_;
 public:
  log_service_impl(const config conf, ptr<net_service> service);

  ~log_service_impl();

  void commit_log(const std::vector<ptr<log_entry>> &, const log_write_option &);

  void write_log(const std::vector<ptr<log_entry>> &, const log_write_option &);

  void write_state(const ptr<log_state> &, const log_write_option &);

  void retrieve_state(fn_state fn1);

  void retrieve_log(fn_tx_log fn2);

  result<ptr<log_entry>> get_log_entry(uint64_t index);

  void set_dsb_id(node_id_t id);

  void on_start();

  void on_stop();

  void handle_replay_to_dsb_response(const replay_to_dsb_response &msg);

 private:
  void replay_to_dsb();

  void clean_up();

  void tick();
};