#pragma once

#include "common/ptr.hpp"
#include "proto/proto.h"
#include "common/result.hpp"
#include "network/net_service.h"
#include "tkrzw_dbm_hash.h"

class archieved {
private:
  tkrzw::HashDBM *dbm_;
  std::string path_;
  std::mutex batch_mutex_;

  typedef std::vector<std::pair<std::string, std::string>> key_value_vec;

  void write_key_value_vector(key_value_vec &key_value_vec);

  void write_key_value(const std::string &key, const std::string &value);

  void delete_key_range_vector(key_value_vec &key_value_vec);

  void delete_key_range(const std::string &begin_key, const std::string &end_key);

public:
  archieved() {}
  ~archieved() {}

  void start();

  void update_cno(uint64_t cno);
  void update_config(std::string json);
  void write_operations(const std::vector<tx_operation> &ops);
  void delete_log(uint32_t xid);
  void delete_logs(const std::vector<uint32_t> &xid);

  void force(std::function<void(EC ec)> fn_force);
};