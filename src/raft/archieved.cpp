#include "raft/archieved.h"
#include <boost/filesystem.hpp>
#include "common/make_int.h"

static const uint64_t CNO_KEY = 1;
static const uint64_t CONFIG_KEY = 2;

void archieved::start() {
  tkrzw::HashDBM::TuningParameters tuning_params;
  dbm_ = new tkrzw::HashDBM();
  dbm_->OpenAdvanced("alog.tkh", true, tkrzw::File::OPEN_DEFAULT, tuning_params);
  dbm_->Set("key", "value");

}

void archieved::write_key_value_vector(key_value_vec &) {
  batch_mutex_.lock();

  batch_mutex_.unlock();
}

void archieved::write_key_value(const std::string &, const std::string &) {
  batch_mutex_.lock();

  batch_mutex_.unlock();
}

void archieved::delete_key_range_vector(key_value_vec &) {
  batch_mutex_.lock();

  batch_mutex_.unlock();
}

void archieved::delete_key_range(const std::string &, const std::string &) {

}

void archieved::update_cno(uint64_t cno) {
  std::string key;
  std::string value;
  key.append((const char *)&CNO_KEY, sizeof(CNO_KEY));
  value.append((const char *)&cno, sizeof(cno));
  write_key_value(key, value);
}

void archieved::update_config(std::string json) {
  std::string key;
  std::string value;
  key.append((const char *)&CONFIG_KEY, sizeof(CONFIG_KEY));
  value.append(json);
  write_key_value(key, value);
}

void archieved::write_operations(const std::vector<tx_operation> &ops) {
  key_value_vec kvs;
  for (const tx_operation &op: ops) {
    uint32_t xid = op.xid();
    uint32_t index = op.index();
    uint64_t id = make_uint64(xid, index);
    std::string key((const char *)&id, sizeof(key));
    std::string value;
    bool ok = op.SerializeToString(&value);
    if (!ok) {
      assert(false);
    }
    kvs.emplace_back(std::make_pair(key, value));
  }
  write_key_value_vector(kvs);
}

void archieved::delete_logs(const std::vector<uint32_t> &xids) {
  key_value_vec kvs;
  for (auto xid: xids) {
    uint64_t lower = make_uint64(xid, 0);
    uint64_t high = make_uint64(xid, UINT32_MAX);
    std::string begin_key((const char *)&lower, sizeof(lower));
    std::string end_key((const char *)&lower, sizeof(high));
    kvs.emplace_back(std::make_pair(begin_key, end_key));
  }
}

void archieved::delete_log(uint32_t xid) {
  uint64_t lower = make_uint64(xid, 0);
  uint64_t high = make_uint64(xid, UINT32_MAX);
  std::string begin_key((const char *)&lower, sizeof(lower));
  std::string end_key((const char *)&lower, sizeof(high));
  delete_key_range(begin_key, end_key);
}

void archieved::force(std::function<void(EC ec)>) {

}