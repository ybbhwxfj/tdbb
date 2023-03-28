#include "replog/log_service_impl.h"
#include "common/logger.hpp"
#include <boost/filesystem.hpp>
#include <memory>

const uint64_t INDEX_TX_CLEAN = UINT64_MAX;
const uint64_t INDEX_TX_END = UINT64_MAX - 1;

log_entry_type to_log_entry_type(tx_cmd_type type) {
  if (type == TX_CMD_RM_COMMIT || type == TX_CMD_RM_ABORT ||
      type == TX_CMD_TM_END) {
    return log_entry_type::TX_LOG_END;
  } else {
    return log_entry_type::TX_LOG_WRITE;
  }
}

log_service_impl::log_service_impl(const config conf, ptr<net_service> service)
    : conf_(conf), cno_(0), service_(service),
      log_strand_(service->get_service(SERVICE_ASYNC_CONTEXT)) {
  boost::filesystem::path p(conf_.db_path());
  p.append("log");
  rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = &cmp_;
  LOG(info) << id_2_name(conf.node_id()) << " log path :" << p.c_str();
  rocksdb::Status status = rocksdb::DB::Open(options, p.c_str(), &logs_);
  BOOST_ASSERT(status.ok());
  if (not status.ok()) {
    LOG(error) << "open rocksdb log error";
    assert(false);
  }
}

log_service_impl::~log_service_impl() { delete logs_; }

void log_service_impl::on_start() { tick(); }

void log_service_impl::on_stop() {
  if (logs_) {
    logs_->Close();
    delete logs_;
    logs_ = nullptr;
  }
}

void log_service_impl::write_log(const std::vector<ptr<raft_log_entry>> &entry,
                                 const log_write_option &opt) {
  scoped_time("log_service_impl::write_log", 10);
  rocksdb::WriteBatch batch;
  for (ptr<raft_log_entry> e : entry) {
    uint64_t index = e->index();
    tx_log_index ek(index);
    std::string ev = e->SerializeAsString();
    rocksdb::Status s =
        batch.Put(rocksdb::Slice(ek.data(), ek.size()), rocksdb::Slice(ev));
    if (not s.ok()) {
      LOG(fatal) << "rocksDB batch put error";
    }
  }
  rocksdb::Status sw = logs_->Write(rocksdb::WriteOptions(), &batch);
  if (not sw.ok()) {
    LOG(fatal) << "rocksDB batch write error";
  }
  if (opt.force()) {
    rocksdb::Status s = logs_->SyncWAL();
    if (not s.ok()) {
      LOG(fatal) << "rocksDB sync WAL error";
    }
  }
}

void log_service_impl::write_state(const ptr<raft_log_state> &state,
                                   const log_write_option &opt) {
  scoped_time("log_service_impl::write_state", 10);
  if (not state) {
    return;
  }
  rocksdb::WriteBatch batch;
  tx_log_index key(0, 0, 0);
  std::string value = state->SerializeAsString();
  batch.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value));
  rocksdb::Status sw = logs_->Write(rocksdb::WriteOptions(), &batch);
  if (not sw.ok()) {
    BOOST_ASSERT(false);
  }
  if (opt.force()) {
    rocksdb::Status s = logs_->SyncWAL();
    if (not s.ok()) {
      BOOST_ASSERT(false);
    }
  }
}

void log_service_impl::retrieve_state(fn_state fn1) {
  scoped_time("log_service_impl::retrieve_state", 10);
  tx_log_index k = tx_log_index::state_index();
  std::string value;
  rocksdb::Status s = logs_->Get(rocksdb::ReadOptions(),
                                 rocksdb::Slice(k.data(), k.size()), &value);
  if (s.ok()) {
    slice v(value.c_str(), value.size());
    fn1(v);
  }
}

void log_service_impl::retrieve_log(fn_tx_log fn2) {
  scoped_time("log_service_impl::retrieve_log", 10);
  rocksdb::Iterator *iter = logs_->NewIterator(rocksdb::ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    tx_log_index k(iter->key().data(), iter->key().size());
    slice s(iter->value().data(), iter->value().size());
    if (k.xid() != 0) {
      if (fn2) {
        fn2(k, s);
      } else {
        break;
      }
    }
  }
}

void log_service_impl::clean_up() {
  scoped_time("log_service_impl::clean_up", 10);
  tx_log_index key(0, 0, 0);
  std::string value;
  rocksdb::Status s = logs_->Get(
      rocksdb::ReadOptions(), rocksdb::Slice(key.data(), key.size()), &value);
  if (not s.ok()) {
    return;
  }

  raft_log_state state;
  if (not state.ParseFromString(value)) {
    return;
  }

  uint64_t consistency_index = state.consistency_index();
  if (consistency_index == 0) {
    return;
  }
  rocksdb::ReadOptions opt;
  rocksdb::WriteBatch batch;
  uint64_t xid = 0;
  bool le_consistency = true;
  rocksdb::Iterator *iter = logs_->NewIterator(opt);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    tx_log_index k(iter->key().data(), iter->key().size());
    if (xid != k.xid()) {
      // find a new tx_rm
      le_consistency = true;
    }
    if (k.xid() != 0) {
      if (k.type() == tx_cmd_type::TX_CMD_REPLAY_DSB) {
        if (le_consistency) {
          tx_log_index b = tx_log_index::tx_min_index(xid);
          tx_log_index e = tx_log_index::tx_max_index(xid);
          rocksdb::Status sd =
              batch.DeleteRange(rocksdb::Slice(b.data(), b.size()),
                                rocksdb::Slice(e.data(), e.size()));
          if (not sd.ok()) {
            BOOST_ASSERT(false);
          }
        }
      } else if (k.index() > consistency_index) {
        le_consistency = false;
      }
    }
    xid = k.xid();
  }

  rocksdb::Status sw = logs_->Write(rocksdb::WriteOptions(), &batch);
  if (not sw.ok()) {
    BOOST_ASSERT(false);
  }
}

void log_service_impl::tick() {
  boost::asio::io_context &service =
      service_->get_service(SERVICE_ASYNC_CONTEXT);
  ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
      service, boost::asio::chrono::milliseconds(10000)));
  auto fn_timeout = boost::asio::bind_executor(
      log_strand_, [timer, this](const boost::system::error_code &error) {
        if (not error.failed()) {
          tick();
        } else {
          LOG(error) << " async wait error " << error.message();
        }
      });
  timer->async_wait(fn_timeout);
  clean_up();
}