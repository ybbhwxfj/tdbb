#include "replog/log_service_impl.h"
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <memory>

const uint64_t INDEX_TX_CLEAN = UINT64_MAX;
const uint64_t INDEX_TX_END = UINT64_MAX - 1;

log_entry_type to_log_entry_type(tx_cmd_type type) {
  if (type == TX_CMD_RM_COMMIT ||
      type == TX_CMD_RM_ABORT ||
      type == TX_CMD_TM_END) {
    return log_entry_type::TX_LOG_END;
  } else {
    return log_entry_type::TX_LOG_WRITE;
  }
}

log_service_impl::log_service_impl(const config conf, ptr<net_service> service) :
    conf_(conf),
    dsb_node_id_(0),
    cno_(0),
    service_(service),
    log_strand_(service->get_service(SERVICE_ASYNC_CONTEXT)) {
  boost::filesystem::path p(conf_.db_path());
  p.append("log");
  rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = &cmp_;
  BOOST_LOG_TRIVIAL(info) << id_2_name(conf.node_id()) << " log path :" << p.c_str();
  rocksdb::Status status = rocksdb::DB::Open(options, p.c_str(), &logs_);
  BOOST_ASSERT(status.ok());
  if (not status.ok()) {
    BOOST_LOG_TRIVIAL(error) << "open rocksdb log error";
    assert(false);
  }
}

log_service_impl::~log_service_impl() {
  delete logs_;
}

void log_service_impl::on_start() {
  tick();
}

void log_service_impl::on_stop() {
  if (logs_) {
    logs_->Close();
    delete logs_;
    logs_ = nullptr;
  }
}

void log_service_impl::set_dsb_id(node_id_t id) {
  std::scoped_lock l(mutex_);
  dsb_node_id_ = id;
}

void log_service_impl::commit_log(const std::vector<ptr<log_entry>> &entry, const log_write_option &opt) {
  rocksdb::WriteBatch batch;
  for (ptr<log_entry> e : entry) {
    uint64_t index = e->index();
    for (const tx_log &l : e->xlog()) {
      uint64_t xid = l.xid();
      tx_log &mlog = const_cast<tx_log &>(l);
      mlog.set_index(e->index());
      mlog.set_term(e->index());
      tx_cmd_type type = l.log_type();
      if (type == TX_CMD_RM_COMMIT) {
        commit_xid_.push_back(xid);
      } else if (type == TX_CMD_RM_ABORT) {
        abort_xid_.push_back(xid);
      } else if (type == TX_CMD_TM_END) {
        commit_xid_.push_back(xid);
      }

      tx_log_index tk(xid, index, to_log_entry_type(mlog.log_type()));
      std::string value = mlog.SerializeAsString();
      rocksdb::Status s1 = batch.Put(rocksdb::Slice(tk.data(), tk.size()), rocksdb::Slice(value));
      if (!s1.ok()) {
        BOOST_ASSERT(false);
        BOOST_LOG_TRIVIAL(error) << "put error";
      }

      tx_log_index ek(index);
      rocksdb::Status s2 = batch.Delete(rocksdb::Slice(ek.data(), ek.size()));
      if (!s2.ok()) {
        BOOST_ASSERT(false);
        BOOST_LOG_TRIVIAL(error) << "delete error";
      }
    }
  }

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
void log_service_impl::write_log(const std::vector<ptr<log_entry>> &entry, const log_write_option &opt) {
  rocksdb::WriteBatch batch;
  for (ptr<log_entry> e : entry) {
    uint64_t index = e->index();
    tx_log_index ek(index);
    std::string ev = e->SerializeAsString();
    rocksdb::Status s = batch.Put(rocksdb::Slice(ek.data(), ek.size()), rocksdb::Slice(ev));
    if (not s.ok()) {
      BOOST_ASSERT(false);
    }
  }
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

void log_service_impl::write_state(const ptr<log_state> &state, const log_write_option &opt) {
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
  tx_log_index k = tx_log_index::state_index();
  std::string value;
  rocksdb::Status s = logs_->Get(rocksdb::ReadOptions(), rocksdb::Slice(k.data(), k.size()), &value);
  if (s.ok()) {
    slice v(value.c_str(), value.size());
    fn1(v);
  }
}

void log_service_impl::retrieve_log(fn_tx_log fn2) {
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

result<ptr<log_entry>> log_service_impl::get_log_entry(uint64_t index) {
  tx_log_index key(index);
  std::string value;
  rocksdb::Status s = logs_->Get(rocksdb::ReadOptions(),
                                 rocksdb::Slice(key.data(), key.size()), &value);
  if (s.ok()) {
    return outcome::failure(EC::EC_NOT_FOUND_ERROR);
  } else {
    ptr<log_entry> p = std::make_shared<log_entry>();
    if (not p->ParseFromString(value)) {
      return outcome::failure(EC::EC_MARSHALL_ERROR);
    } else {
      return outcome::success(p);
    }
  }
}

void log_service_impl::replay_to_dsb() {
  std::vector<xid_t> commit_xid;
  std::vector<xid_t> abort_xid;
  uint64_t cno;
  uint32_t dest;
  {
    std::scoped_lock l(mutex_);
    if (cno_ == 0 || dsb_node_id_ == 0) {
      return;
    }
    cno = cno_;
    dest = dsb_node_id_;
    commit_xid = commit_xid_;
    commit_xid_.clear();
    abort_xid = abort_xid_;
    abort_xid_.clear();

  }

  if (not commit_xid.empty()) {
    replay_to_dsb_request req;
    req.set_cno(cno);
    req.set_source(conf_.node_id());
    req.set_dest(dest);

    // TODO ... send to ...
  }

  if (not abort_xid.empty()) {
    rocksdb::WriteBatch batch;
    for (xid_t xid : abort_xid) {
      tx_log_index key(xid, INT64_MAX, TX_CMD_REPLAY_DSB);
      tx_log log;
      log.set_index(INDEX_TX_CLEAN);
      log.set_xid(xid);
      log.set_log_type(TX_CMD_REPLAY_DSB);
      batch.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(log.SerializeAsString()));
    }
    rocksdb::Status s = logs_->Write(rocksdb::WriteOptions(), &batch);
    if (not s.ok()) {
      BOOST_ASSERT(false);
    }
  }
}

void log_service_impl::handle_replay_to_dsb_response(const replay_to_dsb_response &msg) {
  rocksdb::WriteBatch batch;
  for (const tx_log &l : msg.logs()) {
    xid_t xid = l.xid();
    tx_log_index key(l.xid(), l.index(), to_log_entry_type(l.log_type()));
    tx_log log;
    log.set_index(INDEX_TX_CLEAN);
    log.set_xid(xid);
    log.set_log_type(TX_CMD_REPLAY_DSB);
    batch.Put(rocksdb::Slice(key.data(), key.size()),
              rocksdb::Slice(log.SerializeAsString()));
  }
  rocksdb::Status s = logs_->Write(rocksdb::WriteOptions(), &batch);
  if (not s.ok()) {
    BOOST_ASSERT(false);
  }
}

void log_service_impl::clean_up() {
  tx_log_index key(0, 0, 0);
  std::string value;
  rocksdb::Status s = logs_->Get(rocksdb::ReadOptions(),
                                 rocksdb::Slice(key.data(), key.size()), &value);
  if (not s.ok()) {
    return;
  }

  log_state state;
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
          rocksdb::Status sd = batch.DeleteRange(
              rocksdb::Slice(b.data(), b.size()),
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
  boost::asio::io_context &service = service_->get_service(SERVICE_ASYNC_CONTEXT);
  ptr<boost::asio::steady_timer> timer(
      new boost::asio::steady_timer(
          service, boost::asio::chrono::milliseconds(5000)));
  auto fn_timeout = boost::asio::bind_executor(
      log_strand_,
      [timer, this](const boost::system::error_code &error) {
        if (not error.failed()) {
          tick();
        } else {
          BOOST_LOG_TRIVIAL(error) << " async wait error " << error.message();
        }
      });
  timer->async_wait(fn_timeout);
  replay_to_dsb();
  clean_up();
}