#include "store/rocks_store.h"
#ifdef DB_TYPE_ROCKS
#include "common/endian.h"
#include "common/logger.hpp"
#include "common/tx_log.h"
#include <boost/filesystem.hpp>
#include <map>

std::map<rocksdb::Status::Code, EC> __rockserr2ec_map = {
    {rocksdb::Status::Code::kOk, EC::EC_OK},
    {rocksdb::Status::Code::kNotFound, EC::EC_NOT_FOUND_ERROR},
    {rocksdb::Status::Code::kCorruption, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kNotSupported, EC::EC_NOT_IMPLEMENTED},
    {rocksdb::Status::Code::kInvalidArgument, EC::EC_INVALID_ARGUMENT},
    {rocksdb::Status::Code::kIOError, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kMergeInProgress, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kIncomplete, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kShutdownInProgress, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kTimedOut, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kAborted, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kBusy, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kExpired, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kTryAgain, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kCompactionTooLarge, EC::EC_IO_ERROR},
    {rocksdb::Status::Code::kColumnFamilyDropped, EC::EC_IO_ERROR},
};

EC rocks_to_ec(rocksdb::Status::Code status) {
  auto i = __rockserr2ec_map.find(status);
  if (i != __rockserr2ec_map.end()) {
    return i->second;
  } else {
    BOOST_ASSERT(false);
    return EC::EC_UNKNOWN;
  }
}

rocks_store::rocks_store(const config &conf)
    : conf_(conf), path_(conf.db_path()), node_id_(conf.node_id()),
      node_name_(id_2_name(conf.node_id())) {
  boost::filesystem::path dir(path_);
  dir.append("rocks");
  if (!boost::filesystem::exists(dir)) {
    boost::filesystem::create_directories(dir);
  } else {
    if (!boost::filesystem::is_directory(dir)) {
      LOG(error) << path_ << " is not a directory";
    }
  }

  rocksdb::Options options;
  options.create_if_missing = true;
  options.comparator = &cmp_;
  LOG(info) << id_2_name(conf.node_id()) << " rocksdb path :" << dir.c_str();
  rocksdb::Status status = rocksdb::DB::Open(options, dir.c_str(), &db_);
  if (not status.ok()) {
    BOOST_ASSERT(status.ok());
    LOG(error) << "cannot open rocksdb " << dir.c_str();
  }
}

rocks_store::~rocks_store() { delete db_; }

void rocks_store::close() {
  if (db_) {
    db_->Close();
  }
}

result<void> rocks_store::replay(ptr<std::vector<ptr<tx_operation>>> ops) {
  rocksdb::WriteBatch batch;

  for (auto op_ptr : *ops) {
    tx_operation &op = *op_ptr;
    switch (op.op_type()) {
    case tx_op_type::TX_OP_DELETE: {
      table_id_t table_id = op.tuple_row().table_id();
      tuple_id_t key = op.tuple_row().tuple_id();
      key128 k(table_id, key);
      rocksdb::Status s = batch.Delete(rocksdb::Slice(k));
      if (!s.ok()) {
        // TODO ...
      }
    }
      break;
    case tx_op_type::TX_OP_INSERT:
    case tx_op_type::TX_OP_UPDATE: {
      table_id_t table_id = op.tuple_row().table_id();
      tuple_id_t key = op.tuple_row().tuple_id();
      if (table_id >= MAX_TABLES) {
        // TODO ...
      }

      // bool overwrite = op.op_type() == TX_OP_UPDATE;

      key128 k(table_id, key);
      BOOST_ASSERT(!is_tuple_nil(op.tuple_row().tuple()));
      rocksdb::Status s =
          batch.Put(rocksdb::Slice(k), rocksdb::Slice(op.tuple_row().tuple()));
      if (!s.ok()) {
        // TODO ...
      }
    }
      break;
    default:break;
    }
  }

  rocksdb::Status sw = db_->Write(rocksdb::WriteOptions(), &batch);
  EC ecw = rocks_to_ec(sw.code());
  if (ecw != EC::EC_OK) {
    return outcome::failure(ecw);
  }
  rocksdb::Status ss = db_->SyncWAL();
  EC ecs = rocks_to_ec(ss.code());
  if (ecs != EC::EC_OK) {
    return outcome::failure(ecs);
  } else {
    return outcome::success();
  }
}
result<void> rocks_store::put(table_id_t table_id, tuple_id_t key,
                              tuple_pb &&tuple) {
  // LOG(trace) <<  node_name_  << " load tuple, table id:" << table_id << "
  // tuple id:" << key;
  if (table_id >= MAX_TABLES) {
    return outcome::failure(EC::EC_UNKNOWN_TABLE_ID);
  }
  BOOST_ASSERT(!is_tuple_nil(tuple));
  key128 k(table_id, key);
  rocksdb::Status sw = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(k),
                                rocksdb::Slice(tuple));
  EC ecw = rocks_to_ec(sw.code());
  if (ecw != EC::EC_OK) {
    return outcome::failure(ecw);
  } else {
    return outcome::success();
  }
}

result<void> rocks_store::sync() {
  rocksdb::Status ss = db_->SyncWAL();
  EC ec = rocks_to_ec(ss.code());
  if (ec != EC::EC_OK) {
    LOG(error) << node_name_ << "sync wal";
    return outcome::failure(ec);
  } else {

    LOG(trace) << node_name_ << "sync wal";
    return outcome::success();
  }
}

result<ptr<tuple_pb>> rocks_store::get(table_id_t table_id, tuple_id_t key) {
  if (table_id >= MAX_TABLES) {
    return outcome::failure(EC::EC_UNKNOWN_TABLE_ID);
  }
  ptr<std::string> tuple(cs_new<std::string>());
  key128 k(table_id, key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), k, &(*tuple));
  EC ec = rocks_to_ec(s.code());
  if (ec == EC::EC_NOT_FOUND_ERROR) {
    LOG(debug) << "cannot find tuple, table id:" << table_id
               << " tuple id:" << key;
  }
  if (ec == EC::EC_OK) {

    BOOST_ASSERT(!is_tuple_nil(*tuple));
    return outcome::success(tuple);
  } else {
    return outcome::failure(ec);
  }
  BOOST_ASSERT(not(ec == EC::EC_OK && tuple->empty()));
  return outcome::success();
}

#endif // DB_TYPE_ROCKS