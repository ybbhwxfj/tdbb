#include "store/tkrzw_store.h"
#ifdef DB_TYPE_TK
#include "common/endian.h"
#include "common/logger.hpp"
#include "common/tx_log.h"
#include "tkrzw_dbm_hash.h"
#include <boost/filesystem.hpp>
#include <map>

std::map<tkrzw::Status::Code, EC> __tkrzw2ec_map = {
    {tkrzw::Status::Code::SUCCESS, EC::EC_OK},
    {tkrzw::Status::Code::UNKNOWN_ERROR, EC::EC_UNKNOWN},
    {tkrzw::Status::Code::SYSTEM_ERROR, EC::EC_SYSTEM_ERROR},
    {tkrzw::Status::Code::NOT_IMPLEMENTED_ERROR, EC::EC_NOT_IMPLEMENTED},
    {tkrzw::Status::Code::PRECONDITION_ERROR, EC::EC_PRECONDITION_ERROR},
    {tkrzw::Status::Code::INVALID_ARGUMENT_ERROR, EC::EC_INVALID_ARGUMENT},
    {tkrzw::Status::Code::CANCELED_ERROR, EC::EC_CANCELED_ERROR},
    {tkrzw::Status::Code::NOT_FOUND_ERROR, EC::EC_NOT_FOUND_ERROR},
    {tkrzw::Status::Code::PERMISSION_ERROR, EC::EC_PERMISSION_ERROR},
    {tkrzw::Status::Code::INFEASIBLE_ERROR, EC::EC_INFEASIBLE_ERROR},
    {tkrzw::Status::Code::DUPLICATION_ERROR, EC::EC_DUPLICATION_ERROR},
    {tkrzw::Status::Code::BROKEN_DATA_ERROR, EC::EC_BROKEN_DATA_ERROR},
    {tkrzw::Status::Code::NETWORK_ERROR, EC::EC_NETWORK_ERROR},
    {tkrzw::Status::Code::APPLICATION_ERROR, EC::EC_APPLICATION_ERROR},
};

EC status_to_ec(const tkrzw::Status &status) {
  auto i = __tkrzw2ec_map.find(status.GetCode());
  if (i != __tkrzw2ec_map.end()) {
    return i->second;
  } else {
    BOOST_ASSERT(false);
    return EC::EC_UNKNOWN;
  }
}

tkrzw_store::tkrzw_store(const config &conf)
    : conf_(conf), path_(conf.db_path()), node_id_(conf.node_id()),
      node_name_(id_2_name(conf.node_id())) {
  boost::filesystem::path dir(path_);
  dir.append("tkrzw");
  if (!boost::filesystem::exists(dir)) {
    boost::filesystem::create_directories(dir);
  } else {
    if (!boost::filesystem::is_directory(dir)) {
      LOG(error) << dir.c_str() << " is not a directory";
    }
  }

  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    dbm_[i] = new tkrzw::HashDBM();
    boost::filesystem::path p(dir);
    p.append(std::to_string(i));
    tkrzw::Status status = dbm_[i]->Open(p.c_str(), true);
    if (!status.IsOK()) {
      LOG(error) << "open tkrzw " << p.c_str() << " error";
    }
  }
}

tkrzw_store::~tkrzw_store() {
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    delete dbm_[i];
  }
}

result<void> tkrzw_store::replay(ptr<std::vector<ptr<tx_operation>>> ops) {
  for (auto op_ptr : *ops) {
    tx_operation &op = *op_ptr;
    switch (op.op_type()) {
    case tx_op_type::TX_OP_DELETE: {
      table_id_t table_id = op.tuple_row().table_id();
      tuple_id_t key = op.tuple_row().tuple_id();
      if (table_id >= MAX_TABLES) {
        // TODO ...
      }

      tkrzw::Status status = dbm_[table_id]->Remove(tupleid2binary(key));
      if (!status.IsOK()) {
        return outcome::failure(status_to_ec(status));
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

      bool overwrite = op.op_type() == TX_OP_UPDATE;
      tkrzw::Status status = dbm_[table_id]->Set(
          tupleid2binary(key), op.tuple_row().tuple(), overwrite);
      if (!status.IsOK()) {
        return outcome::failure(status_to_ec(status));
      }
    }
      break;
    default:break;
    }
  }

  return outcome::success();
}

void tkrzw_store::close() {
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    dbm_[i]->Close();
  }
}

result<void> tkrzw_store::put(table_id_t table_id, tuple_id_t key,
                              tuple_pb &&tuple) {
  if (table_id >= MAX_TABLES) {
    return outcome::failure(EC::EC_UNKNOWN_TABLE_ID);
  }
  std::string s((const char *) &key, sizeof(key));
  tkrzw::Status status = dbm_[table_id]->Set(s, tuple, true);
  EC ec = status_to_ec(status);
  if (!status.IsOK()) {
    LOG(error) << node_name_ << " put table_id=" << key << ", tuple_id=" << key
               << " failed";
    return outcome::failure(ec);
  } else {

    LOG(trace) << node_name_ << " put table_id=" << table_id
               << ", tuple_id=" << key << " success";
    return outcome::success();
  }
}

result<ptr<tuple_pb>> tkrzw_store::get(table_id_t table_id, tuple_id_t key) {
  if (table_id >= MAX_TABLES) {
    return outcome::failure(EC::EC_UNKNOWN_TABLE_ID);
  }

  ptr<tuple_binary> tuple(cs_new<tuple_binary>());
  std::string s((const char *) &key, sizeof(key));
  tkrzw::Status status = dbm_[table_id]->Get(s, &(*tuple));
  EC ec = status_to_ec(status);
  if (ec == EC::EC_NOT_FOUND_ERROR) {
    LOG(debug) << "cannot find tuple, table id:" << table_id
               << " tuple id:" << key;
  }
  BOOST_ASSERT(not(ec == EC::EC_OK && tuple->empty()));
  if (ec == EC::EC_OK) {
    return outcome::success(tuple);
  } else {
    return outcome::success();
  }
}

result<void> tkrzw_store::sync() { return outcome::success(); }
#endif // DB_TYPE_TK
