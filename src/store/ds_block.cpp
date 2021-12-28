#include "store/ds_block.h"
#include "store/tkrzw_store.h"
#include "store/rocks_store.h"
#include "common/result.hpp"
#include "common/debug_url.h"
#include "common/make_key.h"
#include "common/uniform_generator.hpp"
#include "proto/proto.h"

ds_block::ds_block(
    const config &conf, net_service *service
) :
    conf_(conf),
    service_(service),
    node_id_(conf.node_id()),
    node_name_(id_2_name(conf.node_id())),
    rlb_node_id_(conf.register_to_node_id()),
    registered_(false),
    cno_(0),
    sch_mgr_(conf.schema_manager()) {
  for (std::pair<table_id_t, table_desc> p: sch_mgr_.id2table()) {
    default_tuple_[p.first] = tuple_pb();
    tuple_pb &pb = default_tuple_[p.first];
    for (const column_desc &c: p.second.column_desc_list()) {
      datum *d = pb.add_item();
      d->set_type(c.column_pb_type());
      d->set_binary(c.default_value());
    }
  }
}

void ds_block::handle_debug(const std::string &path, std::ostream &os) {
  if (not boost::regex_match(path, url_json_prefix)) {
    os << BLOCK_DSB << " name:" << node_name_ << std::endl;
    os << "endpoint:" << conf_.this_node_config().node_peer() << std::endl;
    os << "register_to:" << id_2_name(conf_.register_to_node_id()) << std::endl;
    os << "path:" << path << std::endl;
  }
}

void ds_block::on_start() {
#ifdef DB_TYPE_TK
  if (is_store_tk()) {
    store_ = ptr<store>(new tkrzw_store(conf_));
  }
#endif
#ifdef DB_TYPE_ROCKS
  if (not is_store_tk()) {
    store_ = ptr<store>(new rocks_store(conf_));
  }
#endif
  send_register();
  BOOST_LOG_TRIVIAL(info) << "start up DSB " << node_name_ << " ...";
}

void ds_block::on_stop() {
  if (timer_send_register_) {
    timer_send_register_->cancel();
  }
  if (store_) {
    store_->close();
  }
}

void ds_block::send_register() {
  std::scoped_lock l(register_mutex_);

  dsb_register_dsb_request request;
  request.set_source(node_id_);
  request.set_dest(rlb_node_id_);
  auto r = service_->async_send(
      rlb_node_id_, message_type::D2R_REGISTER_REQ, request);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "send error register_dsb error";
  }
  timer_send_register_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_HANDLE),
      boost::asio::chrono::milliseconds(500)));
  auto s = shared_from_this();
  auto fn_timeout = [s](const boost::system::error_code &error) {
    if (!error.failed()) {
      s->send_register();
    } else {
      //
    }
  };
  timer_send_register_->async_wait(fn_timeout);
}

result<void> ds_block::dsb_handle_message(const ptr<connection> &c, message_type, const client_load_data_request &m) {
  handle_load_data_request(m, c);
  return outcome::success();
}

result<void> ds_block::dsb_handle_message(const ptr<connection> &, message_type, const rlb_register_dsb_response &m) {
  handle_register_dsb_response(m);
  return outcome::success();
}

result<void> ds_block::dsb_handle_message(const ptr<connection> &, message_type, const ccb_read_request &m) {
  handle_read_data(m);
  return outcome::success();
}

result<void> ds_block::dsb_handle_message(const ptr<connection> &, message_type, const replay_to_dsb_request &m) {
  handle_replay_to_dsb(m);
  return outcome::success();
}

void ds_block::handle_load_data_request(const client_load_data_request &msg, ptr<connection> conn) {
  BOOST_ASSERT(msg.wid_lower() < msg.wid_upper());
  for (uint32_t wid = msg.wid_lower(); wid <= msg.wid_upper(); wid++) {
    wid_.push_back(wid);
  }

  load_data();
  client_load_data_response response;
  response.set_error_code(uint32_t(EC::EC_OK));
  auto rs = conn->send_message(CLIENT_LOAD_DATA_RESP, response);
  if (not rs) {
    BOOST_LOG_TRIVIAL(error) << "response load data error, " << rs.error().message();
    return;
  }
}

void ds_block::handle_register_dsb_response(
    const rlb_register_dsb_response &response) {
  panel_report msg;
  msg.set_source(conf_.node_id());
  msg.set_dest(conf_.panel_config().node_id());
  msg.set_lead(response.lead());
  msg.set_registered(response.source());
  msg.set_report_type(DSB_REGISTERED_RLB);
  auto r = service_->async_send(msg.dest(), message_type::PANEL_REPORT, msg);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "send panel report error: " << r.error().message();
  }

  std::unique_lock l(register_mutex_);
  if (cno_ != response.cno()) {
    std::string lead = response.lead() ? "leader" : "follower";
    BOOST_LOG_TRIVIAL(info) << "DSB " << node_name_
                            << " registered to RLB "
                            << id_2_name(response.source())
                            << " cno = " << cno_
                            << " " << lead;
    cno_ = response.cno();
    registered_ = true;
  } else {

  }
}

void ds_block::load_data() {
  BOOST_ASSERT(store_);
  load_item();
  load_customer();
  load_stock();
  load_warehouse();
  load_district();
  load_order();
  auto r = store_->sync();
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << node_name_ << "DSB load error";
  }
  BOOST_LOG_TRIVIAL(info) << node_name_ << " DSB load data ";
}

void ds_block::load_item() {
  for (uint32_t i = 1; i <= conf_.get_tpcc_config().num_item(); i++) {
    tuple_pb tuple = gen_tuple(TPCC_ITEM);
    tuple_id_t id = i;
    tuple_id_t key = uint64_to_key(id);
    result<void> r = store_->put(TPCC_ITEM, key, tuple);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << node_name_ << " load item table error";
    }
  }
}
void ds_block::load_customer() {
  const tpcc_config &c = conf_.get_tpcc_config();
  uint32_t num_district = conf_.get_tpcc_config().num_district();
  uint32_t num_customer = conf_.get_tpcc_config().num_customer();
  for (auto wid: wid_) {
    for (uint32_t did = 1; did <= num_district; did++) {
      for (uint32_t cid = 1; cid <= num_customer; cid++) {
        uint32_t id = make_customer_key(wid, did, cid, c.num_warehouse(),
                                        c.num_district());
        tuple_pb tuple = gen_tuple(TPCC_CUSTOMER);
        tuple_id_t key = uint64_to_key(id);
        result<void> r = store_->put(TPCC_CUSTOMER, key, tuple);
        if (!r) {
          BOOST_LOG_TRIVIAL(error) << node_name_ << " load customer table error";
        }
      }
    }
  }
}
void ds_block::load_stock() {
  const tpcc_config &c = conf_.get_tpcc_config();
  for (auto wid: wid_) {
    for (uint32_t iid = 1; iid <= c.num_item(); iid++) {
      uint32_t id = make_stock_key(wid, iid, c.num_warehouse());
      tuple_pb tuple = gen_tuple(TPCC_STOCK);
      tuple_id_t key = uint64_to_key(id);
      result<void> r = store_->put(TPCC_STOCK, key, tuple);
      if (!r) {
        BOOST_LOG_TRIVIAL(error) << node_name_ << " load stock table error";
      }
    }
  }
}
void ds_block::load_warehouse() {

  for (auto wid: wid_) {
    tuple_pb tuple = gen_tuple(TPCC_WAREHOUSE);
    tuple_id_t key = uint64_to_key(wid);
    result<void> r = store_->put(TPCC_WAREHOUSE, key, tuple);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << node_name_ << " load warehouse table error";
    }
  }
}

void ds_block::load_district() {
  const tpcc_config &c = conf_.get_tpcc_config();
  for (auto wid: wid_) {
    for (uint32_t did = 1; did <= c.num_district(); did++) {
      uint32_t id = make_district_key(wid, did, c.num_warehouse());
      tuple_pb tuple = gen_tuple(TPCC_DISTRICT);// = "district";
      tuple_id_t key = uint64_to_key(id);
      result<void> r = store_->put(TPCC_DISTRICT, key, tuple);
      if (!r) {
        BOOST_LOG_TRIVIAL(error) << node_name_ << " load district table error";
      }
    }
  }
}

void ds_block::load_order() {
  const tpcc_config &c = conf_.get_tpcc_config();

  uint32_t max = c.num_order_initalize();
  uint32_t wh = c.num_warehouse();
  uint32_t max_ol = c.num_max_order_line();
  uniform_generator<uint32_t> generator(max_ol / 3, max_ol * 2 / 3);

  for (auto wid: wid_) {
    for (uint32_t oid = max % wid + 1; oid <= max; oid += wh) {
      uint32_t ioid = make_order_key(wid, oid, wh);
      tuple_pb ord_tuple = gen_tuple(TPCC_ORDER);
      tuple_id_t okey = uint64_to_key(ioid);
      result<void> r = store_->put(TPCC_ORDER, okey, ord_tuple);
      if (!r) {
        BOOST_LOG_TRIVIAL(error) << node_name_ << " load order table error";
      }
      uint32_t ol = generator.generate();
      for (uint32_t oliid = 1; oliid <= ol; oliid++) {
        uint32_t id = make_order_line_key(wid, oid, oliid, wh, max_ol);
        tuple_pb order_line_tuple = gen_tuple(TPCC_ORDER_LINE);
        tuple_id_t key = uint64_to_key(id);
        result<void> rp = store_->put(TPCC_ORDER_LINE, key, order_line_tuple);
        if (!rp) {
          BOOST_LOG_TRIVIAL(error) << node_name_ << " load order_line table error";
        }
      }
      if (oid > 2100) {
        result<void> rp = store_->put(TPCC_NEW_ORDER, okey, ord_tuple);
        if (!r) {
          BOOST_LOG_TRIVIAL(error) << node_name_ << " load new_order table error";
        }
      }
    }
  }
}

void ds_block::handle_read_data(
    const ccb_read_request &request) {
  auto s = shared_from_this();
  auto fn = [s, request]() {
    table_id_t table_id = request.table_id();
    tuple_id_t key = request.tuple_id();
    node_id_t source = request.source();
    node_id_t dest = request.dest();
    uint64_t xid = request.xid();
    uint32_t oid = request.oid();
    auto r = s->store_->get(table_id, key);
    dsb_read_response response;
    EC ec = EC::EC_OK;
    if (not r) {
      ec = r.error().code();
    } else {
      *response.mutable_tuple_row()->mutable_tuple() = *r.value();
    }
    response.set_xid(xid);
    response.set_error_code(uint32_t(ec));
    response.set_source(dest);
    response.set_dest(source);
    response.set_oid(oid);
    response.mutable_tuple_row()->set_table_id(table_id);
    response.mutable_tuple_row()->set_tuple_id(key);

    result<void> rs = s->service_->async_send(source, D2C_READ_DATA_RESP, response);
    if (not rs) {

    }
  };
  boost::asio::dispatch(service_->get_service(SERVICE_IO), fn);
}

tuple_pb ds_block::gen_tuple(table_id_t table_id) {
  return default_tuple_[table_id];
}

void ds_block::handle_replay_to_dsb(const replay_to_dsb_request &msg) {
  auto s = shared_from_this();
  auto fn = [s, msg]() {
    auto r = s->store_->replay(msg);
    if (r) {
      replay_to_dsb_response res;
      auto rs = s->service_->async_send(msg.source(), D2R_WRITE_BATCH_RESP, res);
      if (not rs) {
        BOOST_LOG_TRIVIAL(error) << " send replay to dsb response error";
      }
    }
  };
  boost::asio::dispatch(service_->get_service(SERVICE_IO), fn);
}
