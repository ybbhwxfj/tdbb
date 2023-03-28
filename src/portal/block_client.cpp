#include "portal/block_client.h"
#include "common/config.h"
#include "common/define.h"
#include "network/message_processor.h"
#include "network/net_service.h"
#include "network/sock_server.h"
#include <boost/program_options.hpp>
#include <iostream>
#include <memory>
#include <utility>

const char *BC_COMMAND_LOAD = "load";
const char *BC_COMMAND_RUN = "benchmark";

void sort_dependency_array(dependency_set_array &array);

void block_client::run_command(const std::string &command) {

  http_handler debug_handler = [this](const std::string &path,
                                      std::ostream &os) {
    this->client_handle_debug(path, os);
  };

  std::string address = conf_.this_node_config().address();
  uint16_t port = uint16_t(conf_.this_node_config().port() + 1000);
  LOG(info) << conf_.node_name() << "debug server listen on port " << port;
  debug_ = std::make_shared<debug_server>("0.0.0.0", port, debug_handler);
  debug_->start();
  set_thread_name("FE");
  try {
    if (command == BC_COMMAND_LOAD) {
      load_data();
    } else if (command == BC_COMMAND_RUN) {
      run_benchmark();
    } else if (command.empty()) {
      load_data();
      run_benchmark();
    } else {
      LOG(error) << "unknown command " << command;
    }
    LOG(info) << "run command " << command << " done";
  } catch (std::exception &ex) {
    LOG(error) << "catch exception when run command " << command
               << " error: " << ex.what();
  }
}

void block_client::close() {
  if (close_server_) {
    send_close_request();
    LOG(info) << "block server closed;";
  }
  if (debug_) {
    debug_->stop();
    debug_->join();
    LOG(info) << "debug server closed";
  }
}

void block_client::load_data() {
  bool last_one = false;
  for (size_t i = 0; i < conf_.node_client_list().size(); i++) {
    if (i + 1 == conf_.node_client_list().size() &&
        conf_.node_client_list()[i].node_name() == conf_.node_name()) {
      last_one = true;
    }
  }
  if (last_one) {
    std::vector<ptr<boost::thread>> threads;
    ptr<workload> wl(new workload(conf_));
    for (const node_config &nc : conf_.node_server_list()) {
      if (is_dsb_block(nc.node_id())) {
        threads.emplace_back(new boost::thread(boost::bind(
            &block_client::load_data_thread, this, nc.node_id(), wl)));
      }
    }

    for (ptr<boost::thread> &t : threads) {
      t->join();
    }
  }

  LOG(info) << "block_client load data done";
}

void block_client::run_benchmark() {
  ptr<workload> wl(new workload(conf_));
  wl_ = wl;
  std::vector<ptr<boost::thread>> threads;
  uint32_t num_rg_ = conf_.num_rg();
  {
    ptr<boost::thread> t = std::make_shared<boost::thread>(
        boost::bind(&block_client::run_sync_thread, this));
    threads.push_back(t);
  }
  connect_sync();

  for (const auto &n : conf_.node_server_list()) {
    if (is_ccb_block(n.node_id())) {
      db_client client(conf_.az_id(), n);
      for (;;) {
        bool ok = client.connect();
        if (ok) {
          break;
        }
        sleep(1);
      }
    }
  }

  for (uint32_t term_id = term_id_begin_; term_id <= term_id_end_; term_id++) {
    shard_id_t sd_id = (term_id - 1) % num_rg_ + 1;
    bool log = term_id == term_id_begin_;
    ptr<boost::thread> t = std::make_shared<boost::thread>(boost::bind(
        &block_client::run_benchmark_thread, this, sd_id, term_id, wl, log));
    threads.push_back(t);
  }

  ptr<boost::thread> t = std::make_shared<boost::thread>(
      boost::bind(&block_client::output_result_thread, this, wl));
  threads.push_back(t);

  for (ptr<boost::thread> &th : threads) {
    th->join();
  }
  // barrier 2
  LOG(info) << "block_client benchmark done";
}

void block_client::load_data_thread(node_id_t node_id,
                                    const ptr<workload> &wl) {
  std::string thread_name = "load_data";
  set_thread_name(thread_name);
  wl->load_data(node_id);
}

void block_client::run_benchmark_thread(shard_id_t sd_id, uint32_t terminal_id,
                                        ptr<workload> &wl, bool log) {
  std::string thread_name = "bt_" + std::to_string(terminal_id);
  set_thread_name(thread_name);

  // wait all thread client connect to the database
  if (log) {
    LOG(info) << "connect to database";
  }
  wl->connect_database(sd_id, terminal_id);

  barrier0_->wait();
  LOG(trace) << "barrier wait, connect to database " << terminal_id;

  if (log) {
    LOG(info) << conf_.node_name() << " CLIENT generate new order";
  }
  wl->gen_procedure(sd_id, terminal_id);

  // wait all thread generate requests
  barrier1_->wait();
  LOG(trace) << "barrier wait, generate new order " << terminal_id;

  barrier_wait("barrier wait load and generate");

  if (conf_.get_test_config().percent_cached_tuple() > 0.0) {
    LOG(info) << "warm up cache " << sd_id << " terminal " << terminal_id;
    if (log) {
      LOG(trace) << conf_.node_name() << " CLIENT warm up cache";
    }
    barrier_warm1_->wait();
    wl->warm_up_cached1(sd_id, terminal_id);
    barrier_warm2_->wait();
    LOG(info) << "wait on " << sd_id << " terminal " << terminal_id;
    wl->warm_up_cached2(sd_id, terminal_id);
  } else {
    LOG(trace) << "non exist in cache " << sd_id << " terminal " << terminal_id;
  }

  sleep(2);

  barrier_wait("barrier wait warmup");
  LOG(trace) << "wait on shard " << sd_id << " terminal " << terminal_id;
  barrier2_->wait();

  LOG(trace) << "run new-order on SHARD " << sd_id << " terminal " << terminal_id;
  if (log) {
    LOG(info) << "CLIENT run new-order";
  }
  wl->run_new_order(sd_id, terminal_id);
  if (log) {
    LOG(info) << "CLIENT run new-order done";
  }
  // barrier_wait("barrier wait done");
}

void block_client::output_result_thread(ptr<workload> &wl) {
  barrier2_->wait();
  bool send = false;
  while (true) {
    auto start = std::chrono::steady_clock::now();
    boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));
    auto end = std::chrono::steady_clock::now();
    std::chrono::nanoseconds duration = end - start;
    if (wl->is_stopped() && !send) {
      bench_stop();
      send = true;
    }
    bool stop = wl->output_result(duration);
    if (stop) {
      break;
    }
  }
  tpm_statistic stat = wl->total_stat();

  std::string barrier_name = "wait_finish";
  {
    tpm_stat proto;
    stat.to_proto(proto);
    std::string payload = proto.SerializeAsString();
    barrier_wait(barrier_name, payload);
  }

  ptr<barrier_net> barrier = get_barrier_net(barrier_name);
  std::unordered_map<node_id_t, std::string> payloads = barrier->payload();
  tpm_statistic total;
  for (auto p : payloads) {
    tpm_stat proto;
    if (!proto.ParseFromString(p.second)) {
      PANIC("parse proto error");
    }
    tpm_statistic s;
    s.from_proto(proto);
    total.add(s);
  }

  bench_result res = total.compute_bench_result(conf_.final_num_terminal(), conf_.node_name());

  boost::json::object j = res.to_json();
  std::stringstream ssm;
  ssm << j;
  std::string json_str = ssm.str();
  boost::filesystem::path file_path(conf_.db_path());
  file_path.append("result.json");
  std::ofstream f(file_path.c_str());
  f << json_str << std::endl;

  stop_sync();
}

void block_client::send_close_request() {
  std::set<uint32_t> closed_node;

  for (;;) {
    int not_closed = 0;
    for (const auto &n : conf_list_) {
      if (closed_node.contains(n.node_id())) {
        continue;
      }
      db_client client(conf_.az_id(), n);
      if (client.connect()) {
        close_request req;
        result<void> r = client.send_message(CLOSE_REQ, req);
        if (!r) {
          LOG(error) << "close request send error " << r.error().message();
        }
        not_closed++;
      } else {
        closed_node.insert(n.node_id());
      }
    }
    if (not_closed == 0) {
      break;
    }
    sleep(3);
  }
}

void block_client::client_handle_debug(const std::string &path,
                                       std::ostream &os) {
  for (const auto &c : conf_list_) {
    const std::string &addr = c.address();
    auto port = uint16_t(c.port() + 1000);
    std::stringstream ssm;
    debug_request(addr, port, path, ssm);
    if (boost::regex_match(path, url_json_prefix)) {
      response_[c.node_id()] = ssm.str();
    } else {
      os << ssm.str() << std::endl << std::endl;
    }
  }

  if (boost::regex_match(path, url_json_prefix)) {
    os << std::endl;
    if (boost::regex_match(path, url_dep)) {
      debug_deadlock(os);
    } else if (boost::regex_match(path, url_json_deadlock)) {
      debug_deadlock(os);
    }
  }
}

void block_client::debug_deadlock(std::ostream &os) {
  wait_path p;
  dependency_set_array array;
  for (auto &i : response_) {
    dependency_set &ds = *array.add_array();
    // os << i.second << std::endl;
    bool ok = json_to_pb(i.second, ds);
    if (!ok) {
      BOOST_ASSERT(false);
    }
    p.add_dependency_set(ds);
  }
  sort_dependency_array(array);
  auto json = pb_to_json(array);
  json = json_pretty(json);
  os << json << std::endl;
  os << "---------------" << std::endl;
  auto fn = [&os](const std::vector<xid_t> &circle) {
    for (xid_t x : circle) {
      os << "->" << x;
    }
    os << std::endl;
  };
  p.detect_circle(fn);
}

void sort_dependency_array(dependency_set_array &array) {
  for (dependency_set &ds : *array.mutable_array()) {
    for (dependency &d : *ds.mutable_dep()) {
      std::sort(d.mutable_in()->begin(), d.mutable_in()->end(),
                [](uint64_t x, uint64_t y) { return x < y; });
    }
    std::sort(ds.mutable_dep()->begin(), ds.mutable_dep()->end(),
              [](const dependency &x, const dependency &y) {
                return x.out() < y.out();
              });
  }
}

block_client::block_client(const config &conf, bool close_server)
    : close_server_(close_server), conf_(conf), term_id_begin_(1),
      term_id_end_(conf.final_num_terminal()) {
  conf_list_ = conf_.node_server_list();
  setup_terminal_id();
}

block_client::~block_client() {}

void client_sync_run(config conf, ptr<processor> message_processor) {
  std::string n =
      block_type_list_2_string(conf.this_node_config().block_type_list());
  std::string name =
      "FE_" + n + std::to_string(conf.shard_ids()[0]) + std::to_string(conf.az_id());
  set_thread_name(name);

  BOOST_ASSERT(!conf.node_name().empty());
  BOOST_ASSERT(conf.this_node_config().port() != 0);

  // all these stuff(net_service, sock_server, blocks) has program(this
  // function) life-time, we keep it in smart pointer

  ptr<net_service> service = std::make_shared<net_service>(conf);
  ptr<sock_server> server = std::make_shared<sock_server>(conf, service);

  auto srv_ptr = server.get();

  message_handler handler = [message_processor, srv_ptr,
      &conf](ptr<connection> conn, message_type id,
             byte_buffer &buffer,
             msg_hdr *hdr) -> result<void> {
    try {
      if (id == CLIENT_SYNC_CLOSE) {
        srv_ptr->stop();
        LOG(info) << conf.node_id() << " client receive close";
      } else {
        if (message_processor) {
          auto r = message_processor->process(std::move(conn), id, buffer, hdr);
          if (not r) {
            return r;
          }
        }
      }
      return outcome::success();
    } catch (block_exception &ex) {
      return outcome::failure(ex.error_code());
    }
  };

  // message processing handler
  service->register_handler(handler);
  // start the service and blocks.
  // service->start();
  server->start();

  // this invoke would be blocked when normal processing,
  // when the server stopped, it wait all thread cancel_and_join
  server->join();
  LOG(info) << "block-client service" << id_2_name(conf.node_id())
            << " stopped ";
}

void block_client::run_sync_thread() {
  auto client = shared_from_this();
  ptr<processor> sink =
      cs_new<processor_sink<ptr<block_client>>>(client, MESSAGE_BLOCK_CLI);
  client_sync_run(conf_, sink);
}

void block_client::connect_sync() {
  std::unique_lock l(client_sync_mutex_);

  for (const auto &n : conf_.node_client_list()) {
    if (!client_sync_.contains(n.node_id())) {
      ptr<db_client> client = cs_new<db_client>(conf_.az_id(), n);
      for (;;) {
        bool ok = client->connect();
        if (ok) {
          client_sync_.insert(std::make_pair(n.node_id(), client));
          break;
        }
        sleep(10);
      }
    }
  }
}

void block_client::barrier_wait(std::string name, std::string payload) {
  POSSIBLE_UNUSED(name);
  ptr<barrier_net> b = get_barrier_net(name);
  LOG(trace) << conf_.node_name() << " barrier [" << name << "] begin ";
  for (const auto &p : client_sync_) {
    client_sync msg;
    msg.set_source_id(conf_.node_id());
    msg.set_dest_id(p.first);
    msg.set_name(name);
    if (payload.size() > 0) {
      msg.set_payload(payload);
    }
    auto _ = p.second->send_message(message_type::CLIENT_SYNC, msg);
  }

  b->wait();

  LOG(trace) << conf_.node_name() << " barrier [" << name << "] wait end ";
}

void block_client::bench_stop() {
  for (const auto &p : client_sync_) {
    client_bench_stop msg;
    msg.set_source_id(conf_.node_id());
    msg.set_dest_id(p.first);
    auto _ = p.second->send_message(message_type::CLIENT_BENCH_STOP, msg);
  }
}

void block_client::barrier_done(std::string name, std::string payload, node_id_t node_id) {
  LOG(trace) << conf_.node_name() << "  barrier [" << name << "] end on "
             << node_id;
  ptr<barrier_net> b = get_barrier_net(name);
  b->add_node_id(node_id, payload);
}

result<void> block_client::client_handle_message(const ptr<connection>,
                                                 message_type,
                                                 const ptr<client_sync> msg) {
  barrier_done(msg->name(), msg->payload(), msg->source_id());
  return outcome::success();
}
result<void> block_client::client_handle_message(const ptr<connection>, message_type, const ptr<client_bench_stop>) {
  if (wl_) {
    wl_->set_stopped();
  }
  return outcome::success();
}

void block_client::stop_sync() {
  std::unique_lock l(client_sync_mutex_);
  if (!stopped_.load()) {
    stopped_.store(true);
    for (const auto &p : client_sync_) {
      close_request msg;
      auto _ = p.second->send_message(message_type::CLIENT_SYNC_CLOSE, msg);
    }
  }
}

void block_client::setup_terminal_id() {
  uint32_t num_terminal = conf_.final_num_terminal();
  uint32_t num_client = (uint32_t) conf_.node_client_list().size();
  if (num_client == 0 || num_terminal < num_client) {
    PANIC(boost::format("error clients: %d, terminals: %d") % num_client % num_terminal);
  }
  uint32_t term_per_client = num_terminal / num_client;
  uint32_t found_nodes = 0;
  for (uint32_t i = 0; i < num_client; i++) {
    // terminal id start with 1
    const node_config &n = conf_.node_client_list()[i];
    if (n.node_name() == conf_.node_name()) {
      found_nodes++;
      term_id_begin_ = i * term_per_client + 1;
      term_id_end_ = i * term_per_client + term_per_client;
      if (i + 1 == num_client) { // if the last one
        term_id_end_ = num_terminal;
      }
    }
  }
  if (found_nodes != 1) {
    PANIC("client node id configure error");
  }
  uint32_t client_num_terms = conf_.final_num_terminal();
  LOG(info) << "node " << conf_.node_name() << " num terminals"
            << client_num_terms;
  barrier0_ = cs_new<boost::barrier>(client_num_terms);
  barrier1_ = cs_new<boost::barrier>(client_num_terms);

  barrier_warm1_ = cs_new<boost::barrier>(client_num_terms);
  barrier_warm2_ = cs_new<boost::barrier>(client_num_terms);

  // barrier2 wait run workload && report TPS
  barrier2_ = cs_new<boost::barrier>(client_num_terms + 1);
}



ptr<barrier_net> block_client::get_barrier_net(const std::string &name) {
  std::unique_lock _l(barrier_net_mutex_);
  ptr<barrier_net> b;
  auto i = barrier_net_.find(name);
  if (i != barrier_net_.end()) {
    b = i->second;
  } else {
    b = cs_new<barrier_net>(client_sync_.size());
    barrier_net_.insert(std::make_pair(name, b));
  }
  return b;
}