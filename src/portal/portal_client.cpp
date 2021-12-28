#include "portal/portal_client.h"
#include "common/config.h"
#include "common/ptr.hpp"
#include "common/debug_url.h"
#include "common/json_pretty.h"
#include "common/set_thread_name.h"
#include "network/client.h"
#include "portal/workload.h"
#include "network/debug_server.h"
#include "network/debug_client.h"
#include "common/wait_path.h"
#include "proto/proto.h"
#include <memory>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/regex.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <iostream>

const char *BC_COMMAND_LOAD = "load";
const char *BC_COMMAND_RUN = "benchmark";

using boost::asio::ip::tcp;

class block_client {
private:
  bool close_server_;
  config conf_;
  boost::barrier barrier1_;
  boost::barrier barrier2_;
  std::unordered_map<node_id_t, std::string> response_;
  ptr<debug_server> debug_;
  std::vector<node_config> conf_list_;
public:
  block_client(const config &conf, bool close_server)
      : close_server_(close_server),
        conf_(conf),
        barrier1_(conf.num_terminal()),
      // barrier2 wait run workload && report TPS
        barrier2_(conf.num_terminal() + 1) {
    conf_list_ = conf_.node_config_list();
    conf_list_.push_back(conf_.panel_config());
  }

  void run_command(const std::string &command);

  void close();
private:
  void send_close_request();
  void output_result_thread(ptr<workload> &wl);
  void load_data();
  void run_benchmark();
  void load_data_thread(node_id_t node_id, const ptr<workload> &wl);
  void run_benchmark_thread(shard_id_t rg_id, uint32_t terminal_id, ptr<workload> &wl);
  void handle_debug(const std::string &path, std::ostream &os);
  void debug_deadlock(std::ostream &os);
};

void sort_dependency_array(dependency_set_array &array);

void block_client::run_command(const std::string &command) {

  http_handler debug_handler = [this](const std::string &path,
                                      std::ostream &os
  ) {
    this->handle_debug(path, os);
  };

  std::string address = conf_.this_node_config().address();
  auto port = uint16_t(conf_.this_node_config().port() + 1000);

  debug_ = std::make_shared<debug_server>("0.0.0.0", port, debug_handler);
  debug_->start();
  set_thread_name("bc");
  try {
    if (command == BC_COMMAND_LOAD) {
      load_data();
    } else if (command == BC_COMMAND_RUN) {
      run_benchmark();
    } else if (command.empty()) {
      load_data();
      run_benchmark();
    } else {
      BOOST_LOG_TRIVIAL(error) << "unknown command " << command;
    }
    BOOST_LOG_TRIVIAL(info) << "run command " << command << " done";
  } catch (std::exception &ex) {
    BOOST_LOG_TRIVIAL(error) << "catch exception when run command " << command << " error: " << ex.what();
    close();
  }
}

void block_client::close() {
  if (close_server_) {
    send_close_request();
  }
  if (debug_) {
    debug_->stop();
    debug_->join();
  }
}

void block_client::load_data() {
  std::vector<ptr<boost::thread>> threads;
  ptr<workload> wl(new workload(conf_));
  for (const node_config &nc: conf_.node_config_list()) {
    if (is_dsb_block(nc.node_id())) {
      threads.emplace_back(new boost::thread(
          boost::bind(&block_client::load_data_thread, this, nc.node_id(), wl)));
    }

  }

  for (ptr<boost::thread> &t: threads) {
    t->join();
  }
  BOOST_LOG_TRIVIAL(info) << "block_client load data done";
}

void block_client::run_benchmark() {
  ptr<workload> wl(new workload(conf_));
  std::vector<ptr<boost::thread>> threads;
  uint32_t num_rg_ = conf_.num_rg();
  uint32_t num_terminal = conf_.num_terminal();

  for (const auto &n: conf_.node_config_list()) {
    if (is_ccb_block(n.node_id())) {
      db_client client(n);
      for (;;) {
        bool ok = client.connect();
        if (ok) {
          break;
        }
        sleep(1);
      }
    }
  }
  for (uint32_t i = 0; i < num_terminal; i++) {
    uint32_t terminal_id = i + 1;
    shard_id_t rg_id = i % num_rg_ + 1;
    ptr<boost::thread> t = std::make_shared<boost::thread>(boost::bind(
        &block_client::run_benchmark_thread, this, rg_id, terminal_id, wl));
    threads.push_back(t);
  }
  ptr<boost::thread> t = std::make_shared<boost::thread>(boost::bind(
      &block_client::output_result_thread, this, wl));
  threads.push_back(t);
  for (ptr<boost::thread> &th: threads) {
    th->join();
  }
  BOOST_LOG_TRIVIAL(info) << "block_client benchmark done";
}

void block_client::load_data_thread(node_id_t node_id, const ptr<workload> &wl) {
  std::string thread_name = "load_data";
  set_thread_name(thread_name);
  wl->load_data(node_id);
}

void block_client::run_benchmark_thread(shard_id_t rg_id, uint32_t terminal_id,
                                        ptr<workload> &wl) {
  std::string thread_name = "bt_" + std::to_string(terminal_id);
  set_thread_name(thread_name);

  // wait all thread client connect to the database
  wl->connect_database(rg_id, terminal_id);

  sleep(3);

  wl->gen_new_order(rg_id, terminal_id);

  // wait all thread generate requests
  barrier1_.wait();

  BOOST_LOG_TRIVIAL(info) << "run new-order on RG " << rg_id << " terminal " << terminal_id;

  barrier2_.wait();

  wl->run_new_order(rg_id, terminal_id);
}

void block_client::output_result_thread(ptr<workload> &wl) {
  barrier2_.wait();

  wl->output_result();
}

void block_client::send_close_request() {
  std::set<uint32_t> closed_node;

  for (;;) {
    int not_closed = 0;
    for (const auto &n: conf_list_) {
      if (closed_node.contains(n.node_id())) {
        continue;
      }
      db_client client(n);
      if (client.connect()) {
        close_request req;
        result<void> r = client.send_message(CLOSE_REQ, req);
        if (!r) {
          BOOST_LOG_TRIVIAL(error) << "close request send error " << r.error().message();
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

void block_client::handle_debug(const std::string &path, std::ostream &os) {
  for (const auto &c: conf_list_) {
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
  for (auto &i: response_) {
    dependency_set &ds = *array.add_array();
    //os << i.second << std::endl;
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
    for (xid_t x: circle) {
      os << "->" << x;
    }
    os << std::endl;
  };
  p.detect_circle(fn);
}

int portal_client(int argc, const char *argv[]) {

  boost::program_options::options_description desc(
      "db-block-client Allowed options");

  desc.add_options()("help", "produce help message")(
      "conf", boost::program_options::value<std::string>(),
      "configure file path")
      ("dbtype", boost::program_options::value<std::string>(), "database type")
      ("command", boost::program_options::value<std::string>(), "command")
      ("close-server", boost::program_options::value<std::string>(), "close server, default value is true");

  boost::program_options::variables_map vm;
  try {
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
  } catch (std::exception &ex) {
    BOOST_LOG_TRIVIAL(error) << ex.what();
    std::cout << desc;
    return 1;
  }

  std::string conf_path;
  std::string command;
  bool close_server = true;
  config conf;
  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("conf")) {
    conf_path = vm["conf"].as<std::string>();
  }
  if (vm.count("command")) {
    command = vm["command"].as<std::string>();
  }
  if (vm.count("close-server")) {
    close_server = vm["close-server"].as<bool>();
  }
  if (vm.count("dbtype")) {
    std::string s = vm["dbtype"].as<std::string>();
    BOOST_ASSERT(!s.empty());
    auto type = str2enum<db_type>(s);
    set_block_db_type(type);
  }
  std::ifstream fsm(conf_path);
  std::stringstream ssm;
  ssm << fsm.rdbuf();
  if (not conf.from_json_string(ssm.str())) {
    return 1;
  }

  block_client cli(conf, close_server);
  cli.run_command(command);
  BOOST_LOG_TRIVIAL(info) << "client run command " << command << " done";
  cli.close();
  return 0;
}

void sort_dependency_array(dependency_set_array &array) {
  for (dependency_set &ds: *array.mutable_array()) {
    for (dependency &d: *ds.mutable_dep()) {
      std::sort(d.mutable_out()->begin(), d.mutable_out()->end(), [](uint64_t x, uint64_t y) {
        return x < y;
      });
    }
    std::sort(ds.mutable_dep()->begin(), ds.mutable_dep()->end(),
              [](const dependency &x, const dependency &y) {
                return x.in() < y.in();
              }
    );
  }
}