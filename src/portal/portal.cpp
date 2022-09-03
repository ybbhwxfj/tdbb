#include "portal/portal.h"
#include "common/define.h"
#include "common/block_exception.h"
#include "common/config.h"
#include "concurrency/cc_block.h"
#include "network/net_service.h"
#include "network/sock_server.h"
#include "replog/rl_block.h"
#include "store/ds_block.h"
#include "network/message_processor.h"
#include "panel/pn_block.h"
#include "network/debug_server.h"
#include <memory>
#include <boost/program_options.hpp>
#include <iostream>
#include <utility>
#include <boost/regex.hpp>
#include "common/debug_url.h"
void block_run(const config &conf, const callback &fn);

bool print_message = false;

int portal(int argc, const char *argv[]) {
  boost::program_options::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")
      ("conf", boost::program_options::value<std::string>(), "configure file path")
      ("dbtype", boost::program_options::value<std::string>(), "database type")
      ("bind", boost::program_options::value<std::string>(), "binding");
  boost::program_options::variables_map vm;
  try {
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
  } catch (std::exception &ex) {
    BOOST_LOG_TRIVIAL(error) << "exception: " << ex.what();
    exit(-1);
  }

  std::string conf_path;
  config conf;
  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
  }
  for (int i = 0; i < argc; i++) {
    std::cout << argv[i] << " ";
  }
  std::cout << std::endl;
  if (vm.count("conf")) {
    conf_path = vm["conf"].as<std::string>();
    BOOST_ASSERT(!conf_path.empty());
  } else {
    BOOST_ASSERT(false);
    return false;
  }

  if (vm.count("dbtype")) {
    std::string s = vm["dbtype"].as<std::string>();
    BOOST_ASSERT(!s.empty());
    auto type = str2enum<db_type>(s);
    set_block_db_type(type);
  }

  BOOST_LOG_TRIVIAL(info) << "run block-db " << enum2str(block_db_type());

  std::ifstream fsm(conf_path);
  std::stringstream ssm;
  ssm << fsm.rdbuf();

  if (not conf.from_json_string(ssm.str())) {
    return 1;
  }
  block_run(conf, get_global_callback());
  return 0;
}

void block_run(const config &conf, const callback &fn) {
  std::string n = block_type_list_2_string(conf.this_node_config().block_type_list());
  std::string name =
      "BE_" + n + std::to_string(conf.rg_id()) + std::to_string(conf.az_id());
  set_thread_name(name);

  BOOST_ASSERT(!conf.node_name().empty());
  BOOST_ASSERT(conf.this_node_config().port() != 0);

  // all these stuff(net_service, sock_server, blocks) has program(this
  // function) life-time, we keep it in smart pointer

  ptr<net_service> service = std::make_shared<net_service>(conf);
  ptr<sock_server> server =
      std::make_shared<sock_server>(conf, service);

  std::vector<ptr<processor>> processors;
  processors.resize(MESSAGE_BLOCK_END);
  std::vector<ptr<block>> blocks;

  for (const block_type_t &t : conf.this_node_config().block_type_list()) {
    if (t == BLOCK_CCB) {

      ptr<cc_block> ccb = std::make_shared<cc_block>(
          conf,
          service.get(),
          fn.schedule_before_,
          fn.schedule_after_);
      ptr<processor_sink<ptr<cc_block>>> s1(new processor_sink<ptr<cc_block>>(ccb, MESSAGE_BLOCK_CCB));
      blocks.push_back(ccb);
      processors[s1->message_block_type()] = s1;
    } else if (t == BLOCK_DSB) {
      ptr<ds_block> dsb = std::make_shared<ds_block>(
          conf, service.get()
      );
      ptr<processor_sink<ptr<ds_block>>> s_dsb(new processor_sink<ptr<ds_block>>(dsb, MESSAGE_BLOCK_DSB));
      processors[s_dsb->message_block_type()] = s_dsb;
      blocks.push_back(dsb);
    } else if (t == BLOCK_RLB) {
      ptr<rl_block> rlb = std::make_shared<rl_block>(conf, service,
                                                     fn.become_leader_,
                                                     fn.become_follower_,
                                                     fn.commit_entries_
      );
      ptr<processor_sink<ptr<rl_block>>> s_dsb(new processor_sink<ptr<rl_block>>(rlb, MESSAGE_BLOCK_RLB));
      processors[s_dsb->message_block_type()] = s_dsb;

      blocks.push_back(rlb);
    } else if (t == BLOCK_PNB) {
      ptr<pn_block> pnb = std::make_shared<pn_block>(conf, service);
      ptr<processor_sink<ptr<pn_block>>> s_pnb(new processor_sink<ptr<pn_block>>(pnb, MESSAGE_BLOCK_PNB));
      processors[s_pnb->message_block_type()] = s_pnb;
      blocks.push_back(pnb);
    }
  }

  for (const auto &b : blocks) {
    service->register_block(b.get());
  }
  BOOST_ASSERT(!blocks.empty());
  auto srv_ptr = server.get();

  //struct count_msg {
  //  std::mutex mtx_;
  //  boost::posix_time::ptime start_ts_;
  //  std::unordered_map<message_type, uint32_t> map_;
  //};
  //ptr<count_msg> cm(new count_msg());
  //cm->start_ts_ = std::chrone::stead_lock::now();
  processor_sink<> s;
  message_handler handler = [s, processors, srv_ptr,
      &conf](ptr<connection> conn, message_type id,
             byte_buffer &buffer, msg_hdr *hdr) -> result<void> {

    try {
#ifdef TEST_NETWORK_TIME
      auto b = std::chrono::steady_clock::now();
#endif //TEST_NETWORK_TIME

      message_block bt = s.get_message_block_type(id);
      auto p = processors[bt];
      if (id == CLOSE_REQ) {
        srv_ptr->stop();
        BOOST_LOG_TRIVIAL(info) << id_2_name(conf.node_id()) << " receive close";
      } else {
        if (p) {
          auto r = p->process(std::move(conn), id, buffer, hdr);
          if (not r) {
            return r;
          }
        }
      }
#ifdef TEST_NETWORK_TIME
      auto e = std::chrono::steady_clock::now();
      uint64_t ms = to_milliseconds(e - b);
      if (ms > 50) {
        BOOST_LOG_TRIVIAL(info) << " process message " << ms << "ms, " << enum2holder(id);
      }
      if (print_message) {
        //BOOST_LOG_TRIVIAL(info) << " process message " << enum2holder(id);
      }
#endif // #ifdef TEST_NETWORK_TIME
      return outcome::success();
    } catch (block_exception &ex) {
      return outcome::failure(ex.error_code());
    }
  };

  proto_message_handler local_handler = [s, processors, srv_ptr,
      &conf](ptr<connection> conn, message_type id, ptr<google::protobuf::Message> msg) -> result<void> {

    try {
#ifdef TEST_NETWORK_TIME
      auto b = std::chrono::steady_clock::now();
#endif //TEST_NETWORK_TIME
      message_block bt = s.get_message_block_type(id);
      auto p = processors[bt];
      if (id == CLOSE_REQ) {
        srv_ptr->stop();
        BOOST_LOG_TRIVIAL(info) << id_2_name(conf.node_id()) << " receive close";
      } else {
        if (p) {
          auto r = p->process_msg(std::move(conn), id, msg);
          if (not r) {
            return r;
          }
        }
      }
#ifdef TEST_NETWORK_TIME
      auto e = std::chrono::steady_clock::now();
      uint64_t ms = to_milliseconds(e - b);
      if (ms > 50) {
        BOOST_LOG_TRIVIAL(info) << " process message " << ms << "ms, " << enum2holder(id);
      }
      if (print_message) {
        //BOOST_LOG_TRIVIAL(info) << " process message " << enum2holder(id);
      }
#endif // #ifdef TEST_NETWORK_TIME
      return outcome::success();
    } catch (block_exception &ex) {
      return outcome::failure(ex.error_code());
    }
  };

  service->register_local_handler(local_handler);
  // message processing handler
  service->register_handler(handler);

  http_handler debug_handler = [blocks](const std::string &path,
                                        std::ostream &os
  ) {
    for (const auto &b : blocks) {
      b->handle_debug(path, os);
    }
  };
  ptr<debug_server> debug(new debug_server(
      "0.0.0.0",
      uint16_t(conf.this_node_config().port() + 1000),
      debug_handler
  ));

  // start the service and blocks.
  // service->start();
  server->start();
  debug->start();

  // this invoke would be blocked when normal processing,
  // when the server stopped, it wait all thread cancel_and_join
  server->join();
  BOOST_LOG_TRIVIAL(info) << "block-db " << id_2_name(conf.node_id()) << " stopped ";
  debug->stop();
  debug->join();
  sleep(3);
}