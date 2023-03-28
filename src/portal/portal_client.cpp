#include "portal/portal_client.h"
#include "portal/block_client.h"
#include "common/config.h"
#include "common/panic.h"
#include "common/json_pretty.h"
#include "common/set_thread_name.h"
#include <boost/program_options.hpp>
#include <boost/regex.hpp>
#include <iostream>
#include <unordered_map>

int portal_client(int argc, const char *argv[]) {

  boost::program_options::options_description desc(
      "db-block-client Allowed options");

  desc.add_options()
      ("help", "produce help message")
      ("conf", boost::program_options::value<std::string>(), "configure file path")
      ("dbtype", boost::program_options::value<std::string>(), "database type")
      ("command", boost::program_options::value<std::string>(), "command")
      ("close-server", boost::program_options::value<std::string>(),
       "close server, default value is true");

  boost::program_options::variables_map vm;
  try {
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
  } catch (std::exception &ex) {
    LOG(error) << ex.what();
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

  ptr<block_client> cli = cs_new<block_client>(conf, close_server);
  LOG(info) << "run block-db client ";
  LOG(info) << "  node name " << conf.node_name();
  LOG(info) << "  DB type: " << enum2str(block_db_type());
  LOG(info) << "  terminal: " << conf.num_terminal() << ", final:" << conf.final_num_terminal();
  LOG(info) << "  warehouse: " << conf.get_tpcc_config().num_warehouse();
  LOG(info) << "  access remote wh tx_rm: "
            << conf.get_tpcc_config().percent_remote() * 100.0 << "%";

  cli->run_command(command);
  LOG(info) << "client run command " << command << " done";
  cli->close();
  return 0;
}
