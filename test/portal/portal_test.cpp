#define BOOST_TEST_MODULE PORTAL_TEST
#include "common/callback.h"
#include "common/db_type.h"
#include "common/gen_config.h"
#include "common/ptr.hpp"
#include "history.h"
#include "portal/portal.h"
#include "portal/portal_client.h"
#include <boost/format.hpp>
#include <boost/program_options.hpp>

#include "common/logger.hpp"
#include <boost/move/utility.hpp>
#include <boost/test/unit_test.hpp>
#include <thread>

#define XSTR(x) STR(x)
#define STR(x) #x
//#pragma message "build type value : " XSTR(DB_BUILD_TYPE)

#define DEFAULT_DB_TYPE DB_SN
#define MAX_ARGUMENTS 1024

class argument {
public:
  argument() : c_(0) {}
  argument(int c, const char **v) : c_(0) { add(c, v); }

  void add(int c, const char **v) {
    BOOST_ASSERT(size_t(c + c_) < MAX_ARGUMENTS);
    for (int i = 0; i < c; i++) {
      std::string s(v[i]);
      size_t n = s.find("--test");
      if (n != std::string::npos) {
        i += 1;
      } else {
        v_.push_back(s);
        c_++;
      }
    }
  }

  void add(const char *a) {
    std::string s(a);
    v_.push_back(s);
    c_ += 1;
  }

  const char **argv() const {
    for (int i = 0; i < c_; i++) {
      if (v_.size() < MAX_ARGUMENTS) {
        const_cast<const char *&>(argv_[i]) = v_[i].c_str();
      }
    }
    return (const char **) (argv_);
  }
  int argc() const { return c_; }

  void remove(const std::string &name) {
    int remove_begin = -1;
    int remove_end = -1;
    for (int i = 0; i < c_; i++) {
      std::string s = v_[i];
      if (s == std::string("--") + name ||
          s == std::string("-") + name
          ) {
        remove_begin = i;
        remove_end = i + 1;
        if (i + 1 < c_) {
          remove_end += 1;
        }
      } else if (s == name) {
        remove_begin = i;
        remove_end = i + 1;
      }
    }
    if (remove_begin >= 0) {
      int to_removed = remove_end - remove_begin;
      memmove(argv_ + remove_begin, argv_ + remove_end, to_removed);
      c_ -= to_removed;
    }
  }

private:
  std::vector<std::string> v_;
  int c_;
  const char *argv_[MAX_ARGUMENTS];
};

class argv_list {
private:
  std::vector<ptr<argument>> args_;

public:
  argv_list(uint32_t size) {
    for (size_t i = 0; i < size; i++) {
      ptr<argument> a(new argument());
      args_.push_back(a);
    }
  }

  argv_list(const std::vector<std::string> &conf) {
    size_t size = conf.size();
    for (size_t i = 0; i < size; i++) {
      ptr<argument> a(new argument());
      a->add("bdb");
      a->add("--conf");
      a->add(conf[i].c_str());
      args_.push_back(a);
    }
  }

  ~argv_list() {}

  void add_argv(const char *arg) {
    for (size_t i = 0; i < args_.size(); ++i) {
      args_[i]->add(arg);
    }
  }

  void add_argv(uint32_t i, const char *arg) {
    args_[i]->add(arg);
  }

  void add_argv(int argc, const char **argv) {
    for (size_t i = 0; i < args_.size(); ++i) {
      args_[i]->add(argc, argv);
    }
  }

  const ptr<argument> &get_argv(size_t i) const {
    BOOST_ASSERT(i < args_.size());
    return args_[i];
  }

  void remove(const std::string &arg) {
    for (size_t i = 0; i < args_.size(); ++i) {
      args_[i]->remove(arg);
    }
  }
  size_t size() const { return args_.size(); }
};

void test_run_command(const std::string &command, int argc, const char **argv) {
  boost::program_options::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")(
      "dbtype", boost::program_options::value<std::string>(),
      "database type")("loose-bind", boost::program_options::value<bool>(), "bind")(
      "test-shard", boost::program_options::value<int>(),
      "shard")
      (
          "num-client", boost::program_options::value<int>(),
          "clients")
      ("register-callback", boost::program_options::value<bool>(),
       "register before/after schedule callback");

  boost::program_options::variables_map vm;
  vm.erase("num-client");

  history h;
  bool register_callback = true;
  db_type type = DEFAULT_DB_TYPE;
  bool loose_bind = false;
  int num_shard = NUM_RG > 1 ? NUM_RG : 2;
  int num_client = 1;
  try {
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
    if (vm.contains("register-callback")) {
      register_callback = vm["register-callback"].as<bool>();
    }
    if (vm.count("dbtype")) {
      std::string s = vm["dbtype"].as<std::string>();
      type = str2enum<db_type>(s);
    }
    if (vm.contains("loose-bind")) {
      loose_bind = not vm["loose-bind"].as<bool>();
    }
    if (vm.contains("test-shard")) {
      num_shard = vm["test-shard"].as<int>();
    }
    if (vm.contains("num-client")) {
      num_client = vm["num-client"].as<int>();
    }
  } catch (std::exception &ex) {
    return;
  }

  if (register_callback) {
    callback fn;
    fn.schedule_after_ = [&h](const tx_op &op) { h.add_op(op); };
    set_global_callback(fn);
  }
  set_block_db_type(type);

  config_option opt;
  if (block_db_type() == DB_S) {
    opt.set_config_share(loose_bind);
  } else if (block_db_type() == DB_D || block_db_type() == DB_SN) {
    opt.set_config_share_nothing(num_shard);
  } else if (block_db_type() == DB_SCR) {
    opt.set_config_scale_dsb(1, num_shard);
  } else {
    PANIC("error db type");
  }

  opt.num_az = NUM_AZ;
  opt.priority = true;
  opt.num_client = num_client;
  std::pair<std::vector<std::string>, std::vector<std::string>> conf_list = generate_config_json_file(opt);

  argv_list server_argv(conf_list.second); // servers
  server_argv.add_argv(argc, argv);
  server_argv.remove("num-client");
  boost::posix_time::ptime::time_type now =
      boost::posix_time::second_clock::local_time();
  std::stringstream ssm;
  ssm << TMP_DB_PATH << "/block_" << now << ".log";
  SETUP_LOG(ssm.str());

  std::vector<ptr<std::thread>> thd;
  for (size_t i = 0; i < server_argv.size(); i++) {
    auto v = server_argv.get_argv(i);
    std::thread *t = new std::thread(portal, v->argc(), v->argv());
    thd.push_back(ptr<std::thread>(t));
  }

  // start client
  argv_list client_argv(conf_list.first.size()); // servers
  for (size_t i = 0; i < conf_list.first.size(); i++) {
    std::string conf = conf_list.first[i];
    client_argv.add_argv(i, "bdb");
    client_argv.add_argv(i, "--conf");
    client_argv.add_argv(i, conf.c_str());
    client_argv.add_argv(i, "--command");
    client_argv.add_argv(i, command.c_str());
  }

  for (size_t i = 0; i < client_argv.size(); i++) {
    auto v = client_argv.get_argv(i);
    auto t = new std::thread(portal_client, v->argc(), v->argv());
    thd.push_back(ptr<std::thread>(t));
  }
  for (auto t : thd) {
    t->join();
  }
  BOOST_CHECK(h.is_serializable());
}

/**
 * Make available program's arguments to all tests, recieving
 * this fixture.
 */
struct ArgsFixture {
  ArgsFixture()
      : argc(boost::unit_test::framework::master_test_suite().argc),
        argv((const char **) boost::unit_test::framework::master_test_suite()
            .argv) {}
  int argc;
  const char **argv;
};

BOOST_FIXTURE_TEST_CASE(portal_test_load, ArgsFixture) {
  boost::posix_time::ptime t =
      boost::posix_time::second_clock::universal_time();
  global_test_db_path =
      TMP_DB_PATH + '_' + boost::posix_time::to_simple_string(t);

  boost::filesystem::path p(global_test_db_path);
  p.append("load.data.lock");
  if (!boost::filesystem::exists(global_test_db_path)) {
    boost::filesystem::create_directories(global_test_db_path);
  }
  test_run_command("load", argc, argv);
  std::cout << " create load.data.lock file" << std::endl;
  std::ofstream f;
  f.open(p);
  if (boost::filesystem::exists(p)) {
    std::cout << " no load.data.lock file" << std::endl;
    return;
  } else {
  }
}

BOOST_FIXTURE_TEST_CASE(portal_test_benchmark, ArgsFixture) {
  test_run_command("benchmark", argc, argv);
}