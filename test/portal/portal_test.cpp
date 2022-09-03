#define BOOST_TEST_MODULE PORTAL_TEST
#include "common/db_type.h"
#include "common/gen_config.h"
#include "common/ptr.hpp"
#include "common/callback.h"
#include "history.h"
#include "portal/portal.h"
#include "portal/portal_client.h"
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/move/utility.hpp>
#include <boost/test/unit_test.hpp>
#include <thread>

#define XSTR(x) STR(x)
#define STR(x) #x
//#pragma message "build type value : " XSTR(DB_BUILD_TYPE)

#define DEFAULT_DB_TYPE  DB_SN
#define MAX_ARGUMENTS 1024

class argument {
 public:
  argument() : c_(0) {

  }
  argument(int c, const char **v) : c_(0) {
    add(c, v);
  }

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
  int argc() const {
    return c_;
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

  ~argv_list() {

  }
  void add_argv(const char *arg) {
    for (size_t i = 0; i < args_.size(); ++i) {
      args_[i]->add(arg);
    }
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

  size_t size() const {
    return args_.size();
  }
};

void test_run_command(const std::string &command, int argc, const char **argv) {
  boost::program_options::options_description desc("Allowed options");
  desc.add_options()("help", "produce help message")
      ("dbtype", boost::program_options::value<std::string>(), "database type")
      ("bind", boost::program_options::value<bool>(), "bind")
      ("test-shard", boost::program_options::value<int>(), "shard");
  boost::program_options::variables_map vm;

  callback fn;
  history h;
  fn.schedule_after_ = [&h](const tx_op &op) {
    h.add_op(op);
  };
  set_global_callback(fn);

  db_type type = DEFAULT_DB_TYPE;
  bool unbind = false;
  int num_shards = NUM_RG > 1 ? NUM_RG : 2;

  try {
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
    if (vm.count("dbtype")) {
      std::string s = vm["dbtype"].as<std::string>();
      type = str2enum<db_type>(s);
    }
    if (vm.contains("bind")) {
      unbind = not vm["bind"].as<bool>();
    }
    if (vm.contains("test-shard")) {
      num_shards = vm["test-shard"].as<int>();
    }
  } catch (std::exception &ex) {
    return;
  }

  set_block_db_type(type);

  if (is_shared()) {
    num_shards = 1;
  }
  config_option opt;
  opt.num_az = NUM_AZ;
  opt.num_shard = num_shards;
  opt.loose_bind = unbind;
  std::vector<std::string> conf_list = generate_config_json_file(opt);
  std::string client_conf_file = *conf_list.rbegin();
  conf_list.pop_back();
  argv_list argvs(conf_list);
  argvs.add_argv(argc, argv);

  // boost::log::add_file_log("/tmp/test_db/block.log");
  boost::log::core::get()->set_filter(
      boost::log::trivial::severity >=
          boost::log::trivial::info);

  std::vector<ptr<std::thread>> thd;
  for (size_t i = 0; i < argvs.size(); i++) {
    auto v = argvs.get_argv(i);
    std::thread *t = new std::thread(portal, v->argc(), v->argv());
    thd.push_back(ptr<std::thread>(t));
  }

  // start client
  const char *argv_client[] = {"bdb",
                               "--conf", client_conf_file.c_str(),
                               "--command", command.c_str()};
  thd.emplace_back(
      ptr<std::thread>(new std::thread(portal_client, 5, argv_client)));

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
  ArgsFixture() : argc(boost::unit_test::framework::master_test_suite().argc),
                  argv((const char **) boost::unit_test::framework::master_test_suite().argv) {}
  int argc;
  const char **argv;
};

BOOST_FIXTURE_TEST_CASE(portal_test_load, ArgsFixture) {
  boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time();
  global_test_db_path = TMP_DB_PATH + '_' + boost::posix_time::to_simple_string(t);

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