#include "common/result.hpp"
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <thread>
#include <vector>

using boost::thread_group;
using boost::asio::io_service;
using std::vector;

class ThreadPool {
 private:
  io_service io_service_;
  thread_group thread_group_;
  vector<boost::thread *> service_thread_;

 public:
  ThreadPool() { this->join(); }

  ~ThreadPool() {}

  void run();

  void join();

  io_service &service() { return io_service_; }

 private:
  void process();
};