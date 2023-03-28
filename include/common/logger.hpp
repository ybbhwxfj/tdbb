#pragma once

#include "common/unused.h"
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>

//#define LOG_FILE_LINE
#ifdef LOG_FILE_LINE
#define LOG(level)                                                             \
  BOOST_LOG_TRIVIAL(level) << "(" << __FILE__ << ", " << __LINE__ << ") "
#else
#define LOG(level) BOOST_LOG_TRIVIAL(level)
#endif

inline void SETUP_LOG(std::string file_name) {
  POSSIBLE_UNUSED(file_name);
  static std::atomic_bool setup = false;
  if (!setup.load()) {
    setup.store(true);
  } else {
    return;
  }

  //boost::log::add_file_log(file_name);
  //boost::log::add_console_log(std::cout);
  boost::log::core::get()->set_filter(boost::log::trivial::severity >=
      boost::log::trivial::info);
}
