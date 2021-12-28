#pragma once

#include <string>
#include <cerrno>
inline void set_thread_name(const std::string &thread_name) {
#ifdef __linux__
  int ret = pthread_setname_np(pthread_self(), thread_name.c_str());
  if (ret != 0) {
    perror("set thread name error");
  }
#elif __APPLE__
  pthread_setname_np(thread_name.c_str());
#endif
}