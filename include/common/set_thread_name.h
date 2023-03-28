#pragma once

#include <cerrno>
#include <string>

inline void set_thread_name(const std::string &thread_name) {
#ifdef __linux__
  int ret = pthread_setname_np(pthread_self(), thread_name.c_str());
  if (ret!=0) {
    perror("set thread name error");
  }
#elif __APPLE__
  pthread_setname_np(thread_name.c_str());
#endif
}

inline std::string get_thread_name() {
  std::string name;
#ifdef __linux__
  char buffer[2048];
  int ret = pthread_getname_np(pthread_self(), buffer, sizeof(buffer));
  if (ret!=0) {
    perror("set thread name error");
  }
  name = std::string(buffer);
#elif __APPLE__
  pthread_getname_np(pthread_self());
#endif
  return name;
}