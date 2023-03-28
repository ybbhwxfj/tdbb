#pragma once

#include <algorithm>
#include <boost/assert.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

// This is the type that will hold all the strings.
// Each enumeration type will declare its own specialization.
// Any enum that does not have a specialization will generate a compiler error
// indicating that there is no definition of this variable (as there should be
// be no definition of a generic version).
template<typename T> struct enum_strings {
  typedef std::unordered_map<T, const char *> e2s_t;
  static e2s_t enum2str;
};

// This is a utility type.
// Created automatically. Should not be used directly.
template<typename T> struct enum_ref_holder {
  T &enum_value;

  enum_ref_holder(T &enumVal) : enum_value(enumVal) {}
};

template<typename T> struct enum_const_ref_holder {
  T const &enum_value;

  enum_const_ref_holder(T const &enumVal) : enum_value(enumVal) {}
};

// This is the public interface:
// use the ability of function to deduce their template type without
// being explicitly told to create the correct type of enumRefHolder<T>
template<typename T> enum_const_ref_holder<T> enum2holder(T const &e) {
  return enum_const_ref_holder<T>(e);
}

template<typename T> enum_ref_holder<T> string2holder(const std::string &s) {

  // These two can be made easier to read in C++11
  // using std::begin() and std::end()
  //
  static auto begin = std::begin(enum_strings<T>::enum2str);
  static auto end = std::end(enum_strings<T>::enum2str);
  for (auto i = begin; i!=end; i++) {
    if (std::string(i->second)==std::string(s)) {
      return enum_ref_holder<T>(const_cast<T &>(i->first));
    }
  }
  BOOST_ASSERT(false);
  return enum_ref_holder<T>(const_cast<T &>(end->first));
}

// The next two functions do the actual work of reading/writing an
// enum as a string.
template<typename T>
std::ostream &operator<<(std::ostream &str,
                         enum_const_ref_holder<T> const &data) {
  return str << enum_strings<T>::enum2str[data.enum_value];
}

template<typename T>
std::istream &operator>>(std::istream &str, enum_ref_holder<T> const &data) {
  std::string value;
  str >> value;
  data = string2holder<T>(value);
  return str;
}

template<typename T> std::string enum2str(T const &e) {
  auto i = enum_strings<T>::enum2str.find(e);
  if (i==enum_strings<T>::enum2str.end()) {
    // BOOST_ASSERT(false);
    return std::string();
  }
  return i->second;
}

template<typename T> T str2enum(const std::string &str) {
  return string2holder<T>(str).enum_value;
}
