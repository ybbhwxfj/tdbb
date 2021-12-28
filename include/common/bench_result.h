#pragma once

#include <stdint.h>
#include <boost/json.hpp>
#include <string>

class bench_result {
private:
  float tpm_;
  float abort_;
public:
  bench_result() : tpm_(0), abort_(0) {}
  void set_tpm(float tpm) { tpm_ = tpm; }
  void set_abort(float abort) { abort_ = abort; }

  boost::json::object to_json() const {
    boost::json::object obj;
    obj["tpm"] = tpm_;
    obj["abort"] = abort_;
    return obj;
  }
};
