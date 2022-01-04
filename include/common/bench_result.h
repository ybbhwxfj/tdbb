#pragma once

#include <stdint.h>
#include <boost/json.hpp>
#include <string>

class bench_result {
private:
  float tpm_;
  float abort_;
  float latency_;
  float latency_read_{};
  float latency_append_{};
  float latency_replicate_{};
  float latency_lock_wait_{};
  float latency_part_{};
public:
  bench_result() : tpm_(0), abort_(0), latency_(0) {}
  void set_tpm(float tpm) { tpm_ = tpm; }
  void set_abort(float abort) { abort_ = abort; }
  void set_latency(float latency) { latency_ = latency; }

  void set_latency_read(float v) { latency_read_ = v; }
  void set_latency_append(float v) { latency_append_ = v; }
  void set_latency_replicate(float v) { latency_replicate_ = v; }
  void set_latency_lock_wait(float v) { latency_lock_wait_ = v; }
  void set_latency_part(float v) { latency_part_ = v; }

  boost::json::object to_json() const {
    boost::json::object obj;
    obj["tpm"] = tpm_;
    obj["abort"] = abort_;
    obj["latency"] = latency_;
    obj["latency_read"] = latency_read_;
    obj["latency_append"] = latency_append_;
    obj["latency_replicate"] = latency_replicate_;
    obj["latency_lock_wait"] = latency_lock_wait_;
    obj["latency_part"] = latency_part_;
    return obj;
  }
};
