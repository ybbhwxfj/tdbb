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
  float latency_read_dsb_{};
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

  void set_latency_read_dsb(float v) { latency_read_dsb_ = v; }

  void set_latency_append(float v) { latency_append_ = v; }

  void set_latency_replicate(float v) { latency_replicate_ = v; }

  void set_latency_lock_wait(float v) { latency_lock_wait_ = v; }

  void set_latency_part(float v) { latency_part_ = v; }

  boost::json::object to_json() const {
    boost::json::object obj;
    obj["tpm"] = tpm_;
    obj["abort"] = abort_;
    obj["lt"] = latency_;
    obj["lt_read"] = latency_read_;
    obj["lt_read_dsb"] = latency_read_dsb_;
    obj["lt_app"] = latency_append_;
    obj["lt_app_rlb"] = latency_replicate_;
    obj["lt_lock"] = latency_lock_wait_;
    obj["lt_part"] = latency_part_;
    return obj;
  }

  uint64_t append_latency() const {
    return latency_append_;
  }

  uint64_t replicate_latency() const {
    return latency_replicate_;
  }
};
