#pragma once

class log_write_option {
 private:
  bool force_;
 public:
  log_write_option() : force_(true) {}

  void set_force(bool force) {
    force_ = force;
  }

  bool force() const {
    return force_;
  }
};
