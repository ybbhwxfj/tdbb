
#pragma once

#include "common/result.hpp"
#include "common/message.h"
#include "common/ptr.hpp"
#include <array>
#include <boost/asio.hpp>
#include <boost/endian/conversion.hpp>
#include <memory>
#include <string>
#include <boost/log/trivial.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <iostream>
#include <ostream>

#include <boost/exception/exception.hpp>
#include "common/variable.h"

// valid buffer contains, [read_pos , write_pos),  write_pos <= size
class byte_buffer {
public:
  byte_buffer()
      : wpos_(0), rpos_(0) { data_.resize(MESSAGE_BUFFER_SIZE); }

  explicit byte_buffer(size_t n)
      : wpos_(0), rpos_(0) { data_.resize(n); }
  ~byte_buffer() = default;

  size_t capacity() const {
    return data_.capacity();
  }

  size_t size() const {
    return data_.size();
  }

  const int8_t *data() const {
    return data_.data();
  }

  int8_t *data() {
    return data_.data();
  }

  int8_t *wbegin() {
    BOOST_ASSERT(data_.size() >= wpos_);
    return data_.data() + wpos_;
  }

  int8_t *rbegin() {
    BOOST_ASSERT(data_.size() >= rpos_);
    return data_.data() + rpos_;
  }

  const int8_t *wbegin() const {
    BOOST_ASSERT(data_.size() > wpos_);
    return data_.data() + wpos_;
  }

  const int8_t *rbegin() const {
    BOOST_ASSERT(data_.size() > rpos_);
    return data_.data() + rpos_;
  }

  void resize(size_t size) {
    assert(wpos_ <= size && rpos_ <= size);
    return data_.resize(size);
  }

  void clear() {
    wpos_ = 0;
    rpos_ = 0;
  }

  void append(const byte_buffer &src) {
    if (data_.size() < wpos_ + src.get_rsize()) {
      data_.resize(src.wpos_ + src.get_rsize());
    }
    memcpy(wbegin(), src.rbegin(), src.get_rsize());
    BOOST_ASSERT(data_.size() >= wpos_ + src.get_rsize());
    wpos_ = wpos_ + src.wpos_;
  }

  result<void> write(const void *ptr, size_t size) {
    if (data_.size() < wpos_ + size) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }
    memcpy(this->data() + wpos_, ptr, size);
    BOOST_ASSERT(data_.size() >= wpos_ + size);
    wpos_ = wpos_ + size;
    return outcome::success();
  }

  result<void> read(void *ptr, size_t size) {
    if (wpos_ < rpos_ + size) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }
    memcpy(ptr, this->data() + rpos_, size);
    rpos_ = rpos_ + size;

    return outcome::success();
  }

  void set_rpos(uint64_t r) {
    assert(size() >= r);
    assert(r <= wpos_);
    rpos_ = r;
  }

  void reset() {
    rpos_ = 0;
    wpos_ = 0;
  }

  uint64_t get_rpos() const {
    return rpos_;
  }

  void set_wpos(uint64_t w) {
    assert(size() >= w);
    assert(w >= rpos_);
    wpos_ = w;
  }

  uint64_t get_rsize() const { return wpos_ - rpos_; }

  uint64_t get_wsize() const { return data_.size() - wpos_; }

  uint64_t get_wpos() const { return wpos_; }

private:
  std::vector<int8_t> data_;
  uint64_t wpos_;
  mutable uint64_t rpos_;
};
