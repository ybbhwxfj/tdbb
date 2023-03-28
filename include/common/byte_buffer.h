
#pragma once

#include "common/logger.hpp"
#include "common/message.h"
#include "common/ptr.hpp"
#include "common/result.hpp"
#include "common/variable.h"
#include <array>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/asio.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/exception/exception.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>

// valid buffer contains, [read_pos , write_pos),  write_pos <= size
class byte_buffer {
public:
  byte_buffer() : data_(MESSAGE_BUFFER_SIZE), write_pos_(0), read_pos_(0) {}

  byte_buffer(const byte_buffer &buffer)
      : data_(buffer.data_), write_pos_(buffer.write_pos_),
        read_pos_(buffer.read_pos_) {}

  explicit byte_buffer(size_t n) : data_(n), write_pos_(0), read_pos_(0) {}

  ~byte_buffer() = default;

  size_t capacity() const { return data_.capacity(); }

  size_t size() const { return data_.size(); }

  const int8_t *data() const { return data_.data(); }

  int8_t *data() { return data_.data(); }

  int8_t *write_begin() {
    BOOST_ASSERT(data_.size() >= write_pos_);
    return data_.data() + write_pos_;
  }

  int8_t *read_begin() {
    BOOST_ASSERT(data_.size() >= read_pos_);
    return data_.data() + read_pos_;
  }

  const int8_t *write_begin() const {
    BOOST_ASSERT(data_.size() > write_pos_);
    return data_.data() + write_pos_;
  }

  const int8_t *read_begin() const {
    BOOST_ASSERT(data_.size() > read_pos_);
    return data_.data() + read_pos_;
  }

  void resize_capacity(size_t size) {
    assert(write_pos_ <= size && read_pos_ <= size);
    return data_.resize(size);
  }

  void resize_write_available_size(size_t new_size) {
    uint64_t n = write_available_size();
    if (new_size > n) {
      data_.resize(new_size + write_pos_);
    }
  }

  void reset() {
    read_pos_ = 0;
    write_pos_ = 0;
  }

  void clear() {
    write_pos_ = 0;
    read_pos_ = 0;
  }

  void append(const byte_buffer &src) {
    if (data_.size() < write_pos_ + src.read_available_size()) {
      data_.resize(src.write_pos_ + src.read_available_size());
    }
    memcpy(write_begin(), src.read_begin(), src.read_available_size());
    BOOST_ASSERT(data_.size() >= write_pos_ + src.read_available_size());
    write_pos_ = write_pos_ + src.write_pos_;
  }

  result<void> write(const void *ptr, size_t size) {
    if (data_.size() < write_pos_ + size) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }
    memcpy(this->data() + write_pos_, ptr, size);
    BOOST_ASSERT(data_.size() >= write_pos_ + size);
    write_pos_ = write_pos_ + size;
    return outcome::success();
  }

  result<void> read(void *ptr, size_t size) {
    if (write_pos_ < read_pos_ + size) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }
    memcpy(ptr, this->data() + read_pos_, size);
    read_pos_ = read_pos_ + size;

    return outcome::success();
  }

  void set_read_pos(uint64_t r) {
    assert(size() >= r);
    assert(r <= write_pos_);
    read_pos_ = r;
  }

  uint64_t get_read_pos() const { return read_pos_; }

  void set_write_pos(uint64_t w) {
    assert(size() >= w);
    assert(w >= read_pos_);
    write_pos_ = w;
  }

  uint64_t read_available_size() const { return write_pos_ - read_pos_; }

  uint64_t write_available_size() const { return data_.size() - write_pos_; }

  uint64_t get_write_pos() const { return write_pos_; }

  std::string info_str() const {
    std::stringstream ssm;
    ssm << " wpos:" << write_pos_ << " rpos:" << read_pos_
        << " cap:" << data_.size() << ", ";
    return ssm.str();
  }

private:
  std::vector<int8_t> data_;
  uint64_t write_pos_;
  mutable uint64_t read_pos_;
};
