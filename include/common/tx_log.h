#pragma once

#include "common/endian.h"
#include "common/ptr.hpp"
#include "proto/proto.h"
#include "common/panic.h"

typedef tx_log tx_log_proto;
typedef std::string tx_log_binary;
typedef std::string repeated_tx_logs;

class log_buffer;

const uint64_t OFFSET_TIMESTAMP = sizeof(uint64_t)*1;
const uint64_t OFFSET_NODE_ID = sizeof(uint64_t)*2;
const uint64_t OFFSET_PAYLOAD_SIZE = 0;
const uint64_t HEADER_SIZE = 3*sizeof(uint64_t);

typedef std::function<void(log_buffer &)> fn_handle_log_buffer;
typedef std::function<void(const void *buf, size_t length)>
    fn_handle_log_binary;
typedef std::function<void(const tx_log_proto &)> fn_handle_log_proto;
typedef std::function<void(ptr<tx_log_proto>)> fn_handle_log_proto_ptr;
typedef std::function<void(ptr<tx_log_proto>, uint64_t, node_id_t)>
    fn_handle_log_proto_ptr_and_timstamp;

class log_buffer {
private:
  char *pointer_;
  size_t size_;

public:
  inline static void format(char *buffer_pointer, size_t capacity, char *src,
                            size_t len, uint64_t timestamp, node_id_t node_id) {
    log_buffer buffer(buffer_pointer, capacity);
    buffer.set_payload_size(len);
    buffer.set_timestamp(timestamp);
    buffer.set_node_id(node_id);
    buffer.copy_payload(src, len);
  }

  log_buffer(char *pointer, size_t size) : pointer_(pointer), size_(size) {}

  inline char *data() { return pointer_; }

  inline const char *data() const { return pointer_; }

  inline uint64_t size() const { return size_; }

  inline char *get_data(uint64_t offset) {
    BOOST_ASSERT(size() >= offset + sizeof(uint64_t));
    return const_cast<char *>(data()) + offset;
  }

  inline const char *get_data(uint64_t offset) const {
    BOOST_ASSERT(size() >= offset + sizeof(uint64_t));
    return data() + offset;
  }
  static inline uint64_t add_header_size(uint64_t size) {
    return header_size() + size;
  }
  static inline uint64_t header_size() { return HEADER_SIZE; }

  inline uint64_t timestamp() const {
    uint64_buf_t u64_buf;
    memcpy(&u64_buf, get_data(OFFSET_TIMESTAMP), sizeof(uint64_t));
    return u64_buf.value();
  }

  inline uint64_t payload_size() const {
    uint64_buf_t u64_buf;
    memcpy(&u64_buf, get_data(OFFSET_PAYLOAD_SIZE), sizeof(uint64_t));
    return u64_buf.value();
  }

  inline node_id_t node_id() const {
    uint64_buf_t u64_buf;
    memcpy(&u64_buf, get_data(OFFSET_NODE_ID), sizeof(uint64_t));
    return node_id_t(u64_buf.value());
  }

  inline const char *payload_data() const { return get_data(header_size()); }

  inline void set_timestamp(uint64_t timestamp) {
    uint64_buf_t u64_buf;
    u64_buf = timestamp;
    memcpy(get_data(OFFSET_TIMESTAMP), &u64_buf, sizeof(uint64_t));
  }

  inline void set_payload_size(uint64_t payload_size) {
    uint64_buf_t u64_buf;
    u64_buf = payload_size;
    memcpy(get_data(OFFSET_PAYLOAD_SIZE), &u64_buf, sizeof(uint64_t));
  }

  inline void set_node_id(node_id_t node_id) {
    uint64_buf_t u64_buf;
    u64_buf = uint64_t(node_id);
    memcpy(get_data(OFFSET_NODE_ID), &u64_buf, sizeof(uint64_t));
  }

  inline void copy_payload(const char *src, size_t length) {
    BOOST_ASSERT(size() >= length + header_size());
    memcpy(get_data(header_size()), src, length);
  }
};

inline tx_log_binary tx_log_proto_to_binary(const tx_log_proto &proto) {
  return proto.SerializeAsString();
}

inline tx_log_proto tx_log_binary_to_proto(const tx_log_binary &binary) {
  tx_log_proto proto;
  bool ok = proto.ParseFromString(binary);
  if (!ok) {
  }
  return proto;
}

inline void handle_repeated_tx_logs_to_buffer(repeated_tx_logs &logs,
                                              fn_handle_log_buffer fn) {
  size_t size = 0;
  size_t log_size = logs.size();
  while (size < log_size) {
    log_buffer b(const_cast<char *>(logs.data()) + size, log_size - size);
    size_t buffer_size = b.payload_size() + log_buffer::header_size();
    log_buffer b1(const_cast<char *>(logs.data()) + size, buffer_size);
    fn(b1);
    size += buffer_size;
  }
  BOOST_ASSERT(size==log_size);
}

inline void handle_repeated_tx_logs_to_proto(const repeated_tx_logs &logs,
                                             fn_handle_log_proto_ptr fn) {
  fn_handle_log_buffer callback = [fn](log_buffer buffer) {
    ptr<tx_log_proto> proto(cs_new<tx_log_proto>());
    if (proto->ParseFromArray(buffer.payload_data(), buffer.payload_size())) {
      fn(proto);
    } else {
      PANIC("error handle tx log");
    }
  };
  handle_repeated_tx_logs_to_buffer(const_cast<repeated_tx_logs &>(logs),
                                    callback);
}

inline void handle_repeated_tx_logs_to_proto_and_timestamp(
    const repeated_tx_logs &logs, fn_handle_log_proto_ptr_and_timstamp fn) {
  fn_handle_log_buffer callback = [fn](log_buffer buffer) {
    ptr<tx_log_proto> proto(cs_new<tx_log_proto>());
    if (proto->ParseFromArray(buffer.payload_data(), buffer.payload_size())) {
      fn(proto, buffer.timestamp(), buffer.node_id());
    } else {
      PANIC("error handle tx log to proto, ParseFromArray error");
    }
  };
  handle_repeated_tx_logs_to_buffer(const_cast<repeated_tx_logs &>(logs),
                                    callback);
}