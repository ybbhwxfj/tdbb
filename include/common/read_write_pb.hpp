#pragma once

#include "common/block_exception.h"
#include "common/byte_buffer.h"
#include "common/define.h"
#include "common/endian.h"
#include "common/message.h"
#include "common/utils.h"
#include "common/variable.h"
#include "proto/hello.pb.h"
#include "proto/raft.pb.h"
#include <boost/functional/hash.hpp>

using namespace boost::endian;

const static uint16_t MSG_HDR_MAGIC_NUMBER1 = 1988;
const static uint16_t MSG_HDR_MAGIC_NUMBER2 = 2021;

extern std::mutex __mutex;
extern std::atomic<uint64_t> _sequence;
extern std::unordered_map<uint64_t, std::vector<int8_t>> __map;

inline uint64_t get_sequence() { return ++_sequence; }

inline void buf_insert(uint64_t hash, std::vector<int8_t> buf) {
  std::scoped_lock l(__mutex);
  __map.insert(std::make_pair(hash, buf));
}

inline void find(uint64_t hash, std::vector<int8_t> buf) {
  std::scoped_lock l(__mutex);
  std::vector<std::vector<int8_t>> v;
  auto p = __map.find(hash);
  if (p!=__map.end()) {
    if (p->second.size()==buf.size()) {
      for (size_t i = 0; i < p->second.size(); i++) {
        if (p->second[i]!=buf[i]) {
          LOG(error) << "assert false";
        }
      }
    } else {
      assert(false);
    }
    //__map.erase(p);
  } else {
    BOOST_ASSERT(false);
  }
}

class msg_hdr {
private:
  uint16_buf_t type_;
  uint64_buf_t length_;
#ifdef DEBUG_NETWORK_SEND_RECV
  uint16_buf_t magic1_;
  uint16_buf_t magic2_;
  uint64_buf_t hash_;
  uint64_buf_t offset_;
#endif
#ifdef TEST_NETWORK_TIME
  // millis since epoch when send this message
  uint64_buf_t millis_since_epoch_;
#endif // TEST_NETWORK_TIME
public:
  msg_hdr()
#ifdef DEBUG_NETWORK_SEND_RECV
  : magic1_(MSG_HDR_MAGIC_NUMBER1), magic2_(MSG_HDR_MAGIC_NUMBER2)
#endif
  {
  }

  void set_type(message_type mt) {
    type_ = uint16_t(mt);
#ifdef TEST_NETWORK_TIME
    uint64_t millis = system_clock_ms_since_epoch();
    set_millis_since_epoch(millis);
#endif
  }

  message_type type() const { return message_type(type_.value()); }

#ifdef TEST_NETWORK_TIME
  void set_millis_since_epoch(uint64_t ms) { millis_since_epoch_ = ms; }

  uint64_t millis_since_epoch() const { return millis_since_epoch_.value(); }

#endif

  void set_offset(uint64_t offset) {
    POSSIBLE_UNUSED(offset);
#ifdef DEBUG_NETWORK_SEND_RECV
    offset_ = offset;
#endif
  }

  void set_length(uint64_t size) { length_ = size; }

  uint64_t length() const { return length_.value(); }

  std::string to_string() const {
    std::stringstream ssm;
    ssm << " offset:" << offset()
        << ", message:" << enum2str(message_type(type()))
        << ", length:" << length() << ", hash: " << hash()
        << " , magic:" << magic();
    return ssm.str();
  }

  uint64_t offset() const {
#ifdef DEBUG_NETWORK_SEND_RECV
    return offset_.value();
#else
    return 0;
#endif
  }
  uint64_t body_length() const {
#ifdef DEBUG_NETWORK_SEND_RECV
    return length_.value() - msg_hdr::size() * 2;
#else
    return length_.value() - msg_hdr::size();
#endif
  }

  void set_hash(uint64_t hash) {
    POSSIBLE_UNUSED(hash);
#ifdef DEBUG_NETWORK_SEND_RECV
    hash_ = hash;
#endif
  }

  uint64_t hash() const {
#ifdef DEBUG_NETWORK_SEND_RECV
    return hash_.value();
#else
    return 0;
#endif
  }

  std::string magic() const {
#ifdef DEBUG_NETWORK_SEND_RECV
    bool ok = check_magic();
    uint16_t m1 = magic1_.value();
    uint16_t m2 = magic2_.value();
    std::string str;
    if (not ok) {
      str = "magic mismatch .. ";
    }
    return str + std::to_string(m1) + ":" + std::to_string(m2);
#else
    return std::string();
#endif
  }

  bool check_magic() const {
#ifdef DEBUG_NETWORK_SEND_RECV
    uint16_t m1 = magic1_.value();
    uint16_t m2 = magic2_.value();
    return (m1 == MSG_HDR_MAGIC_NUMBER1 && m2 == MSG_HDR_MAGIC_NUMBER2);
#else
    return true;
#endif
  }

  static size_t size() { return sizeof(msg_hdr); }
};

typedef std::function<void(msg_hdr &)> fn_msg_hdr;

template<class PROTOBUF>
result<void> proto_to_buf(byte_buffer &buffer, message_type id, PROTOBUF &msg,
                          fn_msg_hdr fn) {
  uint64_t header_and_tailer_size;
#ifdef DEBUG_NETWORK_SEND_RECV
  header_and_tailer_size = msg_hdr::size() * 2;
#else
  header_and_tailer_size = msg_hdr::size();
#endif

  size_t body_size = msg.ByteSizeLong();
  if (buffer.write_available_size() < header_and_tailer_size + body_size) {
    buffer.resize_write_available_size(header_and_tailer_size + body_size);
  }

  BOOST_ASSERT(buffer.size() >=
      body_size + msg_hdr::size() + buffer.get_write_pos());
  bool ok = msg.SerializeToArray(buffer.write_begin() + msg_hdr::size(),
                                 buffer.write_available_size());
  if (!ok) {
    return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
  }

  // second, switch to header write position, and write message header
  uint64_t wpos = buffer.get_write_pos();
  msg_hdr header;
  header.set_type(id);
  header.set_length(body_size + header_and_tailer_size);

#ifdef DEBUG_NETWORK_SEND_RECV
  uint64_t hash =
      boost::hash_range(buffer.write_begin() + msg_hdr::size(),
                        buffer.write_begin() + msg_hdr::size() + body_size);
  // LOG(info) << "send message hash " << hash;
  header.set_hash(hash);
#endif

  if (fn) {
#ifdef DEBUG_NETWORK_SEND_RECV
    fn(header);
#endif
  }

  result<void> write_header_result = buffer.write(&header, sizeof(header));
  if (!write_header_result) {
    buffer.set_write_pos(wpos);
    return write_header_result;
  }
#ifdef DEBUG_NETWORK_SEND_RECV
  buffer.set_write_pos(wpos + msg_hdr::size() + body_size);
  result<void> write_tailer_result = buffer.write(&header, sizeof(header));
  if (!write_tailer_result) {
    buffer.set_write_pos(wpos);
    return write_tailer_result;
  }
#endif
  // set the buffer write position

  buffer.set_write_pos(wpos + header_and_tailer_size + body_size);

  return outcome::success();
}

inline result<msg_hdr> buf_to_msg_hdr(byte_buffer &buffer) {
  if (buffer.read_available_size() < msg_hdr::size()) {
    return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
  }
  uint32_t rpos = buffer.get_read_pos();
  BOOST_ASSERT(rpos==buffer.get_read_pos());
  msg_hdr header;
  result<void> read_header_result = buffer.read(&header, sizeof(header));
  if (!read_header_result) {
    buffer.set_read_pos(rpos);
    return read_header_result.error();
  }

  return outcome::success(header);
}

template<class PROTOBUF>
result<void> buf_to_proto(byte_buffer &buffer, PROTOBUF &msg) {
  bool ok =
      msg.ParseFromArray(buffer.read_begin(), buffer.read_available_size());
  if (!ok) {
    LOG(fatal) << "unmarshall error";
    return outcome::failure(EC::EC_UNMARSHALL_ERROR);
  }
  return outcome::success();
}
