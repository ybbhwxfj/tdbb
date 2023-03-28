#pragma once

#include "common/byte_buffer.h"
#include "common/define.h"
#include "common/id.h"
#include "common/message.h"
#include "common/msg_time.h"
#include "common/ptr.hpp"
#include "common/panic.h"
#include "common/read_write_pb.hpp"
#include "common/result.hpp"
#include "network/message_handler.h"
#include <boost/archive/polymorphic_binary_oarchive.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <memory>
#include <mutex>

using boost_ec = boost::system::error_code;
using boost::asio::ip::tcp;
class sender;

// do not use private inheritance
class connection : public std::enable_shared_from_this<connection> {
private:
  node_id_t peer_;
  message_handler handler_;
  uint64_t offset_;
  ptr<tcp::socket> socket_;
  // there may be more than one write IO action at a time, so mutex is necessary
  std::mutex mutex_;

  // recv_buf_
  byte_buffer recv_buf_;
  std::stringstream ssm_;
  byte_buffer send_buf_;
  std::vector<ptr<byte_buffer>> send_msg_pending_;

  bool connected_;
  bool client_;
  bool writing_in_action_;
  msg_time time1_;
  msg_time time2_;
  boost::asio::io_context::strand strand_;
  uint64_t read_start_;

public:
  connection(boost::asio::io_context::strand s, node_id_t id)
      : peer_(id), offset_(0), connected_(false), client_(true),
        writing_in_action_(false), time1_("conn"), strand_(s) {}

  connection(boost::asio::io_context::strand s, ptr<tcp::socket> socket,
             message_handler handler, bool client)
      : handler_(handler), offset_(0), socket_(socket),
        connected_(socket_->is_open()), client_(client),
        writing_in_action_(false), time1_("conn"), strand_(s) {}

  const boost::asio::io_context::strand &get_strand() const { return strand_; }

  void connected();

  virtual void connected(ptr<tcp::socket> sock, message_handler h);

  virtual ~connection() {
    close();
    // time1_.print();
    // LOG(info) << "time2:";
    // time2_.print();
  }

  void close();

  void async_read();

  void async_write(const byte_buffer &buffer);

  void async_read_done(berror ec, size_t bytes_recv);

  void async_write_done();

  void set_handler(message_handler handler) { handler_ = handler; };

  result<void> process_message_buffer();

  result<void> process_message_body(message_type msg_id, msg_hdr *hdr);

  void process_error(berror ec);

  template<typename M>
  result<void> send_message(message_type id, const M &msg) {
    std::lock_guard lk(mutex_);
#ifdef DEBUG_NETWORK_SEND_RECV
    fn_msg_hdr fn = [this](msg_hdr &hdr) {
      hdr.set_offset(this->offset_);
      this->debug_hdr_send(hdr);
    };
#else // TRACE_MESSAGE
    fn_msg_hdr fn = nullptr;
#endif
    if (not connected_) {
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
    if (writing_in_action_) {
      BOOST_ASSERT(false);
    }
    writing_in_action_ = true;
    send_buf_.reset();
    result<void> res = proto_to_buf(send_buf_, id, msg, fn);
    if (not res) {
      return res;
    }

    uint32_t len = send_buf_.read_available_size();
    boost_ec ec;
    size_t size = boost::asio::write(
        *socket_, boost::asio::buffer(send_buf_.read_begin(), len), ec);
    if (ec.failed()) {
      return outcome::failure(ec);
    }
#ifdef DEBUG_NETWORK_SEND_RECV
    offset_ += size;
#endif
    if (len!=size) {
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    writing_in_action_ = false;
    return outcome::success();
  }

  template<typename M> result<void> recv_message(message_type id, M &msg) {
    std::lock_guard<std::mutex> lk(mutex_);
    recv_buf_.reset();

    auto r = recv_msg_hdr();
    if (not r) {
      LOG(error) << "message header read error " << r.error().message();
      return outcome::failure(r.error().code());
    }
    msg_hdr header = r.value();
    if (!header.check_magic()) {
#ifdef DEBUG_NETWORK_SEND_RECV
      debug_hdr_recv(header, false);
#endif
      return outcome::failure(EC::EC_MESSAGE_MAGIC_ERROR);
    }
#ifdef DEBUG_NETWORK_SEND_RECV
    debug_hdr_recv(header);
#endif
    uint64_t body_size = header.body_length();
    if (header.type()!=id) {
      PANIC("error message id");
      return outcome::failure(EC::EC_MESSAGE_ID_ERROR);
    }
    if (body_size >= recv_buf_.write_available_size()) {
      recv_buf_.resize_capacity(recv_buf_.get_write_pos() + body_size);
    }
    recv_buf_.reset();
    recv_buf_.set_write_pos(body_size);

    size_t rd_size = boost::asio::read(
        *socket_, boost::asio::buffer(recv_buf_.data(), body_size));
#ifdef DEBUG_NETWORK_SEND_RECV
    size_t hash =
        boost::hash_range(recv_buf_.read_begin(), recv_buf_.write_begin());
    if (header.hash() != hash) {
      return outcome::failure(EC::EC_MESSAGE_HASH_ERROR);
    }
#endif
    if (rd_size!=body_size) {
      PANIC("read body size");
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    result<void> um_result = buf_to_proto(recv_buf_, msg);
#ifdef DEBUG_NETWORK_SEND_RECV
    auto recv_tailer = recv_msg_hdr();
    if (not recv_tailer) {
      LOG(error) << "message tailer read error " << r.error().message();
      return outcome::failure(r.error().code());
    }
    msg_hdr tailer = r.value();
    if (!tailer.check_magic()) {
      debug_hdr_recv(tailer, false);
      return outcome::failure(EC::EC_MESSAGE_MAGIC_ERROR);
    }
    debug_hdr_recv(tailer, false);
    recv_buf_.reset();
#endif

    return um_result;
  }

  template<typename M>
  result<void> async_send(message_type id, const ptr<M> msg,
                          bool non_connect = false) {
    return gut_async_send(id, msg, non_connect);
  }

private:
  template<typename M>
  result<void> gut_async_send(message_type id, const ptr<M> msg,
                              bool non_connect = false) {
#ifdef DEBUG_NETWORK_SEND_RECV
    fn_msg_hdr fn = [this](msg_hdr &hdr) {
      hdr.set_offset(offset_);
      this->debug_hdr_send(hdr);
    };
#else // TRACE_MESSAGE
    fn_msg_hdr fn = nullptr;
#endif
    if (not connected_) {
      if (non_connect) {
        ptr<byte_buffer> buf(cs_new<byte_buffer>());
        result<void> res = proto_to_buf(*buf, id, *msg, fn);
        if (not res) {
          return res;
        }
        send_msg_pending_.push_back(buf);
        return outcome::failure(EC::EC_NET_UNCONNECTED);
      } else {
        return outcome::failure(EC::EC_NET_UNCONNECTED);
      }
    }
    if (not writing_in_action_) { // no message sending in action ...
      send_buf_.reset();
      result<void> res = proto_to_buf(send_buf_, id, *msg, fn);
      if (not res) {
        return res;
      }
      writing_in_action_ = true;
      async_write(send_buf_);
      return outcome::success();
    } else {
      if (send_msg_pending_.empty() ||
          (not send_msg_pending_.empty() &&
              (*send_msg_pending_.rbegin())->size() > MESSAGE_BUFFER_SIZE)) {
        // create a new buffer
        send_msg_pending_.push_back(ptr<byte_buffer>(new byte_buffer()));
      }

      ptr<byte_buffer> &last = *send_msg_pending_.rbegin();
      result<void> res = proto_to_buf(*last, id, *msg, fn);
      if (not res) {
        LOG(fatal) << "proto_to_buf error";
        return outcome::failure(EC::EC_MARSHALL_ERROR);
      } else {
        return outcome::success();
      }
    }
  }

  std::string remote_endpoint() const {
    if (socket_->is_open()) {
      try {
        return socket_->remote_endpoint().address().to_string() + ":" +
            std::to_string(socket_->remote_endpoint().port());
      } catch (boost::system::system_error &e) {
        return std::string(e.what());
      }
    } else {
      return std::string();
    }
  }

  std::string local_endpoint() const {
    if (socket_->is_open()) {
      try {
        return socket_->local_endpoint().address().to_string() + ":" +
            std::to_string(socket_->local_endpoint().port());
      } catch (boost::system::system_error &e) {
        return std::string(e.what());
      }
    } else {
      return std::string();
    }
  }

  void debug_hdr_send(const msg_hdr &hdr, bool is_header = true) {
    std::string header = is_header ? "header" : "tailer";
    LOG(info) << "send: " << header << " [" << local_endpoint() << "] to"
              << "[" << remote_endpoint() << "]" << hdr.to_string();
  }

  void debug_hdr_recv(const msg_hdr &hdr, bool is_header = true) {
    std::string header = is_header ? "header" : "tailer";
    LOG(info) << "recv: " << header << " [" << remote_endpoint() << "] to"
              << " [" << local_endpoint() << "]" << hdr.to_string();
  }

  result<msg_hdr> recv_msg_hdr() {
    if (msg_hdr::size() > recv_buf_.write_available_size()) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }

    boost::system::error_code ec;
    size_t hdr_size = boost::asio::read(
        *socket_, boost::asio::buffer(recv_buf_.read_begin(), msg_hdr::size()),
        ec);
    if (ec.failed()) {
      return outcome::failure(ec);
    }
    recv_buf_.set_write_pos(hdr_size);
    if (hdr_size!=msg_hdr::size()) {
      LOG(error) << "message header read error";
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    auto r = buf_to_msg_hdr(recv_buf_);
    return r;
  }
};