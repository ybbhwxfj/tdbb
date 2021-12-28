#pragma once
#include "common/ptr.hpp"
#include "common/buffer.h"
#include "common/define.h"
#include "common/message.h"
#include "common/id.h"
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

  ptr<tcp::socket> socket_;
  // there may be more than one write IO action at a time, so mutex is necessary
  std::mutex mutex_;

  // recv_buf_
  byte_buffer recv_buf_;

  byte_buffer send_buf_;
  std::vector<ptr<byte_buffer>> send_msg_pending_;

  bool connected_;
  bool client_;
  bool writing_in_action_;

public:
  connection(node_id_t id) :
      peer_(id),
      connected_(false), client_(true), writing_in_action_(false) {}

  connection(ptr<tcp::socket> socket, message_handler handler, bool client)
      : handler_(handler), socket_(socket), connected_(true),
        client_(client), writing_in_action_(false) {}

  bool is_connected();

  void connected();

  virtual void connected(ptr<tcp::socket> sock, message_handler h);

  void unconnected();

  virtual ~connection() {
    connected_ = false;
  }

  void close();

  void async_read();

  void async_write(const byte_buffer &buffer);

  void async_read_done(berror ec, size_t bytes_recv);

  void async_write_done();

  void set_handler(message_handler handler) { handler_ = handler; };

  result<void> process_message_buffer();

  result<void> process_message_body(message_type msg_id);

  void process_error(berror ec);

  template<typename M>
  result<void> send_message(message_type id, const M &msg) {
    std::lock_guard lk(mutex_);
    if (not connected_) {
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
    if (writing_in_action_) {
      BOOST_ASSERT(false);
    }
    writing_in_action_ = true;
    send_buf_.reset();
    result<void> res = pb_msg_to_buf(send_buf_, id, msg);
    if (not res) {
      return res;
    }

    uint32_t len = send_buf_.get_rsize();
    boost_ec ec;
    size_t size = boost::asio::write(
        *socket_, boost::asio::buffer(send_buf_.rbegin(), len), ec);
    if (ec.failed()) {
      return outcome::failure(ec);
    }
    if (len != size) {
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    writing_in_action_ = false;
    return outcome::success();
  }

  template<typename M> result<void> recv_message(message_type id, M &msg) {
    std::lock_guard<std::mutex> lk(mutex_);
    recv_buf_.reset();

    msg_hdr header;
    if (sizeof(header) > recv_buf_.get_wsize()) {
      return outcome::failure(EC::EC_INSUFFICIENT_SPACE);
    }

    boost::system::error_code ec;
    size_t hdr_size = boost::asio::read(
        *socket_, boost::asio::buffer((void *)&header, sizeof(header)), ec);
    if (ec.failed()) {
      return outcome::failure(ec);
    }
    if (hdr_size != sizeof(header)) {
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    if (!header.check_magic()) {
      return outcome::failure(EC::EC_MESSAGE_MAGIC_ERROR);
    }
    uint64_t body_size = header.length();
    if (header.type() != id) {
      return outcome::failure(EC::EC_MESSAGE_ID_ERROR);
    }
    if (body_size >= recv_buf_.get_wsize()) {
      recv_buf_.resize(recv_buf_.get_wpos() + body_size);
    }

    recv_buf_.set_wpos(body_size);

    size_t rd_size = boost::asio::read(
        *socket_, boost::asio::buffer(recv_buf_.data(), body_size));
    size_t hash = boost::hash_range(recv_buf_.rbegin(), recv_buf_.wbegin());
    if (header.hash() != hash) {
      return outcome::failure(EC::EC_MESSAGE_HASH_ERROR);
    }
    if (rd_size != body_size) {
      return outcome::failure(EC::EC_MESSAGE_LENGTH_ERROR);
    }
    result<void> um_result = buf_to_pb(recv_buf_, msg);
    return um_result;
  }

  template<typename M> result<void> async_send(message_type id, const M &msg) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (not connected_) {
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
    if (not writing_in_action_) { // no message sending in action ...
      send_buf_.reset();
      result<void> res = pb_msg_to_buf(send_buf_, id, msg);
      if (not res) {
        return res;
      }
      writing_in_action_ = true;
      async_write(send_buf_);
      return outcome::success();
    } else {
      for (int i = 0; i < 2; i++) {
        if (send_msg_pending_.size() == 0) {
          send_msg_pending_.push_back(
              ptr<byte_buffer>(new byte_buffer(MESSAGE_BUFFER_SIZE)));
        }

        ptr<byte_buffer> &last =
            send_msg_pending_[send_msg_pending_.size() - 1];
        result<void> res = pb_msg_to_buf(*last, id,
                                         msg);
        if (not res) {
          if (res.error() == EC::EC_INSUFFICIENT_SPACE) {
            send_msg_pending_.push_back(
                ptr<byte_buffer>(new byte_buffer(MESSAGE_BUFFER_SIZE)));
            continue;
          } else {
            return res;
          }
        } else {
          return res;
        }
      }
      return berror(EC::EC_INSUFFICIENT_SPACE);
    }
  }
private:
  void debug_hdr_send(const msg_hdr &hdr) {
    BOOST_LOG_TRIVIAL(trace) << "send:"
                             << "[" << socket_->local_endpoint().address() << ":" << socket_->local_endpoint().port()
                             << "] to"
                             << "[" << socket_->remote_endpoint().address() << ":" << socket_->remote_endpoint().port()
                             << "]"
                                ", message:" << enum2str(message_type(hdr.type())) << ", length:" << hdr.type()
                             << ", hash:" << hdr.hash();
  }

  void debug_hdr_recv(const msg_hdr &hdr) {
    BOOST_LOG_TRIVIAL(info) << "recv:"
                            << "[" << socket_->remote_endpoint().address() << ":" << socket_->remote_endpoint().port()
                            << "] to"
                            << "[" << socket_->local_endpoint().address() << ":" << socket_->local_endpoint().port()
                            << "]"
                               ", message:" << enum2str(message_type(hdr.type())) << ", length:" << hdr.type()
                            << ", hash: " << hdr.hash();
  }
};