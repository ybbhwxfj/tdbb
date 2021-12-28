#include "network/connection.h"
#include "common/ptr.hpp"
#include <boost/log/trivial.hpp>
#include <unordered_map>
#include <utility>

std::mutex __mutex;
std::atomic<uint64_t> _sequence = 0;
std::unordered_map<uint64_t, std::vector<int8_t>> __map;

void connection::async_read() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (not connected_) {
    return;
  }

  BOOST_ASSERT(recv_buf_.get_wpos() < recv_buf_.size());

  auto t = shared_from_this();
  socket_->async_read_some(
      boost::asio::buffer(recv_buf_.wbegin(), recv_buf_.get_wsize()),
      [t](const boost_ec &ec, size_t bytes_read) {
        if (ec.failed()) {
          BOOST_LOG_TRIVIAL(error) << "async_read_some error: " << ec.message();
        }
        if (ec == boost::asio::error::eof) {
          return;
        }
        t->async_read_done(berror(ec), bytes_read);
      });
}

void connection::async_read_done(berror ec, size_t byte_read) {
  if (ec.failed()) {
    process_error(ec);
    return;
  }
  uint32_t wpos = recv_buf_.get_wpos() + uint32_t(byte_read);
  if (wpos > recv_buf_.size()) { // insufficient space
    BOOST_ASSERT(false);
    return;
  }

  recv_buf_.set_wpos(wpos);

  //BOOST_LOG_TRIVIAL(debug) << socket_->remote_endpoint().address() << ":" <<  socket_->remote_endpoint().port() << " read " << byte_read << " bytes";
  result<void> result = process_message_buffer();
  if (!result) {
    process_error(result.error());
    return;
  } else {
    async_read();
  }
}

result<void> connection::process_message_buffer() {

  size_t prev_message_length = 0;
  while (recv_buf_.get_wpos() - recv_buf_.get_rpos() >=
      MSG_HDR_SIZE) {
    result<msg_hdr> r = buf_to_hdr(recv_buf_);
    if (not r) {
      BOOST_ASSERT(false);
      if (r.error() == EC::EC_INSUFFICIENT_SPACE) {
        break;
      } else {
        BOOST_LOG_TRIVIAL(error)
          << "read message header error: " << r.error().message();
        return r.error();
      }
    }
    msg_hdr hdr = r.value();
    uint32_t length = hdr.length();
    message_type id = hdr.type();
    uint32_t new_wpos = recv_buf_.get_rpos() + length;
    if (new_wpos > recv_buf_.get_wpos()) { // insufficient space to store a body ...
      assert(recv_buf_.get_rpos() >= MSG_HDR_SIZE);
      recv_buf_.set_rpos(recv_buf_.get_rpos() -
          MSG_HDR_SIZE); // unread message header

      prev_message_length = new_wpos - recv_buf_.get_wpos();
      break;
    }
    size_t hash = boost::hash_range(recv_buf_.data() + recv_buf_.get_rpos(),
                                    recv_buf_.data() + new_wpos);
    //debug_hdr_recv(hdr);
    //BOOST_LOG_TRIVIAL(info) << " receive message hash " << hdr.hash();
    if (hash != hdr.hash()) {
      BOOST_ASSERT(false);
      return outcome::failure(EC::EC_MESSAGE_HASH_ERROR);
    }

    uint64_t wpos = recv_buf_.get_wpos();
    recv_buf_.set_wpos(new_wpos);
    result<void> rb = process_message_body(id);
    if (not rb) {
      BOOST_ASSERT(false);
    } else {
      recv_buf_.set_wpos(wpos);
      recv_buf_.set_rpos(recv_buf_.get_rpos() + length);
    }
  }

  if (recv_buf_.get_wpos() > recv_buf_.get_rpos()) {
    size_t left_begin = recv_buf_.get_rpos();
    if (left_begin > 0) {
      size_t unprocessed = recv_buf_.get_wpos() - recv_buf_.get_rpos();
      if (unprocessed <= left_begin) { // non-overlapping
        BOOST_ASSERT(recv_buf_.size() >= left_begin + unprocessed);
        std::memcpy(recv_buf_.data(), recv_buf_.data() + left_begin,
                    unprocessed);
      } else { // overlapping
        std::memmove(recv_buf_.data(), recv_buf_.data() + left_begin,
                     unprocessed);
      }
      recv_buf_.set_rpos(0);
      recv_buf_.set_wpos(unprocessed);
    }
  } else if (recv_buf_.get_wpos() == recv_buf_.get_rpos()) {
    recv_buf_.reset();
  } else {
    BOOST_ASSERT(false);
  }
  if (recv_buf_.get_wpos() + prev_message_length > MESSAGE_BUFFER_SIZE) {
    recv_buf_.resize(recv_buf_.get_wpos() + prev_message_length);
  } else if (recv_buf_.get_wpos() <= MESSAGE_BUFFER_SIZE && recv_buf_.size() > MESSAGE_BUFFER_SIZE) {
    recv_buf_.resize(MESSAGE_BUFFER_SIZE);
  }

  return outcome::success();
}

result<void> connection::process_message_body(message_type msg_id) {
  if (handler_ == nullptr) {
    return outcome::success();
  }
  auto t = shared_from_this();
  result<void> r = handler_(t, msg_id, recv_buf_);
  return r;
}

void connection::process_error(berror ec) {
  if (ec) {
    if (socket_->is_open()) {
      socket_->close();
    }
  }
}

void connection::async_write(const byte_buffer &buffer) {
  auto t = shared_from_this();
  size_t size = buffer.get_rsize();
  boost::asio::async_write(
      *socket_, boost::asio::buffer(buffer.rbegin(), buffer.get_rsize()),
      [size, t, buffer](boost_ec ec, size_t _b) {
        if (not ec.failed()) {
          if (size != _b) {
            BOOST_ASSERT(false);
            BOOST_LOG_TRIVIAL(error)
              << "async write message fail, send bytes:" << _b;
          }
          t->async_write_done();
        } else {
          t->process_error(berror(ec));
        }
      });
}

void connection::async_write_done() {
  std::lock_guard lk(mutex_);
  send_buf_.reset();
  connected_ = true;
  if (!send_msg_pending_.empty()) {
    size_t size = 0;
    for (const auto &message: send_msg_pending_) {
      size += message->get_rsize();
    }
    if (send_buf_.size() < size) {
      send_buf_.resize(size);
      send_buf_.clear();
    }

    for (const auto &message: send_msg_pending_) {
      send_buf_.append(*message);
    }
    send_msg_pending_.clear();
    async_write(send_buf_);
  } else {
    writing_in_action_ = false;
  }
}

bool connection::is_connected() {
  std::lock_guard<std::mutex> lk(mutex_);
  return connected_;
}

void connection::connected() {
  std::lock_guard<std::mutex> lk(mutex_);
  connected_ = true;
}

void connection::connected(ptr<tcp::socket> sock, message_handler handler) {
  std::lock_guard<std::mutex> lk(mutex_);

  socket_ = std::move(sock);
  connected_ = true;
  if (handler_ == nullptr) {
    handler_ = std::move(handler);
  }
}

void connection::unconnected() {
  std::lock_guard<std::mutex> lk(mutex_);
  connected_ = false;
}

void connection::close() {
  std::lock_guard<std::mutex> lk(mutex_);
  connected_ = false;
  if (socket_) {
    socket_->close();
  }
}