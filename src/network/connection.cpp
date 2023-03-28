#include "network/connection.h"
#include "common/logger.hpp"
#include "common/ptr.hpp"
#include "common/scoped_time.h"
#include "common/utils.h"
#include <unordered_map>
#include <utility>

std::mutex __mutex;
std::atomic<uint64_t> _sequence = 0;
std::unordered_map<uint64_t, std::vector<int8_t>> __map;

void connection::async_read() {
  // std::lock_guard<std::mutex> lk(mutex_);
  scoped_time _t("connection async read");
  if (not connected_) {
    return;
  }

  BOOST_ASSERT(recv_buf_.get_write_pos() < recv_buf_.size());
  auto t = shared_from_this();
  auto read_handler = boost::asio::bind_executor(
      strand_, [t](const boost_ec &ec, size_t bytes_read) {
        scoped_time _t_read("read call back", 10);
        if (ec == boost::asio::error::eof) {
          return;
        }
        t->read_start_ = steady_clock_ms_since_epoch();
        if (ec.failed()) {
          LOG(error) << "async_read_some error: " << ec.message();
        }
        t->async_read_done(berror(ec), bytes_read);
      });
#ifdef DEBUG_NETWORK_SEND_RECV
  ssm_ << "async read some " << recv_buf_.info_str() << std::endl;
#endif
  socket_->async_read_some(
      boost::asio::buffer(recv_buf_.write_begin(),
                          recv_buf_.write_available_size()),
      read_handler);
}

void connection::async_read_done(berror ec, size_t byte_read) {
  scoped_time _t("connection async read done");
  if (ec.failed()) {
    process_error(ec);
    return;
  }
  uint32_t wpos = recv_buf_.get_write_pos() + uint32_t(byte_read);
  if (wpos > recv_buf_.size()) { // insufficient space
    LOG(fatal) << "insufficient buffer space";
    return;
  }

  recv_buf_.set_write_pos(wpos);

  // LOG(debug) << socket_->remote_endpoint().address() << ":" <<
  // socket_->remote_endpoint().port() << " read " << byte_read << " bytes";
  result<void> result = process_message_buffer();
  if (!result) {
    process_error(result.error());
    return;
  } else {
    async_read();
  }
}

result<void> connection::process_message_buffer() {
#ifdef DEBUG_NETWORK_SEND_RECV
  ssm_ << __LINE__ << " handle recv_buf_: " << recv_buf_.info_str()
       << std::endl;
#endif
  scoped_time _t("process message buffer");
  size_t prev_message_length = 0;
  uint32_t wpos = recv_buf_.get_write_pos();
  while (wpos - recv_buf_.get_read_pos() >= msg_hdr::size()) {
    uint32_t rpos = recv_buf_.get_read_pos();
    result<msg_hdr> r = buf_to_msg_hdr(recv_buf_);
    if (not r) {
      LOG(fatal) << "buf_to_msg_hdr" << r.error().message();
    }

    msg_hdr hdr = r.value();
    if (!hdr.check_magic()) {
      recv_buf_.set_read_pos(rpos);
#ifdef DEBUG_NETWORK_SEND_RECV
      debug_hdr_recv(hdr);
#endif
      LOG(info) << "[" << socket_->remote_endpoint() << "] ["
                << socket_->local_endpoint() << "] " << ssm_.str();
      return outcome::failure(EC::EC_MESSAGE_MAGIC_ERROR);
    }
#ifdef DEBUG_NETWORK_SEND_RECV
    debug_hdr_recv(hdr);
#endif
    // body size + possible tailer size
    uint32_t length = hdr.length() - msg_hdr::size();
    uint32_t body_length = hdr.body_length();
    message_type id = hdr.type();
    uint32_t pos_end_tailer = recv_buf_.get_read_pos() + length;
    uint32_t pos_end_body = recv_buf_.get_read_pos() + body_length;
    if (pos_end_tailer >
        recv_buf_.get_write_pos()) { // insufficient space to store a body(+
      // possible tailer) ...
      assert(recv_buf_.get_read_pos() >= msg_hdr::size());
      recv_buf_.set_read_pos(recv_buf_.get_read_pos() -
          msg_hdr::size()); // unread message header
#ifdef DEBUG_NETWORK_SEND_RECV
      ssm_ << __LINE__ << " break recv_buf_" << recv_buf_.info_str()
           << std::endl;
#endif
      prev_message_length = length;
      break;
    }
#ifdef DEBUG_NETWORK_SEND_RECV
    size_t hash = boost::hash_range(recv_buf_.data() + recv_buf_.get_read_pos(),
                                    recv_buf_.data() +
                                        recv_buf_.get_read_pos() + body_length);

    // LOG(info) << " receive message hash " << hdr.hash();
    if (hash != hdr.hash()) {
      LOG(error) << "receive message hash inconsistency" << hdr.hash();
      debug_hdr_recv(hdr);
      return outcome::failure(EC::EC_MESSAGE_HASH_ERROR);
    }
#endif

#ifdef TEST_NETWORK_TIME
    uint64_t millis = system_clock_ms_since_epoch();
    uint64_t start_ms = hdr.millis_since_epoch();
    if (millis >= start_ms) {
      auto current_ms = system_clock_ms_since_epoch();
      if (current_ms - start_ms > TEST_NETWORK_MS_MAX) {
        LOG(warning) << "send network message " << current_ms - start_ms
                     << "ms, message:" << enum2str(id);
      }
      start_ms = millis - start_ms;
      time1_.add(id, boost::posix_time::milliseconds(start_ms));
    } else {
      LOG(error) << "error test network time";
    }
#endif
    recv_buf_.set_write_pos(pos_end_body);
    result<void> rb = process_message_body(id, &hdr);
    recv_buf_.set_write_pos(wpos);
    if (not rb) {
      LOG(fatal) << "handle message body error: " << r.error().message();
    } else {
      recv_buf_.set_read_pos(recv_buf_.get_read_pos() + body_length);
    }

#ifdef DEBUG_NETWORK_SEND_RECV
    ssm_ << __LINE__ << " handle body, hdr" << hdr.to_string()
         << ", recv_buf_:" << recv_buf_.info_str() << std::endl;
    result<msg_hdr> recv_tailer = buf_to_msg_hdr(recv_buf_);
    if (not recv_tailer) {
      LOG(fatal) << "message tailer read error " << r.error().message();
      return outcome::failure(r.error().code());
    }
    msg_hdr tailer = recv_tailer.value();
    if (!tailer.check_magic()) {
      debug_hdr_recv(tailer, false);
      LOG(fatal) << "message tailer read error " << r.error().message();
      return outcome::failure(EC::EC_MESSAGE_MAGIC_ERROR);
    }
    debug_hdr_recv(tailer, false);
    ssm_ << __LINE__ << " tailer:" << tailer.to_string() << " recv_buf_"
         << recv_buf_.info_str() << std::endl;
#endif
  }

  if (wpos > recv_buf_.get_read_pos()) {
#ifdef DEBUG_NETWORK_SEND_RECV
    ssm_ << __LINE__ << " wpos > rpos, recv_buf_" << recv_buf_.info_str()
         << std::endl;
#endif
    size_t left_begin = recv_buf_.get_read_pos();
    if (left_begin > 0) {
      size_t unprocessed = wpos - recv_buf_.get_read_pos();
      if (unprocessed <= left_begin) { // non-overlapping
        BOOST_ASSERT(recv_buf_.size() >= left_begin + unprocessed);
        std::memcpy(recv_buf_.data(), recv_buf_.data() + left_begin,
                    unprocessed);
      } else { // overlapping
        std::memmove(recv_buf_.data(), recv_buf_.data() + left_begin,
                     unprocessed);
      }

      recv_buf_.set_read_pos(0);
      recv_buf_.set_write_pos(unprocessed);
    }
  } else if (recv_buf_.get_write_pos() == recv_buf_.get_read_pos()) {
#ifdef DEBUG_NETWORK_SEND_RECV
    ssm_ << __LINE__ << " wpos == rpos, recv_buf_" << recv_buf_.info_str()
         << std::endl;
#endif
    recv_buf_.reset();
  } else {
    LOG(fatal) << "handle message buffer size error";
  }
  if (recv_buf_.get_write_pos() + prev_message_length > MESSAGE_BUFFER_SIZE) {
#ifdef DEBUG_NETWORK_SEND_RECV
    ssm_ << __LINE__ << " wpos + prev > recv_buf_:" << recv_buf_.info_str()
         << std::endl;
#endif
    recv_buf_.resize_write_available_size(recv_buf_.get_write_pos() +
        prev_message_length);
  } else if (recv_buf_.get_write_pos() <= MESSAGE_BUFFER_SIZE &&
      recv_buf_.size() > MESSAGE_BUFFER_SIZE) {
#ifdef DEBUG_NETWORK_SEND_RECV
    ssm_ << __LINE__ << " wpos <= buf_size && size > buf_size, recv_buf_:"
         << recv_buf_.info_str() << std::endl;
#endif
    recv_buf_.resize_capacity(MESSAGE_BUFFER_SIZE);
  }
  return outcome::success();
}

result<void> connection::process_message_body(message_type msg_id,
                                              msg_hdr *hdr) {
  scoped_time _t(
      (boost::format("process message %s") % enum2str(msg_id)).str());
  if (handler_ == nullptr) {
    return outcome::success();
  }
  auto t = shared_from_this();
  result<void> r = handler_(t, msg_id, recv_buf_, hdr);
  return r;
}

void connection::process_error(berror ec) {
  if (ec.failed()) {
    LOG(error) << "process connection error:" << ec.message();
    if (socket_->is_open()) {
      socket_->close();
    }
    connected_ = false;
  }
}

void connection::async_write(const byte_buffer &buffer) {
  auto t = shared_from_this();
  size_t size = buffer.read_available_size();
  auto write_handler = boost::asio::bind_executor(
      get_strand(), [size, t](boost_ec ec, size_t _b) {
        if (not ec.failed()) {
          if (size != _b) {
            LOG(fatal) << "async_write size error" << _b;
          }
          t->async_write_done();
          t->offset_ += size;
        } else {
          LOG(error) << "async write error send bytes: " << ec.message();
          t->process_error(berror(ec));
        }
      });

  boost::asio::async_write(
      *socket_,
      boost::asio::buffer(buffer.read_begin(), buffer.read_available_size()),
      write_handler);
}

void connection::async_write_done() {
  send_buf_.reset();
  connected_ = true;
  if (!send_msg_pending_.empty()) {
    size_t size = 0;
    for (const auto &message : send_msg_pending_) {
      size += message->read_available_size();
    }
    if (send_buf_.size() < size) {
      send_buf_.resize_capacity(size);
    }

    for (const auto &message : send_msg_pending_) {
      send_buf_.append(*message);
    }
    BOOST_ASSERT(send_buf_.read_available_size() == size);

    send_msg_pending_.clear();
    async_write(send_buf_);
  } else {
    writing_in_action_ = false;
  }
}

void connection::connected() { connected_ = true; }

void connection::connected(ptr<tcp::socket> sock, message_handler handler) {
  socket_ = std::move(sock);
  connected_ = true;
  if (handler_ == nullptr) {
    handler_ = std::move(handler);
  }
}

void connection::close() {
  if (connected_) {
    connected_ = false;
    if (socket_) {
      try {
        if (socket_->is_open()) {
          socket_->shutdown(boost::asio::socket_base::shutdown_both);
        }
      } catch (std::exception &ex) {
        LOG(error) << "close connection error " << ex.what();
      }
    }
  }
}