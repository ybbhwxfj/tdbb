#pragma once

#include "common/byte_buffer.h"
#include "common/message.h"
#include "common/ptr.hpp"
#include "common/read_write_pb.hpp"
#include "common/result.hpp"
#include "common/utils.h"
#include "network/connection.h"
#include "proto/proto.h"

class processor {
public:
  virtual ~processor() {}

  virtual result<void> process(ptr<connection> conn, message_type id,
                               byte_buffer &buffer, msg_hdr *hdr) = 0;

  virtual result<void> process_msg(ptr<connection> conn, message_type id,
                                   ptr<google::protobuf::Message> msg) = 0;

  virtual processor *clone(void *block_ptr) = 0;
};

class block_handler {
public:
  template<typename T>
  result<void> handle_message(const ptr<connection>, message_type,
                              const ptr<T>) {
    return outcome::success();
  }
};

template<typename T, typename B_PTR = ptr<block_handler>>
class message_processor : public processor {
public:
  message_processor() {}

  result<void> process_msg(ptr<connection> conn, message_type id,
                           ptr<google::protobuf::Message> msg) {
    auto msg_t = std::static_pointer_cast<T>(msg);
    auto r2 = b_ptr_->handle_message(conn, id, msg_t);
    if (not r2) {
      return r2;
    }
    return r2;
  }

  result<void> process(ptr<connection> conn, message_type id,
                       byte_buffer &buffer, msg_hdr *hdr) override {
    ptr<T> message(new T());
    if (hdr!=nullptr) {
    }
    auto r1 = buf_to_proto(buffer, *message);
    if (not r1) {
      return r1;
    }
    auto r2 = this->process_msg(conn, id, message);
    return r2;
  }

  processor *clone(void *p) override {
    message_processor<T, B_PTR> *r = new message_processor<T, B_PTR>();
    if (p) {
      r->b_ptr_ = *((B_PTR *) (p));
    } else {
      r->b_ptr_ = b_ptr_;
    }
    r->message_ = message_;
    return r;
  }

private:
  B_PTR b_ptr_;
  T message_;
};

template<typename B_PTR = ptr<block_handler>>
class processor_sink : public processor {
public:
  processor_sink() : b_(nullptr), b_type_(MESSAGE_BLOCK_INVALID) { init(); }

  processor_sink(B_PTR b, message_block bt) : b_(b), b_type_(bt) { init(); }

  result<void> process(ptr<connection> conn, message_type id,
                       byte_buffer &buffer, msg_hdr *hdr) override {
    processor *p = processor_[id];
    if (p) {
      return p->process(conn, id, buffer, hdr);
    } else {
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    }
  }

  result<void> process_msg(ptr<connection> conn, message_type id,
                           ptr<google::protobuf::Message> msg_ptr) override {
    processor *p = processor_[id];
    if (p) {
      return p->process_msg(conn, id, msg_ptr);
    } else {
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    }
  }

  processor *clone(void *) override {
    BOOST_ASSERT(false); // cannot clone;
    return nullptr;
  }

  ~processor_sink() override {
    for (auto &id : processor_) {
      if (id) {
        delete id;
        id = nullptr;
      }
    }
  }

  message_block message_block_type() const { return b_type_; }

  message_block get_message_block_type(message_type t) const {
    return block_[t];
  }

private:
  void init() {
    block_.resize(MESSAGE_END + 1, MESSAGE_BLOCK_INVALID);
    for (const auto &kv1 : id2processor_) {
      message_block t1 = kv1.first;
      for (auto kv2 : kv1.second) {
        message_type t2 = kv2.first;
        block_[t2] = t1;
      }
    }
    processor_.resize(MESSAGE_END + 1, nullptr);
    auto i = id2processor_.find(b_type_);
    if (i!=id2processor_.end()) {
      for (auto kv : i->second) {
        processor *p = kv.second;
        if (p) {
          processor_[kv.first] = p->clone(&b_);
        }
      }
    }
  };

private:
  B_PTR b_;
  message_block b_type_;
  std::vector<processor *> processor_;
  std::vector<message_block> block_;

private:
  static std::map<message_block, std::map<message_type, processor *>>
      id2processor_;
};

#define NP(T) (new message_processor<T, B_PTR>())

template<typename B_PTR>
std::map<message_block, std::map<message_type, processor *>>
    processor_sink<B_PTR>::id2processor_ = {
    {MESSAGE_BLOCK_CCB,
     {
         {REQUEST_HELLO, NP(hello)},
         {RESPONSE_HELLO, NP(hello)},
         {CLOSE_REQ, NP(close_request)},

         // the following message are processed by RLB

         // the following message are processed by CCB
         {R2C_REGISTER_RESP, NP(rlb_register_ccb_response)},
         {COMMIT_LOG_ENTRIES, NP(rlb_commit_entries)},
         {D2C_READ_DATA_RESP, NP(dsb_read_response)},

         {CLIENT_TX_REQ, NP(tx_request)},
         {CLIENT_CCB_STATE_REQ, NP(ccb_state_req)},
         {CCB_HANDLE_WARM_UP_REQ, NP(warm_up_req)},
         {CCB_HANDLE_WARM_UP_RESP, NP(warm_up_resp)},
         {TX_TM_COMMIT, NP(tx_tm_commit)},
         {TX_TM_ABORT, NP(tx_tm_abort)},
         {TX_TM_END, NP(tx_tm_end)},
         {TX_RM_PREPARE, NP(tx_rm_prepare)},
         {TX_RM_ACK, NP(tx_rm_ack)},
         {TX_VICTIM, NP(tx_victim)},
         {TX_TM_REQUEST, NP(tx_request)},
         {LEAD_STATUS_REQUEST, NP(lead_status_request)},
         {LEAD_STATUS_RESPONSE, NP(lead_status_response)},

         {CALVIN_EPOCH, NP(calvin_epoch)},
         {CALVIN_PART_COMMIT, NP(calvin_part_commit)},
         {CALVIN_EPOCH_ACK, NP(calvin_epoch_ack)},

         {DL_DEPENDENCY, NP(dependency_set)},
         {CCB_ERROR_CONSISTENCY, NP(error_consistency)},
     }},
    {MESSAGE_BLOCK_DSB,
     {
         {DSB_HANDLE_WARM_UP_REQ, NP(warm_up_req)},
         {C2D_READ_DATA_REQ, NP(ccb_read_request)},
         {R2D_REGISTER_RESP, NP(rlb_register_dsb_response)},
         {CLIENT_LOAD_DATA_REQ, NP(client_load_data_request)},
         {R2D_REPLAY_TO_DSB_REQ, NP(replay_to_dsb_request)},
         {DSB_ERROR_CONSISTENCY, NP(error_consistency)},
     }},
    {MESSAGE_BLOCK_RLB,
     {
         {RAFT_APPEND_ENTRIES_REQ, NP(append_entries_request)},
         {RAFT_APPEND_ENTRIES_RESP, NP(append_entries_response)},
         {RAFT_REQ_VOTE_REQ, NP(request_vote_request)},
         {RAFT_REQ_VOTE_RESP, NP(request_vote_response)},
         {RAFT_PRE_VOTE_REQ, NP(pre_vote_request)},
         {RAFT_PRE_VOTE_RESP, NP(pre_vote_response)},
         {RAFT_TRANSFER_LEADER, NP(transfer_leader)},
         {RAFT_TRANSFER_NOTIFY, NP(transfer_notify)},
         {C2R_APPEND_LOG_REQ, NP(ccb_append_log_request)},
         {D2R_WRITE_BATCH_RESP, NP(replay_to_dsb_response)},
         {C2R_REGISTER_REQ, NP(ccb_register_ccb_request)},
         {D2R_REGISTER_REQ, NP(dsb_register_dsb_request)},
         {C2R_REPORT_STATUS_RESP, NP(ccb_report_status_response)},
     }},
    {MESSAGE_BLOCK_CLI,
     {
         {CLIENT_HANDLE_WARM_UP_RESP, NP(warm_up_resp)},
         {CLIENT_TX_RESP, NP(tx_response)},
         {CLIENT_LOAD_DATA_RESP, NP(client_load_data_response)},
         {CLIENT_SYNC, NP(client_sync)},
         {CLIENT_BENCH_STOP, NP(client_bench_stop)},
         {CLIENT_CCB_STATE_RESP, NP(ccb_state_resp)},
         {CLIENT_SYNC_CLOSE, NP(close_request)},
     }},

};
