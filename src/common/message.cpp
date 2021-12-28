#include "common/message.h"

template<> enum_strings<message_type>::e2s_t enum_strings<message_type>::enum2str = {
    {REQUEST_MIN, "REQUEST_MIN"},

    {REQUEST_HELLO, "REQUEST_HELLO"},
    {RESPONSE_HELLO, "RESPONSE_HELLO"},

    {CLOSE_REQ, "CLOSE_REQ"},
    {CLOSE_RESP, "CLOSE_RESP"},

    // the following message are processed by RLB
    {RLB_MESSAGE_BEGIN, "RLB_MESSAGE_BEGIN"},
    {RAFT_APPEND_ENTRIES_REQ, "RAFT_APPEND_ENTRIES_REQ"},
    {RAFT_APPEND_ENTRIES_RESP, "RAFT_APPEND_ENTRIES_RESP"},
    {RAFT_REQ_VOTE_REQ, "RAFT_REQ_VOTE_REQ"},
    {RAFT_REQ_VOTE_RESP, "RAFT_REQ_VOTE_RESP"},

    {RAFT_TRANSFER_LEADER, "RAFT_TRANSFER_LEADER"},
    {RAFT_TRANSFER_NOTIFY, "RAFT_TRANSFER_NOTIFY"},
    {C2R_APPEND_LOG_REQ, "C2R_APPEND_LOG_REQ"},
    {C2R_REPLAY_LOG_RESP, "C2R_REPLAY_LOG_RESP"},
    {D2R_WRITE_BATCH_RESP, "D2R_WRITE_BATCH_RESP"},
    {C2R_REGISTER_REQ, "C2R_REGISTER_REQ"},
    {D2R_REGISTER_REQ, "D2R_REGISTER_REQ"},
    {C2R_REPORT_STATUS_RESP, "C2R_REPORT_STATUS_RESP"},
    {RLB_MESSAGE_END, "RLB_MESSAGE_END"},

    // the following message are processed by CCB
    {CCB_MESSAGE_BEGIN, "CCB_MESSAGE_BEGIN"},
    {R2C_REGISTER_RESP, "R2C_REGISTER_RESP"},
    {COMMIT_LOG_ENTRIES, "COMMIT_LOG_ENTRIES"},
    {D2C_READ_DATA_RESP, "D2C_READ_DATA_RESP"},

    {R2C_REPORT_STATUS_REQ, "R2C_REPORT_STATUS_REQ"},

    {CCB_BORADCAST_STATUS_REQ, "CCB_BORADCAST_STATUS_REQ"},
    {CCB_BORADCAST_STATUS_RESP, "CCB_BORADCAST_STATUS_RESP"},
    {CLIENT_TX_REQ, "CLIENT_TX_REQ"},
    {TX_TM_COMMIT, "TX_TM_COMMIT"},
    {TX_TM_ABORT, "TX_TM_ABORT"},
    {TX_RM_PREPARE, "TX_RM_PREPARE"},
    {TX_RM_ACK, "TX_RM_ACK"},
    {TX_TM_REQUEST, "TX_TM_REQUEST"},
    {LEAD_STATUS_REQUEST, "LEAD_STATUS_REQUEST"},
    {LEAD_STATUS_RESPONSE, "LEAD_STATUS_RESPONSE"},

    {CALVIN_EPOCH, "CALVIN_EPOCH"},
    {CALVIN_PART_COMMIT, "CALVIN_PART_COMMIT"},
    {CALVIN_EPOCH_ACK, "CALVIN_EPOCH_ACK"},

    {RM_ENABLE_VIOLATE, "RM_ENABLE_VIOLATE"},
    {TM_ENABLE_VIOLATE, "TM_ENABLE_VIOLATE"},
    {PANEL_INFO_RESP_TO_CCB, "PANEL_INFO_RESP_TO_CCB"},
    {CCB_MESSAGE_END, "CCB_MESSAGE_END"},

    // the following message are processed by DSB
    {DSB_MESSAGE_BEGIN, "DSB_MESSAGE_BEGIN"},
    {C2D_READ_DATA_REQ, "C2D_READ_DATA_REQ"},

    {R2D_REGISTER_RESP, "R2D_REGISTER_RESP"},
    {CLIENT_LOAD_DATA_REQ, "CLIENT_LOAD_DATA_REQ"},
    {DSB_MESSAGE_END, "DSB_MESSAGE_END"},

    {PANEL_MESSAGE_BEGIN, "PANEL_MESSAGE_BEGIN"},
    {PANEL_REPORT, "PANEL_REPORT"},
    {PANEL_INFO_REQ, "PANEL_INFO_REQ"},
    {PANEL_MESSAGE_END, "PANEL_MESSAGE_END"},

    {CLI_MESSAGE_BEGIN, "CLI_MESSAGE_BEGIN"},
    {CLIENT_TX_RESP, "CLIENT_TX_RESP"},
    {CLIENT_LOAD_DATA_RESP, "CLIENT_LOAD_DATA_RESP"},
    {PANEL_INFO_RESP_TO_CLIENT, "  PANEL_INFO_RESP_TO_CLIENT,"},
    {CLI_MESSAGE_END, "CLI_MESSAGE_END"},
};