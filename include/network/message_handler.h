#pragma once

#include "common/define.h"
#include "common/result.hpp"
#include "common/message.h"

class connection;

typedef std::function<result<void>(
    ptr<connection> connection,
    message_type id,
    byte_buffer &buffer,
    msg_hdr *hdr
)> message_handler;

typedef std::function<result<void>(
    ptr<connection> connection,
    message_type id,
    ptr<google::protobuf::Message> message
)> proto_message_handler;
