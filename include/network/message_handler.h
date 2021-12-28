#pragma once

#include "common/define.h"
#include "common/result.hpp"
#include "common/message.h"

class connection;

typedef std::function<result<void>(
    ptr<connection> connection,
    message_type id,
    byte_buffer &buffer)> message_handler;

