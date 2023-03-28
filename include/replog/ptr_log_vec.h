#pragma once

#include "common/ptr.hpp"
#include "common/tx_log.h"
#include "proto/proto.h"
#include <vector>

typedef ptr<std::vector<ptr<tx_log_binary>>> ptr_log_vec;