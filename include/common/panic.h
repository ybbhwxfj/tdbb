#pragma once

#include "common/logger.hpp"

#define PANIC(msg) { LOG(fatal) << (msg); exit(-1); }
