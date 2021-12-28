#pragma once
#include <boost/regex.hpp>

static const boost::regex url_json_prefix{"/json/.*"};
static const boost::regex url_dep{"/json/dep.*"};
static const boost::regex url_lock{"/lock.*"};
static const boost::regex url_tx{"/tx"};
static const boost::regex url_tx_xid{"/tx/(\\d+)"};

static const boost::regex url_log{"/log"};
static const boost::regex url_log_xid{"/log/(\\d+)"};
static const boost::regex url_log_offset{"/log_offset"};
static const boost::regex url_panel{"/panel.*"};
static const boost::regex url_deadlock{"/deadlock"};
static const boost::regex url_json_deadlock{"/json/deadlock"};