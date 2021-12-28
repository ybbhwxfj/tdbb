#pragma once

#include "common/error_code.h"
#include "common/berror.h"
#include <boost/outcome.hpp>
#include <boost/outcome/config.hpp>
#include <boost/outcome/std_result.hpp>

namespace outcome = BOOST_OUTCOME_V2_NAMESPACE;
template<class R, class S = berror,
    class NoValuePolicy = outcome::policy::default_policy<R, S, void>> //
using result = outcome::boost_result<R, S, NoValuePolicy>;