#pragma once

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include "common/db_type.h"
#include "common/block_type.h"

#define NUM_AZ 3
#define NUM_RG 5

struct config_option {
  config_option()
      : priority(false),
        num_az(NUM_AZ),
        num_client(5) {
    ccb_num_shard = NUM_RG;
    dsb_num_shard = NUM_RG;
    rlb_num_shard = NUM_RG;
  }

  void set_num_shard(uint32_t num) {
    set_ccb_num_shard(num);
    set_dsb_num_shard(num);
    set_rlb_num_shard(num);
  }

  void set_config_share_nothing(uint32_t num_shard) {
    set_num_shard(num_shard);
    set_tight_binding();
  }

  void set_config_share(bool is_loose_bind) {
    set_num_shard(1);
    if (is_loose_bind) {
      set_loose_binding();
    } else {
      set_tight_binding();
    }
  }

  void set_config_scale_dsb(uint32_t num, uint32_t dsb_num) {
    set_ccb_num_shard(num);
    set_rlb_num_shard(num);
    set_dsb_num_shard(dsb_num);
    set_ccb_rlb_binding();
  }

  void set_ccb_rlb_binding() {
    std::vector<std::string> vec;
    vec.emplace_back(CCB);
    vec.emplace_back(RLB);
    binding.push_back(vec);

    std::vector<std::string> dsb_vec;
    dsb_vec.emplace_back(DSB);
    binding.push_back(dsb_vec);
  }

  void set_loose_binding() {
    std::vector<std::string> ccb_vec;
    ccb_vec.emplace_back(CCB);
    binding.push_back(ccb_vec);

    std::vector<std::string> rlb_vec;
    rlb_vec.emplace_back(RLB);
    binding.push_back(rlb_vec);

    std::vector<std::string> dsb_vec;
    dsb_vec.emplace_back(DSB);
    binding.push_back(dsb_vec);
  }

  void set_tight_binding() {
    std::vector<std::string> vec;
    vec.emplace_back(CCB);
    vec.emplace_back(RLB);
    vec.emplace_back(DSB);
    binding.push_back(vec);
  }

  uint32_t get_ccb_num_shard() const {
    return ccb_num_shard;
  }

  uint32_t get_dsb_num_shard() const {
    return dsb_num_shard;
  }

  uint32_t get_rlb_num_shard() const {
    return rlb_num_shard;
  }

  void set_ccb_num_shard(uint32_t num) {
    ccb_num_shard = num;
  }

  void set_dsb_num_shard(uint32_t num) {
    dsb_num_shard = num;
  }

  void set_rlb_num_shard(uint32_t num) {
    rlb_num_shard = num;
  }

  uint32_t get_max_num_shard() const {
    uint32_t max_num_shard = std::max(ccb_num_shard, dsb_num_shard);
    max_num_shard = std::max(max_num_shard, rlb_num_shard);
    return max_num_shard;
  }

  uint32_t get_num_shard(const std::string &shard_name) const {
    if (shard_name==CCB) {
      return get_ccb_num_shard();
    } else if (shard_name==DSB) {
      return get_dsb_num_shard();
    } else if (shard_name==RLB) {
      return get_rlb_num_shard();
    } else {
      return 0;
    }
  }

  bool priority;
  std::vector<std::vector<std::string>> binding;
  uint32_t num_az;
  uint32_t ccb_num_shard;
  uint32_t dsb_num_shard;
  uint32_t rlb_num_shard;
  uint32_t num_client;
};
