#pragma once

#include <stdint.h>
#include <map>
#include <vector>
#include <boost/assert.hpp>
#include <set>
#include "common/block_type.h"
typedef uint32_t az_id_t; //Availability Zone Id
typedef uint32_t shard_id_t; //Replication Group Id
typedef uint32_t node_id_t; // Node ID
typedef uint32_t table_id_t;
typedef uint32_t column_id_t;
typedef uint32_t oid_t;
typedef uint64_t tuple_id_t;
typedef uint64_t xid_t;
typedef uint64_t log_index_t; // started from 1

//#define MAKE_NODE_ID(_az_id, _rg_id) (((_az_id) << 16) | (rg_id))

#define __CCB_FLAG  1
#define __RLB_FLAG  2
#define __DSB_FLAG  4
#define __PNB_FLAG  8
#define __CLB_FLAG 16
inline node_id_t MAKE_NODE_ID(az_id_t az, shard_id_t rg, const std::set<std::string> &block_types) {
  uint32_t ccb_flag = block_types.contains(BLOCK_CCB) ? __CCB_FLAG : 0;
  uint32_t rlb_flag = block_types.contains(BLOCK_RLB) ? __RLB_FLAG : 0;
  uint32_t dsb_flag = block_types.contains(BLOCK_DSB) ? __DSB_FLAG : 0;
  uint32_t pnb_flag = block_types.contains(BLOCK_PNB) ? __PNB_FLAG : 0;
  uint32_t clb_flag = block_types.contains(BLOCK_PNB) ? __CLB_FLAG : 0;
  return rg << 16 | az << 8 | ccb_flag | rlb_flag | dsb_flag | pnb_flag | clb_flag;
}

inline az_id_t TO_RG_ID(node_id_t node_id) {
  return az_id_t(node_id >> 16);
}

inline shard_id_t TO_AZ_ID(node_id_t node_id) {
  return shard_id_t((node_id & 0x0000ff00) >> 8);
}

inline bool is_ccb_block(node_id_t node_id) {
  return (node_id & __CCB_FLAG) != 0;
}

inline bool is_rlb_block(node_id_t node_id) {
  return (node_id & __RLB_FLAG) != 0;
}

inline bool is_dsb_block(node_id_t node_id) {
  return (node_id & __DSB_FLAG) != 0;
}

inline bool is_pnb_block(node_id_t node_id) {
  return (node_id & __PNB_FLAG) != 0;
}
inline bool is_block_type(node_id_t node_id, block_type_id_t id) {
  if (id == BLOCK_TYPE_ID_DSB) {
    return is_dsb_block(node_id);
  } else if (id == BLOCK_TYPE_ID_CCB) {
    return is_ccb_block(node_id);
  } else if (id == BLOCK_TYPE_ID_RLB) {
    return is_rlb_block(node_id);
  } else if (id == BLOCK_TYPE_ID_PNB) {
    return is_pnb_block(node_id);
  } else {
    return false;
  }
}

inline std::string id_2_name(node_id_t node_id) {
  std::string name;
  name += is_ccb_block(node_id) ? "C" : "";
  name += is_rlb_block(node_id) ? "R" : "";
  name += is_dsb_block(node_id) ? "D" : "";
  name += is_pnb_block(node_id) ? "P" : "";
  name += "s";
  name += std::to_string(TO_RG_ID(node_id));
  name += "a";
  name += std::to_string(TO_AZ_ID(node_id));
  return name;
}

typedef std::map<uint32_t, std::vector<uint32_t>> rg2wid_map_t;
typedef std::map<uint32_t, uint32_t> wid2rg_map_t;

inline std::string tuple_id_2_binary(tuple_id_t id) {
  return std::string((const char *)(&id), sizeof(id));
}

inline tuple_id_t binary_2_tuple_id(const std::string &b) {
  return *((tuple_id_t *)b.c_str());
}



