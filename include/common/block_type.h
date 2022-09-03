#pragma once

#include <string>

typedef std::string block_type_t;

#define CCB "CCB"
#define RLB "RLB"
#define DSB "DSB"
#define PNB "PNB"
#define CLIENT "CLIENT"

const block_type_t BLOCK_CCB = CCB;
const block_type_t BLOCK_RLB = RLB;
const block_type_t BLOCK_DSB = DSB;
const block_type_t BLOCK_PNB = PNB;
const block_type_t BLOCK_CLIENT = CLIENT;

enum block_type_id_t {
  BLOCK_TYPE_ID_CCB = 1,
  BLOCK_TYPE_ID_RLB = 2,
  BLOCK_TYPE_ID_DSB = 3,
  BLOCK_TYPE_ID_PNB = 4,
  BLOCK_TYPE_SIZE = 5,
};

static const std::map<block_type_id_t, block_type_t> __node_type_id_2_name = {
    {BLOCK_TYPE_ID_CCB, BLOCK_CCB},
    {BLOCK_TYPE_ID_RLB, BLOCK_RLB},
    {BLOCK_TYPE_ID_DSB, BLOCK_DSB},
    {BLOCK_TYPE_ID_PNB, BLOCK_PNB},
};

static const std::map<block_type_t, block_type_id_t> __node_type_name_2_id = {
    {BLOCK_CCB, BLOCK_TYPE_ID_CCB},
    {BLOCK_RLB, BLOCK_TYPE_ID_RLB},
    {BLOCK_DSB, BLOCK_TYPE_ID_DSB},
    {BLOCK_PNB, BLOCK_TYPE_ID_PNB}
};

inline block_type_t node_type_id_2_name(block_type_id_t id) {
  auto iter = __node_type_id_2_name.find(id);
  if (iter == __node_type_id_2_name.end()) {
    BOOST_ASSERT(__node_type_id_2_name.contains(id));
    return "";
  } else {
    return iter->second;
  }
}

inline std::string block_type_list_2_string(const std::set<block_type_t> &l) {
  std::string r = "";
  for (block_type_t s : l) {
    if (!s.empty()) {
      r += s[0];
    }
  }
  return r;
}