#pragma once

#include "common/enum_str.h"
#include <string>

#define BUILD_SHARED 1
#define BUILD_SHARE_NOTHING 2
#define BUILD_CALVIN 3
#define BUILD_GEO_REP_OPTIMIZE 4
#define BUILD_TK 5

#ifndef DB_BUILD_TYPE

#define DB_TYPE_SHARED
#define DB_TYPE_SHARE_NOTHING
#define DB_TYPE_CALVIN
#define DB_TYPE_GEO_REP_OPTIMIZE
#define DB_TYPE_TK
#define DB_TYPE_ROCKS
#define DB_TYPE_NON_DETERMINISTIC

#else

#if DB_BUILD_TYPE == BUILD_SHARED
#define DB_TYPE_SHARED
#define DB_TYPE_ROCKS
#elif DB_BUILD_TYPE == BUILD_SHARE_NOTHING
#define DB_TYPE_SHARE_NOTHING
#define DB_TYPE_NON_DETERMINISTIC
#define DB_TYPE_ROCKS
#elif DB_BUILD_TYPE == BUILD_CALVIN
#define DB_TYPE_CALVIN
#define DB_TYPE_ROCKS
#elif DB_BUILD_TYPE == BUILD_GEO_REP_OPTIMIZE
#define DB_TYPE_SHARE_NOTHING
#define DB_TYPE_GEO_REP_OPTIMIZE
#define DB_TYPE_NON_DETERMINISTIC
#define DB_TYPE_ROCKS
#elif DB_BUILD_TYPE == BUILD_TK
#define DB_TYPE_TK
#define DB_TYPE_SHARE_NOTHING
#define DB_TYPE_NON_DETERMINISTIC
//#error "unknown build type...."
#endif

#endif

enum db_type {
  DB_S = BUILD_SHARED,
  DB_SN = BUILD_SHARE_NOTHING,
  DB_D = BUILD_CALVIN,
  DB_GRO = BUILD_GEO_REP_OPTIMIZE,
  DB_TK = BUILD_TK,
};

template<> enum_strings<db_type>::e2s_t enum_strings<db_type>::enum2str;

extern db_type global_db_type;

inline db_type block_db_type() {
#ifdef DB_BUILD_TYPE
  return db_type(DB_BUILD_TYPE);
#else
  return global_db_type;
#endif
}

inline bool is_shared() { return block_db_type()==DB_S; }

inline bool is_shared_nothing() {
  return block_db_type() == DB_SN || block_db_type() == DB_GRO;
}

inline bool is_deterministic() { return block_db_type()==DB_D; }

inline bool is_non_deterministic() { return block_db_type()!=DB_D; }

inline bool is_geo_rep_optimized() {
  return block_db_type() == DB_GRO;
}

inline bool is_store_tk() { return block_db_type()==DB_TK; }

inline void set_block_db_type(db_type t) { global_db_type = t; }
