#include "common/db_type.h"

db_type global_db_type = DB_S;

template<>
enum_strings<db_type>::e2s_t enum_strings<db_type>::enum2str = {
    {DB_S, "db-s"},
    {DB_SN, "db-sn"},
    {DB_D, "db-d"},
    {DB_SCR, "db-scr"},
    {DB_TK, "db-tk"},
};
