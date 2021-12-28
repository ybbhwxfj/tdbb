#include "concurrency/lock.h"

template<>
enum_strings<lock_mode>::e2s_t enum_strings<lock_mode>::enum2str = {
    {LOCK_READ_ROW, "lock read"},
    {LOCK_WRITE_ROW, "lock write"},
};
