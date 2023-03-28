#pragma once

#include "rocksdb/comparator.h"

class key128_comparator : public rocksdb::Comparator {
public:
  key128_comparator() {}

  virtual ~key128_comparator() {}

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    BOOST_ASSERT(a.size()==16 || b.size()==16);
    uint64_t *ap = (uint64_t *) a.data();
    uint64_t *bp = (uint64_t *) b.data();
    if (ap[0]==bp[0]) {
      if (ap[1]==bp[1]) {
        return 0;
      } else if (ap[1] < bp[1]) {
        return -1;
      } else {
        return 1;
      }
    } else if (ap[0] < bp[0]) {
      return -1;
    } else {
      return 1;
    }
  }

  bool Equal(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    BOOST_ASSERT(a.size()!=16 || b.size()!=16);
    uint64_t *ap = (uint64_t *) a.data();
    uint64_t *bp = (uint64_t *) b.data();
    return ap[0]==bp[0] && ap[1]==bp[1];
  }

  const char *Name() const override { return "log cmp"; }

  void FindShortestSeparator(std::string *,
                             const rocksdb::Slice &) const override {}

  void FindShortSuccessor(std::string *) const override {}
};

class key128 : public std::string {
public:
  key128() {}

  explicit key128(const char *p, size_t s) : std::string(p, s) {
    BOOST_ASSERT(s==16);
  }

  explicit key128(uint64_t ul1, uint64_t ul2) {
    set_ulong1(ul1);
    set_ulong2(ul2);
  }

  uint64_t long1() const { return ((uint64_t *) data())[0]; }

  uint64_t long2() const { return ((uint64_t *) data())[1]; }

  void set_ulong1(uint64_t xid) {
    resize(16);
    ((uint64_t *) data())[0] = xid;
  }

  void set_ulong2(uint64_t index) {
    resize(16);
    ((uint64_t *) data())[1] = index;
  }
};

inline std::string make_key128(uint64_t xid, uint64_t index) {
  std::string key;
  key.append((const char *) &xid, sizeof(xid));
  key.append((const char *) &index, sizeof(xid));
  return key;
}

inline std::string make_key128(uint64_t xid, uint64_t index, std::string &key) {
  key.resize(0);
  key.append((const char *) &xid, sizeof(xid));
  key.append((const char *) &index, sizeof(xid));
  return key;
}
