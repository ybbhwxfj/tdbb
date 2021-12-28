#pragma once

class slice {
public:
  // Create an empty slice.
  slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  slice(const char *d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to the contents of "s"
  /* implicit */
  slice(const std::string &s) : data_(s.data()), size_(s.size()) {}



  // Create a slice that refers to s[0,strlen(s)-1]
  /* implicit */
  slice(const char *s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

  // Return a pointer to the beginning of the referenced data
  const char *data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  void remove_suffix(size_t n) {
    assert(n <= size());
    size_ -= n;
  }

  // private: make these public for rocksdbjni access
  const char *data_;
  size_t size_;

  // Intentionally copyable
};