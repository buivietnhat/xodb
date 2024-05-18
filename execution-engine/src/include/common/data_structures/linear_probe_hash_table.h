#pragma once

#include "common/hash_util.h"

namespace xodb::common {

template <typename T>
class LinearProbeHashTable {
 public:
  explicit LinearProbeHashTable(size_t size) : max_size_(size) {}


 private:
  size_t max_size_;
  std::vector<T> table_;
};

}
