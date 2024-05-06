#pragma once

#include <future>

namespace xodb::common {

template <typename T>
class Future {
  using Promise = std::promise<T>;

 public:
  static Promise CreatePromise() {
    return {};
  }
};

}  // namespace xodb::common
