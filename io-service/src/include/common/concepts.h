#pragma once

#include <concepts>

namespace xodb {

template <typename T>
concept Hashable = requires(T t) {
  { std::hash<T>{}(t) } -> std::convertible_to<std::size_t>;
};

}  // namespace xodb