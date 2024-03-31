
#pragma once

#include <cassert>
#include <exception>
#include <iostream>
#include <stdexcept>

namespace xodb {

#define XODB_ASSERT(expr, message) assert((expr) && (message))

#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define XODB_ENSURE(expr, message)                    \
  if (!(expr)) {                                      \
    std::cerr << "ERROR: " << (message) << std::endl; \
    std::terminate();                                 \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

//===----------------------------------------------------------------------===//
// Scope guard
//===----------------------------------------------------------------------===//
#define CONCAT2(x, y) x##y
#define CONCAT(x, y) CONCAT2(x, y)
#ifdef __COUNTER__
#define ANON_VAR(x) CONCAT(x, __COUNTER__)
#else
#define ANON_VAR(x) CONCAT(x, __LINE__)
#endif

#define ON_SCOPE_EXIT auto ANON_VAR(SCOPE_EXIT_STATE) = ScopeGuardOnExit() + [&]()

#define ON_SCOPE_EXIT_ROLLBACK(NAME) auto NAME = ScopeGuardOnExit() + [&]()

class ScopeGuardBase {
 public:
  ScopeGuardBase() {}
  auto operator=(const ScopeGuardBase &other) -> ScopeGuardBase & = delete;
};

template <typename Func>
class ScopeGuard : public ScopeGuardBase {
 public:
  ScopeGuard(Func &&func) : func_(func) {}

  ScopeGuard(const Func &func) : func_(func) {}

  ScopeGuard(ScopeGuard &&other) : ScopeGuardBase(std::move(other)), func_(other.func_) {}

  ~ScopeGuard() { func_(); }

 private:
  Func func_;
};

// this trick is for c++14, in c++17 we can call the constructor directly
template <typename Func>
auto MakeGuard(Func &&func) -> ScopeGuard<Func> {
  return ScopeGuard<Func>(std::forward<Func>(func));
}

struct ScopeGuardOnExit {};

template <typename Func>
ScopeGuard<Func> operator+(ScopeGuardOnExit, Func &&func) {
  return ScopeGuard<Func>(std::forward<Func>(func));
}

}  // namespace xodb
