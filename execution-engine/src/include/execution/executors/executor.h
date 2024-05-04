#pragma once

namespace xodb::execution {
class Executor {
 public:
  virtual void Execute() = 0;
};

}  // namespace xodb::execution
