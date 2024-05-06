#pragma once

#include <memory>
#include "execution/executors/executor.h"

namespace xodb::execution {

class FilterPlan;

class FilterExecutor : public Executor {
 public:
  void Execute() const override;

 private:
  std::shared_ptr<FilterPlan> plan_;
};

}  // namespace xodb::execution
