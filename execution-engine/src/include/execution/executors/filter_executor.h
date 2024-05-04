#pragma once

#include "execution/executors/executor.h"
#include <memory>

namespace xodb::execution {

class FilterPlan;

class FilterExecutor : public Executor {
 public:
  void Execute() override;

 private:
  std::shared_ptr<FilterPlan> plan_;
};

}

