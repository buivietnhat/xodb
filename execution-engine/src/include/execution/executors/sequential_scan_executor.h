#pragma once

#include <memory>
#include "execution/executors/abstract_executor.h"

namespace xodb::plan {
class SequentialScanPlan;
}

namespace xodb::execution {

class SequentialScanExecutor : public AbstractExecutor {
 public:
  SequentialScanExecutor(const std::shared_ptr<PrimitiveRepository> &primitive_repository,
                 const std::shared_ptr<ExecutionContext> &context, std::shared_ptr<plan::SequentialScanPlan> plan);

  void Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const override;

 private:
  std::shared_ptr<plan::SequentialScanPlan> plan_;
};

}  // namespace xodb::execution
