#include "execution/executors/sequential_scan_executor.h"
#include "plan/sequential_scan_plan.h"

namespace xodb::execution {

SequentialScanExecutor::SequentialScanExecutor(const std::shared_ptr<PrimitiveRepository> &primitive_repository,
                               const std::shared_ptr<ExecutionContext> &context, std::shared_ptr<plan::SequentialScanPlan> plan)
    : AbstractExecutor(primitive_repository, context), plan_(std::move(plan)) {
}

void SequentialScanExecutor::Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const {
  auto predicate_infos = plan_->GetPredicateInfos();
}

}  // namespace xodb::execution