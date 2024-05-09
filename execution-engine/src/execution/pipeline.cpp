#include "execution/pipeline.h"
#include "common/concurrency/worker_pool.h"
#include "common/config.h"
#include "common/macros.h"
#include "data_model/table.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/executor_factory.h"
#include "plan/abstract_plan.h"

namespace xodb::execution {

Pipeline::Pipeline(PlanList plans, std::shared_ptr<ExecutionContext> context,
                   std::shared_ptr<data_model::TableMetaList> table_meta_infos)
    : original_plan_(std::move(plans)), context_(std::move(context)), table_meta_infos_(std::move(table_meta_infos)) {
  XODB_ASSERT(!original_plan_.empty(), "construct pipeline with no plans");
  XODB_ASSERT(context_ != nullptr, "");
  XODB_ASSERT(original_plan_.front()->GetCardinality().has_value(), "the most outter leaf must have cardinality info");
}

auto Pipeline::CompileTaskList() -> std::vector<Task> {
  std::vector<PlanList> subplans = GenerateSubplans();
  std::vector<Task> tasks;
  tasks.reserve(subplans.size());

  for (const auto &subplan : subplans) {
    tasks.push_back(CreateSubtask(subplan));
  }
  return tasks;
}

auto Pipeline::GenerateSubplans() -> std::vector<PlanList> { return std::vector<PlanList>(); }

auto Pipeline::CreateSubtask(const Pipeline::PlanList &subplan) const -> Task {
  auto execution_pipeline = GenerateExecutors(subplan);
  auto task = [pipeline = std::move(execution_pipeline)] {
    data_model::TableIndex in, out;
    for (const auto &executor : *pipeline) {
      std::swap(in, out);
      out.Clear();
      executor->Execute(in, out);
    }

    return out;
  };

  return task;
}

auto Pipeline::GenerateExecutors(const PlanList &subplan) const -> ExecutorList {
  ExecutorList executors = std::make_shared<std::vector<std::unique_ptr<AbstractExecutor>>>();
  executors->reserve(subplan.size());

  for (const auto &plan : subplan) {
    executors->push_back(ExecutorFactory::CreateExecutor(context_, plan));
  }

  return executors;
}

}  // namespace xodb::execution