#pragma once

#include <functional>
#include <list>
#include <memory>
#include <vector>

namespace xodb::plan {
class AbstractPlan;
}

namespace xodb::data_model {
struct TableIndex;
struct TableMetaList;
}  // namespace xodb::data_model

namespace xodb::execution {

class AbstractExecutor;
class ExecutionContext;

/**
 * Pipeline contains set of operators reprensented as executors that must be executed in serial order
 * Implement push-based approach, which means operators executed from leaf nodes propergating to their parents
 * Most leaf plan is at front of `plans`, data is pushed along the way
 */
class Pipeline {
  using ExecutorList = std::shared_ptr<std::vector<std::unique_ptr<AbstractExecutor>>>;
  using PlanList = std::list<std::shared_ptr<plan::AbstractPlan>>;
  using Task = std::function<data_model::TableIndex(void)>;

 public:
  /**
   *
   * @param plans must be ordered that the first index of plans is the one must be executed first, and so on
   */
  Pipeline(PlanList plans, std::shared_ptr<ExecutionContext> context,
           std::shared_ptr<data_model::TableMetaList> table_meta_infos);

  std::vector<Task> CompileTaskList();

 private:
  std::vector<PlanList> GenerateSubplans();

  Task CreateSubtask(const PlanList &subplan) const;

  ExecutorList GenerateExecutors(const PlanList &subplan) const;

  PlanList original_plan_;
  std::shared_ptr<ExecutionContext> context_;
  std::shared_ptr<data_model::TableMetaList> table_meta_infos_;
};

}  // namespace xodb::execution
