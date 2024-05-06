#pragma once

#include <functional>
#include <list>
#include <memory>
#include <vector>

namespace xodb::plan {
class Plan;
}

namespace xodb::execution {

class Executor;
class ExecutionContext;

/**
 * Pipeline contains set of operators reprensented as executors that must be executed in serial order
 * Implement push-based approach, which means operators executed from leaf nodes propergating to their parents
 * Most leaf plan is at front of `plans`, data is pushed along the way
 */
class Pipeline {
  using ExecutorList = std::vector<std::unique_ptr<Executor>>;
  using PlanList = std::list<std::shared_ptr<plan::Plan>>;
  using Task = std::function<void(void)>;

 public:
  /**
   *
   * @param plans must be ordered that the first index of plans is the one must be executed first, and so on
   */
  explicit Pipeline(PlanList plans, std::shared_ptr<ExecutionContext> context);

  std::vector<Task> CompileTaskList();

 private:
  std::vector<PlanList> GenerateSubplans();

  Task CreateSubtask(const PlanList &subplan) const;

  ExecutorList GenerateExecutors(const PlanList &subplan) const;

  PlanList original_plan_;
  std::shared_ptr<ExecutionContext> context_;
};

}  // namespace xodb::execution
