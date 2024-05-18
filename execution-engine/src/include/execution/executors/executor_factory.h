#pragma once

#include <memory>
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregate_executor.h"
#include "execution/executors/build_phase_hash_join_executor.h"
#include "execution/executors/probe_phase_hash_join_executor.h"
#include "execution/executors/sequential_scan_executor.h"
#include "plan/abstract_plan.h"

namespace xodb::execution {

class ExecutorFactory {
 public:
  ExecutorFactory() = delete;

  static std::unique_ptr<AbstractExecutor> CreateExecutor(std::shared_ptr<ExecutionContext> context,
                                                          std::shared_ptr<plan::AbstractPlan> plan) {
    switch (plan->GetType()) {
      case plan::PlanType::SEQUENCE_SCAN:
        return nullptr;
      case plan::PlanType::AGGREGATE:
        break;
      case plan::PlanType::FILTER:
        break;
      case plan::PlanType::PROBE_PHASE_HASH_JOIN:
        break;
      case plan::PlanType::BUILD_PHASE_HASH_JOIN:
        break;
      default:
        return nullptr;
    }
    return nullptr;
  }
};

}  // namespace xodb::execution
