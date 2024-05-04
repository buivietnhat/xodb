#pragma once

#include <memory>
#include "execution/executors/aggregate_executor.h"
#include "execution/executors/build_phase_hash_join_executor.h"
#include "execution/executors/executor.h"
#include "execution/executors/filter_executor.h"
#include "execution/executors/probe_phase_hash_join_executor.h"
#include "execution/executors/sequence_scan_executor.h"
#include "plan/plan.h"

namespace xodb::execution {

class ExecutorFactory {
 public:
  ExecutorFactory() = delete;

  static std::unique_ptr<Executor> CreateExecutor(std::shared_ptr<ExecutionContext> context,
                                                  std::shared_ptr<plan::Plan> plan) {
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
