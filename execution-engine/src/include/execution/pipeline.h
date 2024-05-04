#pragma once

#include <vector>

namespace xodb::plan {
class Plan;
}

namespace xodb::execution {

/**
 * Pipeline contains set of operators reprensented as executors that must be executed in serial order
 * Implement push-based approach, which means operators executed from leaf nodes propergating to their parents
 * Most leaf plan is at front of `plans`, data is pushed along the way
 */
class Pipeline {
 public:
  /**
   *
   * @param plans must be ordered that the first index of plans is the one must be executed first, and so on
   */
  explicit Pipeline(std::vector<plan::Plan> plans);

  void Execute(std::vector<size_t> &index_out);

 private:
  std::vector<plan::Plan> plans_;
};

}  // namespace xodb::execution
