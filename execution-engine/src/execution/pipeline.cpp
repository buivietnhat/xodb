#include "execution/pipeline.h"
#include "execution/executors/executor_factory.h"
#include "plan/plan.h"

namespace xodb::execution {

Pipeline::Pipeline(std::vector<plan::Plan> plans) : plans_(std::move(plans)) {}

void Pipeline::Execute(std::vector<size_t> &index_out) {
  std::vector<size_t> column_indexes_out;
  std::vector<size_t> column_indexes_in;

  // TODO(nhat): executors may throw exceptions, need to handle them here
  //  for (auto &node : executors_) {
  //    column_indexes_in.clear();
  //    std::swap(column_indexes_in, column_indexes_out);

  //    node.Execute(column_indexes_in, column_indexes_out);
  //  }

  index_out = column_indexes_out;
}

}  // namespace xodb::execution