#pragma once

#include <arrow/chunked_array.h>
#include <memory>
#include "execution/executors/abstract_executor.h"

namespace xodb::plan {
class SequentialScanPlan;
struct PredicateFunction;
}  // namespace xodb::plan

namespace xodb::data_model {
struct TableMetaList;
}

namespace xodb::execution {

class SequentialScanExecutor : public AbstractExecutor {
 public:
  SequentialScanExecutor(const std::shared_ptr<PrimitiveRepository> &primitive_repository,
                         const std::shared_ptr<ExecutionContext> &context,
                         std::shared_ptr<data_model::TableMetaList> table_meta_infos,
                         std::shared_ptr<plan::SequentialScanPlan> plan);

  void Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const override;

 private:
  static size_t ApplyPredicate(const data_model::TableIndex &in, data_model::TableIndex &out,
                             const std::shared_ptr<arrow::ChunkedArray> &column, void *val,
                             plan::PredicateFunction pred);

  std::shared_ptr<plan::SequentialScanPlan> plan_;
};

}  // namespace xodb::execution
