#include "execution/executors/sequential_scan_executor.h"
#include <common/macros.h>
#include "data_model/table.h"
#include "execution/execution_context.h"
#include "plan/sequential_scan_plan.h"

namespace xodb::execution {

SequentialScanExecutor::SequentialScanExecutor(const std::shared_ptr<PrimitiveRepository> &primitive_repository,
                                               const std::shared_ptr<ExecutionContext> &context,
                                               std::shared_ptr<data_model::TableMetaList> table_meta_infos,
                                               std::shared_ptr<plan::SequentialScanPlan> plan)
    : AbstractExecutor(primitive_repository, context, std::move(table_meta_infos)), plan_(std::move(plan)) {}

void SequentialScanExecutor::Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const {
  const auto &table_name = plan_->table_name_;
  XODB_ASSERT(table_meta_infos_->map.contains(table_name), "must have meta data info for the table");

  auto *io_service_proxy = context_->GetIOServiceProxy();
  XODB_ASSERT(io_service_proxy != nullptr, "");

  std::shared_ptr<arrow::Table> table_data = io_service_proxy->ReadTable(table_name, plan_->output_col_indexes_);
  if (table_data == nullptr) {
    // TODO(nhat): throw an exception here
    //    throw
  }

  auto predicate_infos = plan_->GetPredicateInfos();
  auto in_copy = in;

  for (size_t pred_index = 0; const auto &pred : predicate_infos) {
    std::shared_ptr<arrow::ChunkedArray> column_field = table_data->GetColumnByName(pred.column_name);
    if (column_field == nullptr) {
      // TODO(nhat): throw an exception here
    }

    if (!ApplyPredicate(in_copy, out, column_field, std::move(pred.function))) {
      //      // TODO(nhat): throw an exception here
    }

    if (pred_index < predicate_infos.size()) {
      std::swap(in_copy, out);
      out.Clear();
    }
    pred_index++;
  };
}

bool SequentialScanExecutor::ApplyPredicate(const data_model::TableIndex &in, data_model::TableIndex &out,
                                            const std::shared_ptr<arrow::ChunkedArray> &column,
                                            plan::PredicateFunction pred) {

  return true;
}

}  // namespace xodb::execution