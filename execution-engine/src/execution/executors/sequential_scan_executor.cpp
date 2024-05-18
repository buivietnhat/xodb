#include "execution/executors/sequential_scan_executor.h"
#include <common/macros.h>
#include "common/exception.h"
#include "data_model/table.h"
#include "execution/execution_context.h"
#include "execution/primitive_repository.h"
#include "fmt/format.h"
#include "plan/sequential_scan_plan.h"

namespace xodb::execution {

SequentialScanExecutor::SequentialScanExecutor(const std::shared_ptr<ExecutionContext> &context,
                                               std::shared_ptr<data_model::TableMetaList> table_meta_infos,
                                               std::shared_ptr<plan::SequentialScanPlan> plan)
    : AbstractExecutor(context, std::move(table_meta_infos)), plan_(std::move(plan)) {}

void SequentialScanExecutor::Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const {
  const auto &table_name = plan_->table_name_;
  XODB_ASSERT(table_meta_infos_->map.contains(table_name), "must have meta data info for the table");

  auto *io_service_proxy = context_->GetIOServiceProxy();
  XODB_ASSERT(io_service_proxy != nullptr, "");

  std::shared_ptr<arrow::Table> table_data =
      io_service_proxy->ReadTable(table_name, plan_->output_col_indexes_, table_meta_infos_->map.at(table_name));
  if (table_data == nullptr) {
    throw EXECUTION_EXCEPTION(fmt::format("sequential scan: cannot read table {}", table_name.c_str()));
  }

  auto predicate_infos = plan_->GetPredicateInfos();
  auto in_copy = in;

  for (size_t pred_index = 0; const auto &pred : predicate_infos) {
    XODB_ASSERT(pred.val != nullptr, "value for predicate must not be null");

    std::shared_ptr<arrow::ChunkedArray> column_field = table_data->GetColumnByName(pred.column_name);
    if (column_field == nullptr) {
      throw EXECUTION_EXCEPTION(fmt::format("sequential scan: cannot read column {} of table {}",
                                            pred.column_name.c_str(), table_name.c_str()));
    }

    auto output_size = ApplyPredicate(in_copy, out, column_field, pred.val, std::move(pred.function));
    if (output_size == 0) {
      return;
    }

    if (pred_index < predicate_infos.size() - 1) {
      std::swap(in_copy, out);
      out.Clear();
    }

    pred_index++;
  };
}

size_t SequentialScanExecutor::ApplyPredicate(const data_model::TableIndex &in, data_model::TableIndex &out,
                                              const std::shared_ptr<arrow::ChunkedArray> &column, void *val,
                                              plan::PredicateFunction pred) {
  return pred.func(column, in.indexes, val, out.indexes);
}

}  // namespace xodb::execution