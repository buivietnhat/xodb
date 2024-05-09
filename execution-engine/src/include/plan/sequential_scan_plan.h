#pragma once

#include <type_traits>
#include "data_model/table.h"
#include "plan/abstract_plan.h"

namespace xodb::plan {

struct PredicateFunction {
   std::add_pointer<size_t(size_t, void *input_col, void *val, void *res)>::type func;
};

class SequentialScanPlan : public AbstractPlan {
  friend class execution::SequentialScanExecutor;

  struct PredicateInfo {
    std::string column_name;
    PredicateFunction function;
  };

 public:
  SequentialScanPlan(const std::vector<PredicateInfo> &predicate_infos, std::string table_name,
                     std::optional<data_model::ColumnIndex> output_col_indexes = {})
      : AbstractPlan(PlanType::FILTER),
        predicate_infos_(predicate_infos),
        table_name_(std::move(table_name)),
        output_col_indexes_(std::move(output_col_indexes)) {}

  SequentialScanPlan(const Cardinality &cardinality, const std::vector<PredicateInfo> &predicate_infos,
                     std::string table_name, std::optional<data_model::ColumnIndex> output_col_indexes = {})
      : AbstractPlan(PlanType::FILTER, cardinality),
        predicate_infos_(predicate_infos),
        table_name_(std::move(table_name)),
        output_col_indexes_(std::move(output_col_indexes)) {}

  std::shared_ptr<AbstractPlan> CreateSubPlan(Cardinality cardinality) const override {
    return std::make_shared<SequentialScanPlan>(cardinality, predicate_infos_, table_name_);
  }

  const std::vector<PredicateInfo> &GetPredicateInfos() const { return predicate_infos_; }

 private:
  std::vector<PredicateInfo> predicate_infos_;
  std::string table_name_;
  std::optional<data_model::ColumnIndex> output_col_indexes_;
};

}  // namespace xodb::plan
