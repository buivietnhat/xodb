#pragma once

#include <type_traits>
#include "plan/abstract_plan.h"

namespace xodb::plan {

using PredicateFunction = std::add_pointer<size_t(size_t, void *input_col, void *val, void *res)>::type;

class SequentialScanPlan : public AbstractPlan {
  struct PredicateInfo {
    std::string column_name;
    PredicateFunction function;
  };

 public:
  SequentialScanPlan(const std::vector<PredicateInfo> &predicate_infos, std::string table_name)
      : AbstractPlan(PlanType::FILTER), predicate_infos_(predicate_infos), table_name_(std::move(table_name)) {}

  SequentialScanPlan(const Cardinality &cardinality, const std::vector<PredicateInfo> &predicate_infos, std::string table_name)
      : AbstractPlan(PlanType::FILTER, cardinality),
        predicate_infos_(predicate_infos),
        table_name_(std::move(table_name)) {}

  std::shared_ptr<AbstractPlan> CreateSubPlan(Cardinality cardinality) const override {
    return std::make_shared<SequentialScanPlan>(cardinality, predicate_infos_, table_name_);
  }

  const std::vector<PredicateInfo> &GetPredicateInfos() const { return predicate_infos_; }

 private:
  std::vector<PredicateInfo> predicate_infos_;
  std::string table_name_;
};

}  // namespace xodb::plan
