#include "execution/executors/sequential_scan_executor.h"
#include <numeric>
#include "data_model/table.h"
#include "execution/executor_test_util.h"
#include "execution/primitive_repository.h"
#include "plan/sequential_scan_plan.h"

namespace xodb::execution {

TEST_F(ExecutionTest, SequentialScanExecutorTest) {
  plan::PredicateInfo int_greater_than_five_predicate;
  int32_t val = 5;
  int_greater_than_five_predicate.column_name = int_column_name_;
  int_greater_than_five_predicate.function = {PrimitiveRepository::Select_GreaterThan_Int32};
  int_greater_than_five_predicate.val = &val;

  std::vector<plan::PredicateInfo> predicates{int_greater_than_five_predicate};

  table_meta_infos_->map.insert({"sample_table", {}});

  auto sequential_scan_plan = std::make_shared<plan::SequentialScanPlan>(predicates, "sample_table");
  auto sequential_scan_executor =
      std::make_shared<SequentialScanExecutor>(context_, table_meta_infos_, std::move(sequential_scan_plan));

  auto num_rows = sample_table_->num_rows();
  data_model::TableIndex in(num_rows);
  std::fill(in.indexes.begin(), in.indexes.end(), 1);

  data_model::TableIndex out(num_rows);

  sequential_scan_executor->Execute(in, out);

  std::vector<int> expected_out{0, 0, 0, 0, 0, 1, 1, 1, 1, 1};
  EXPECT_EQ(expected_out, out.indexes);
}

}  // namespace xodb::execution