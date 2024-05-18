#pragma once

#include <arrow/api.h>
#include <memory>
#include "client/mock_io_service.h"
#include "common/macros.h"
#include "data_model/table.h"
#include "execution/execution_context.h"
#include "execution/primitive_repository.h"
#include "gtest/gtest.h"

namespace xodb::execution {

template <typename T>
static void PrintPretty(const std::shared_ptr<T> &object) {
  arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
  XODB_ASSERT(arrow::PrettyPrint(*object, print_options, &std::cout) == arrow::Status::OK(), "");
}

class ExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    XODB_ASSERT(arrow::Status::OK() == CreateSampleTable(), "should ok");

    context_ = std::make_shared<ExecutionContext>(std::make_shared<MockIoService>(sample_table_));
    table_meta_infos_ = std::make_shared<data_model::TableMetaList>();
  }

  void TearDown() override { Test::TearDown(); }

  arrow::Status CreateSampleTable() {
    std::vector<int32_t> int_data{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<std::string> string_data{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};

    auto int_field = std::make_shared<arrow::Field>(int_column_name_, arrow::int32());
    auto string_field = std::make_shared<arrow::Field>(str_column_name_, arrow::utf8());
    auto schema = arrow::schema({int_field, string_field});

    arrow::Int32Builder int_builder;
    ARROW_RETURN_NOT_OK(int_builder.AppendValues(int_data));

    arrow::StringBuilder string_builder;
    ARROW_RETURN_NOT_OK(string_builder.AppendValues(string_data));

    std::shared_ptr<arrow::Array> int_array, string_array;
    ARROW_ASSIGN_OR_RAISE(int_array, int_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(string_array, string_builder.Finish());

    sample_table_ = arrow::Table::Make(schema, {int_array, string_array});

    return arrow::Status::OK();
  }

  std::shared_ptr<PrimitiveRepository> primitive_repository_;
  std::shared_ptr<ExecutionContext> context_;
  std::shared_ptr<data_model::TableMetaList> table_meta_infos_;
  std::shared_ptr<arrow::Table> sample_table_;
  const std::string int_column_name_ = "int_column";
  const std::string str_column_name_ = "str_column";
};

}  // namespace xodb::execution
