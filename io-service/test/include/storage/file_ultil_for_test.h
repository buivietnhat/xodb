#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/flight/api.h>
#include "common/macros.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace xodb {

inline bool AreTablesEqual(const std::shared_ptr<arrow::Table> &table1, const std::shared_ptr<arrow::Table> &table2) {
  // Check if schemas are equal
  if (!table1->schema()->Equals(*table2->schema())) {
    return false;
  }

  // Check if record batches are equal
  if (table1->num_rows() != table2->num_rows()) {
    return false;
  }

  // Iterate over each column
  for (int i = 0; i < table1->num_columns(); ++i) {
    // Get the arrays for each column in both tables
    auto array1 = table1->column(i)->chunk(0);
    auto array2 = table2->column(i)->chunk(0);

    // Check if arrays are equal
    if (!array1->Equals(array2)) {
      return false;
    }
  }

  // Tables are equal
  return true;
}

arrow::Status CreateParquetFileIntString(arrow::fs::FileSystem *fs, const std::string &file_path,
                                         const std::vector<int32_t> &int_data,
                                         const std::vector<std::string> &string_data) {
  XODB_ASSERT(int_data.size() == string_data.size(), "two columns must have equal size");

  ARROW_ASSIGN_OR_RAISE(auto sink, fs->OpenOutputStream(file_path));
  auto int_field = std::make_shared<arrow::Field>("int_column", arrow::int32());
  auto string_field = std::make_shared<arrow::Field>("string_column", arrow::utf8());
  auto schema = arrow::schema({int_field, string_field});

  arrow::Int32Builder int_builder;
  ARROW_RETURN_NOT_OK(int_builder.AppendValues(int_data));

  arrow::StringBuilder string_builder;
  ARROW_RETURN_NOT_OK(string_builder.AppendValues(string_data));

  std::shared_ptr<arrow::Array> int_array, string_array;
  ARROW_ASSIGN_OR_RAISE(int_array, int_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(string_array, string_builder.Finish());

  auto table = arrow::Table::Make(schema, {int_array, string_array});
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  return arrow::Status::OK();
}

arrow::Status CreateParquetFileStringString(arrow::fs::FileSystem *fs, const std::string &file_path,
                                            const std::vector<std::string> &string_data1,
                                            const std::vector<std::string> &string_data2) {
  XODB_ASSERT(string_data1.size() == string_data2.size(), "two columns must have equal size");

  ARROW_ASSIGN_OR_RAISE(auto sink, fs->OpenOutputStream(file_path));
  auto string_field1 = std::make_shared<arrow::Field>("string_column1", arrow::utf8());
  auto string_field2 = std::make_shared<arrow::Field>("string_column2", arrow::utf8());
  auto schema = arrow::schema({string_field1, string_field2});

  arrow::StringBuilder string_builder1;
  ARROW_RETURN_NOT_OK(string_builder1.AppendValues(string_data1));

  arrow::StringBuilder string_builder2;
  ARROW_RETURN_NOT_OK(string_builder2.AppendValues(string_data2));

  std::shared_ptr<arrow::Array> string_array1, string_array2;
  ARROW_ASSIGN_OR_RAISE(string_array1, string_builder1.Finish());
  ARROW_ASSIGN_OR_RAISE(string_array2, string_builder2.Finish());

  auto table = arrow::Table::Make(schema, {string_array1, string_array2});
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  return arrow::Status::OK();
}

arrow::Status CreateParquetFileIntInt(arrow::fs::FileSystem *fs, const std::string &file_path,
                                      const std::vector<int32_t> &int_data1, const std::vector<int32_t> &int_data2) {
  XODB_ASSERT(int_data1.size() == int_data2.size(), "two columns must have equal size");

  ARROW_ASSIGN_OR_RAISE(auto sink, fs->OpenOutputStream(file_path));
  auto int_field1 = std::make_shared<arrow::Field>("int_column1", arrow::int32());
  auto int_field2 = std::make_shared<arrow::Field>("int_column2", arrow::int32());

  auto schema = arrow::schema({int_field1, int_field2});

  arrow::Int32Builder int_builder1;
  ARROW_RETURN_NOT_OK(int_builder1.AppendValues(int_data1));

  arrow::Int32Builder int_builder2;
  ARROW_RETURN_NOT_OK(int_builder2.AppendValues(int_data2));

  std::shared_ptr<arrow::Array> int_array1, int_array2;
  ARROW_ASSIGN_OR_RAISE(int_array1, int_builder1.Finish());
  ARROW_ASSIGN_OR_RAISE(int_array2, int_builder2.Finish());

  auto table = arrow::Table::Make(schema, {int_array1, int_array2});
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  return arrow::Status::OK();
}

}  // namespace xodb