#pragma once

#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "common/macros.h"

namespace xodb {

/*
 * This class represent as a PAX file format on memory
 * */
class ParquetFile {
 public:
  std::shared_ptr<arrow::Table> GetTable() const { return table_; }

  ParquetFile() = default;

  ParquetFile(std::shared_ptr<arrow::Table> table, std::string file_name)
      : table_(std::move(table)), file_name_(std::move(file_name)) {
    XODB_ASSERT(file_name != "", "file id must be valid");
  }

  std::shared_ptr<arrow::Table> GetTable(const std::vector<int> &indices) const {
    arrow::Result<std::shared_ptr<arrow::Table>> table_result = table_->SelectColumns(indices);
    if (table_result.status() != arrow::Status::OK()) {
      return nullptr;
    }

    return table_result.ValueOrDie();
  }

  bool Valid() const { return file_name_ != ""; }

  const std::string &GetFileName() const { return file_name_; }

  void Invalidate() {
    file_name_ = "";
  }

 private:
  std::shared_ptr<arrow::Table> table_;
  std::string file_name_{""};
};

}  // namespace xodb