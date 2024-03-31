#pragma once

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/table.h>
#include "common/macros.h"

namespace xodb {

/*
 * This class represent as a PAX file format on memory
 * */
class ParquetFile {
 public:
  std::shared_ptr<arrow::Table> GetTable() const { return table_; }

  std::shared_ptr<arrow::Table> GetTable(const std::vector<int> &indices) const {
    arrow::Result<std::shared_ptr<arrow::Table>> table_result = table_->SelectColumns(indices);
    if (table_result.status() != arrow::Status::OK()) {
      return nullptr;
    }

    return table_result.ValueOrDie();
  }

 private:
  std::shared_ptr<arrow::Table> table_;
};

}  // namespace xodb