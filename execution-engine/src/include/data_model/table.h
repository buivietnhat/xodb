#pragma once

#include <arrow/api.h>
#include "data_model/schema.h"

namespace xodb::data_model {

struct ColumnIndex {
  std::vector<size_t> indexes;
};

struct TableIndex {
  std::vector<size_t> indexes;
  void Clear() {
    indexes.clear();
  }
};

struct TableMetaData {
  std::string table_name;
  Schema schema;
  std::string s3_location;
};

struct TableMetaList {
  std::unordered_map<std::string, TableMetaData> map;
};

class Table {
 public:
 private:
  std::shared_ptr<arrow::Table> table_;
  TableMetaData meta_data_;
};
}  // namespace xodb::data_model