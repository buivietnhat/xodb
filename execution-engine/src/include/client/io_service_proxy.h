#pragma once

#include <arrow/api.h>

namespace xodb {

namespace data_model {
struct ColumnIndex;
}

class IOServiceProxy {
 public:
  virtual std::shared_ptr<arrow::Table> ReadTable(
      const std::string &table_name, const std::optional<data_model::ColumnIndex> &column_indexes) const = 0;
};

}  // namespace xodb
