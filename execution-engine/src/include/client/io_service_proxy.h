#pragma once

#include <arrow/api.h>

namespace xodb {

namespace data_model {
struct ColumnIndex;
struct TableMetaData;
}  // namespace data_model

class IOServiceProxy {
 public:
  virtual std::shared_ptr<arrow::Table> ReadTable(const std::string &table_name,
                                                  const std::optional<data_model::ColumnIndex> &column_indexes,
                                                  const data_model::TableMetaData &meta_data) const = 0;

  virtual ~IOServiceProxy() = default;
};

}  // namespace xodb
