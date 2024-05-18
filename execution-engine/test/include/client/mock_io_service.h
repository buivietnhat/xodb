#pragma once
#include "client/io_service_proxy.h"

namespace xodb {

class MockIoService : public IOServiceProxy {
 public:
  explicit MockIoService(std::shared_ptr<arrow::Table> sample_table) : sample_table_(std::move(sample_table)) {}

  std::shared_ptr<arrow::Table> ReadTable(const std::string &table_name,
                                          const std::optional<data_model::ColumnIndex> &column_indexes,
                                          const data_model::TableMetaData &meta_data) const override {
    if (table_name != "sample_table") {
      return nullptr;
    }

    return sample_table_;
  }

 private:
  std::shared_ptr<arrow::Table> sample_table_;
};

}  // namespace xodb
