#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/flight/api.h>
#include "buffer/buffer_pool_manager.h"

namespace xodb {

class IOService {
 public:
  explicit IOService(std::shared_ptr<arrow::fs::FileSystem> root, std::unique_ptr<BufferPoolManager> buffer_pool_manager);

  std::shared_ptr<::arrow::Table> ReadTable(const std::string &table_name);

  std::shared_ptr<::arrow::Table> ReadTable(const std::string &table_name, const std::vector<std::string> &columns);

  // the service might be just restarted, retrieve all the table information
  arrow::Status Recover();

  const std::unordered_map<std::string, std::vector<std::string>> &GetTableInfos() const { return table_to_files_; }

 private:
  arrow::Status RetrieveFiles(const std::vector<std::string> &file_list, std::optional<std::vector<int>> indices,
                              std::vector<std::shared_ptr<arrow::Table>> *out);

  std::shared_ptr<arrow::fs::FileSystem> root_;

  std::unique_ptr<BufferPoolManager> buffer_pool_manager_{nullptr};

  // map table_name -> list of file corresponding to that table
  std::unordered_map<std::string, std::vector<std::string>> table_to_files_;
  mutable std::mutex mu_; // protect table_to_files
};

}  // namespace xodb
