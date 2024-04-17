#include "service/io_service.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/arrow_file_util.h"
#include "fmt/format.h"

namespace xodb {

IOService::IOService(std::shared_ptr<arrow::fs::FileSystem> root,
                     std::unique_ptr<BufferPoolManager> buffer_pool_manager)
    : root_(std::move(root)), buffer_pool_manager_(std::move(buffer_pool_manager)) {
  XODB_ASSERT(buffer_pool_manager_ != nullptr, "");
}

std::shared_ptr<::arrow::Table> IOService::ReadTable(const std::string &table_name) {
  std::unique_lock table_to_files_latch(mu_);
  if (!table_to_files_.contains(table_name)) {
    // TODO(nhat): get files info from Catalog, load it from S3 server
    throw NotImplementedException("load from Catalog and S3");
  }

  const auto &file_list = table_to_files_[table_name];
  table_to_files_latch.unlock();

  std::vector<std::shared_ptr<arrow::Table>> tables_read;
  tables_read.reserve(file_list.size());

  if (RetrieveFiles(file_list, {}, &tables_read) != arrow::Status::OK()) {
    return nullptr;
  }

  return arrow::ConcatenateTables(tables_read).ValueOrDie();
}

std::shared_ptr<::arrow::Table> IOService::ReadTable(const std::string &table_name,
                                                     const std::vector<std::string> &columns) {
  if (!table_to_files_.contains(std::string(table_name))) {
    // TODO(nhat): get files info from Catalog, load it from S3 server
    throw NotImplementedException("load from Catalog and S3");
  }

  auto full_table = ReadTable(table_name);
  auto schema = full_table->schema();
  std::vector<int> indices;

  for (const auto &col : columns) {
    auto index = schema->GetFieldIndex(col);
    if (index == -1) {
      std::cout << "Not found column " << col << " for table " << table_name << std::endl;
      return nullptr;
    }
    indices.push_back(index);
  }

  arrow::Result<std::shared_ptr<arrow::Table>> table_result = full_table->SelectColumns(indices);
  if (table_result.status() != arrow::Status::OK()) {
    return nullptr;
  }

  return table_result.ValueOrDie();
}

arrow::Status IOService::RetrieveFiles(const std::vector<std::string> &file_list,
                                       std::optional<std::vector<int>> indices,
                                       std::vector<std::shared_ptr<arrow::Table>> *out) {
  ParquetFile file;
  bool select_colums = indices.has_value();

  for (const auto &filename : file_list) {
    if (!buffer_pool_manager_->FetchFile(filename, &file)) {
      return arrow::Status::Invalid(fmt::format("couldn't load file {}", filename));
    }

    std::shared_ptr<arrow::Table> table;
    if (select_colums) {
      table = file.GetTable(*indices);
    } else {
      table = file.GetTable();
    }

    if (table == nullptr) {
      return arrow::Status::Invalid(fmt::format("couldn't load file {} with indices", filename));
    }

    out->push_back(std::move(table));
  }

  return arrow::Status::OK();
}

arrow::Status IOService::Recover() {
  std::vector<std::string> tables;
  ARROW_RETURN_NOT_OK(FileUtil::ListAllDirectory(root_, tables));

  for (const auto &table : tables) {
    auto table_root = std::make_shared<arrow::fs::SubTreeFileSystem>(table, root_);
    std::vector<std::string> filenames;
    ARROW_RETURN_NOT_OK(FileUtil::ListAllFiles(table_root, PARQUET, filenames, true, {}, table + "/"));
    table_to_files_[table] = std::move(filenames);
  }

  return arrow::Status();
}

}  // namespace xodb
