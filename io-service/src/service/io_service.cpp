#include "service/io_service.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/arrow_file_util.h"
#include "fmt/format.h"

namespace xodb {

IOService::IOService(std::shared_ptr<arrow::fs::FileSystem> root, BufferPoolManager *buffer_pool_manager)
    : root_(std::move(root)), buffer_pool_manager_(buffer_pool_manager) {
  XODB_ASSERT(buffer_pool_manager_ != nullptr, "");
}

std::shared_ptr<::arrow::Table> IOService::ReadTable(const std::string &table_name) {
  if (!table_to_files_.contains(table_name)) {
    // TODO(nhat): get files info from Catalog, load it from S3 server
    throw NotImplementedException("load from Catalog and S3");
  }

  const auto &file_list = table_to_files_[table_name];
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

  //  // first validate the schema
  //  auto schema = schema_[table_name];
  //  std::vector<int> indices;
  //  indices.reserve(columns.size());
  //  for (const auto &col : columns) {
  //    auto index = schema->GetFieldIndex(col);
  //    if (index == -1) {
  //      return arrow::Status::Invalid(fmt::format("couldn't read colum {} of table {}", col, table_name));
  //    }
  //    indices.push_back(index);
  //  }
  //
  //  XODB_ENSURE(table_to_files_.contains(table_name), "it should be");
  //
  //  const auto &file_list = table_to_files_[table_name];
  //  std::vector<std::shared_ptr<arrow::Table>> tables_read;
  //  tables_read.reserve(file_list.size());
  //
  //  // retrieve files with selected indices
  //  ARROW_RETURN_NOT_OK(RetrieveFiles(file_list, indices, &tables_read));
  //
  //  ARROW_ASSIGN_OR_RAISE(*out, arrow::ConcatenateTables(tables_read));

  return nullptr;
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
