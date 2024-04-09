#pragma once

#include "storage/s3_file_loader.h"

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

class MockS3Loader : public S3FileLoader {
 public:
  MockS3Loader(std::shared_ptr<arrow::fs::FileSystem> root) : root_(std::move(root)) {}

  bool FetchFile(const std::string &file_name, ParquetFile *file) override {
    got_called += 1;
    return FetchFileImpl(file_name, file) == arrow::Status::OK();
  }

  int GetCallNumber() const { return got_called; }

 private:
  arrow::Status FetchFileImpl(const std::string &filename, ParquetFile *file) {
    XODB_ASSERT(file != nullptr, "");

    ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(filename));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

    *file = ParquetFile{std::move(table), filename};

    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::fs::FileSystem> root_;
  int got_called{0};
};

}