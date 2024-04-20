#pragma once

#include "storage/s3_file_loader.h"

namespace xodb {

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

}  // namespace xodb