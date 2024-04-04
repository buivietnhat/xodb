#include <arrow/api.h>
#include "storage/local_disk_file_loader.h"

namespace xodb {

LocalDiskFileLoader::LocalDiskFileLoader(size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                                         std::shared_ptr<arrow::fs::FileSystem> root)
    : FilePoolManager(max_size, std::move(replacer)), root_(std::move(root)) {
  XODB_ASSERT(root_ != nullptr, "root fs must not be null");

  file_names_.resize(max_size, "");
}

void LocalDiskFileLoader::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_, "");
  XODB_ASSERT(file != nullptr, "");

  auto filename = file_names_[frame_id];
  XODB_ENSURE(LoadFile(filename, file) == arrow::Status::OK(), "must be success");
}

bool LocalDiskFileLoader::SeekFileAndUpdateCache(const std::string &file_name, frame_id_t frame_id, ParquetFile *file) {
  // TODO(nhat) : fetch from S3
  return false;
}

std::optional<std::string> LocalDiskFileLoader::GetFileNameOfFrame(frame_id_t frame_id) const {
  XODB_ASSERT(frame_id < size_, "");
  return file_names_[frame_id];
}

void LocalDiskFileLoader::RemoveFrame(frame_id_t frame_id) {
  auto file_name = file_names_[frame_id];
  XODB_ENSURE(root_->DeleteFile(file_name) == arrow::Status::OK(), "");
}

arrow::Status LocalDiskFileLoader::LoadFile(const std::string &filename, ParquetFile *file) {
  ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(filename));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  *file = ParquetFile{std::move(table), filename};

  return arrow::Status::OK();
}

}  // namespace xodb