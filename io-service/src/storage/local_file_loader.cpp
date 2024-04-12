#include <arrow/api.h>
#include "storage/local_disk_file_loader.h"
#include "common/util/arrow_file_util.h"

namespace xodb {

LocalDiskFileLoader::LocalDiskFileLoader(size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                                         std::shared_ptr<arrow::fs::FileSystem> root,
                                         std::unique_ptr<S3FileLoader> s3_file_loader)
    : FilePoolManager(max_size, std::move(replacer)), root_(std::move(root)), s3_loader_(std::move(s3_file_loader)) {
  XODB_ASSERT(root_ != nullptr, "root fs must not be null");
  XODB_ASSERT(s3_loader_ != nullptr, "s3 loader must not be null");

  file_names_.resize(max_size, "");

  XODB_ASSERT(WarmUp() == arrow::Status::OK(), "must success");
}

void LocalDiskFileLoader::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_, "");
  XODB_ASSERT(file != nullptr, "");

  auto filename = file_names_[frame_id];
  XODB_ENSURE(LoadFile(filename, file) == arrow::Status::OK(), "must be success");
}

std::optional<std::string> LocalDiskFileLoader::GetFileNameOfFrame(frame_id_t frame_id) const {
  XODB_ASSERT(frame_id < size_, "");
  return file_names_[frame_id];
}

void LocalDiskFileLoader::RemoveFrame(frame_id_t frame_id) {
  XODB_ASSERT(frame_id < size_, "");
  auto file_name = file_names_[frame_id];
  XODB_ENSURE(root_->DeleteFile(file_name) == arrow::Status::OK(), "");
}

arrow::Status LocalDiskFileLoader::LoadFile(const std::string &filename, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "");

  ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(filename));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  *file = ParquetFile{std::move(table), filename};

  return arrow::Status::OK();
}

bool LocalDiskFileLoader::SeekFile(const std::string &file_name, ParquetFile *file) {
  return s3_loader_->FetchFile(file_name, file);
}

void LocalDiskFileLoader::UpdateCache(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_, "");
  XODB_ASSERT(file->Valid(), "file must be valid");

  auto file_name = file->GetFileName();
  file_names_[frame_id] = file_name;

  XODB_ENSURE(CreateFile(file_name, file->GetTable()) == arrow::Status::OK(), "must be success");
}

arrow::Status LocalDiskFileLoader::WarmUp() {
  std::vector<std::string> filenames;
  ARROW_RETURN_NOT_OK(FileUtil::ListAllFiles(root_, "parquet", filenames, true));

  for (const auto &filename : filenames) {
    auto frame_id = AddFileToPool(filename);
    file_names_[frame_id] = filename;
    std::cout << "add file " << filename << " to the cache" << std::endl;
  }


  return arrow::Status::OK();
}

arrow::Status LocalDiskFileLoader::CreateFile(const std::string &file_name, std::shared_ptr<arrow::Table> table) const {
  ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_name));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  ARROW_RETURN_NOT_OK(sink->Close());
  return arrow::Status::OK();
}

}  // namespace xodb