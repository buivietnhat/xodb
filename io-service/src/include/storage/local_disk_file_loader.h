#pragma once

#include <arrow/filesystem/filesystem.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "object_pool/file_pool_manager.h"
#include "storage/parquet_file.h"
#include "storage/s3_file_loader.h"

namespace xodb {

class LocalDiskFileLoader : public FilePoolManager {
 public:
  LocalDiskFileLoader(size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                      std::shared_ptr<arrow::fs::FileSystem> root, std::unique_ptr<S3FileLoader> s3_file_loader);

  DISALLOW_COPY_AND_MOVE(LocalDiskFileLoader);

  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override;

  arrow::Status LoadFile(const std::string &filename, ParquetFile *file);

  // warm up cache, incase after restart
  arrow::Status WarmUp();

 private:
  arrow::Status CreateFile(const std::string &file_name, std::shared_ptr<arrow::Table> table) const;

  bool SeekFile(const std::string &file_name, ParquetFile *file) override;

  void UpdateCache(frame_id_t frame_id, ParquetFile *file) override;

  std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const override;

  void RemoveFrame(frame_id_t frame_id) override;

  std::vector<std::string> file_names_;
  std::shared_ptr<arrow::fs::FileSystem> root_;
  std::unique_ptr<S3FileLoader> s3_loader_;
};

}  // namespace xodb
