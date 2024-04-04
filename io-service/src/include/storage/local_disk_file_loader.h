#pragma once

#include <arrow/filesystem/filesystem.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "object_pool/file_pool_manager.h"
#include "storage/parquet_file.h"

namespace xodb {

class LocalDiskFileLoader : public FilePoolManager {
 public:
  LocalDiskFileLoader(size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                      std::shared_ptr<arrow::fs::FileSystem> root);

  DISALLOW_COPY_AND_MOVE(LocalDiskFileLoader);

 private:
  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override;

  bool SeekFileAndUpdateCache(const std::string &file_name, frame_id_t frame_id, ParquetFile *file) override;

  std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const override;

  void RemoveFrame(frame_id_t frame_id) override;

  arrow::Status LoadFile(const std::string &filename, ParquetFile *file);

 private:
  std::vector<std::string> file_names_;
  std::shared_ptr<arrow::fs::FileSystem> root_;
};

}  // namespace xodb
