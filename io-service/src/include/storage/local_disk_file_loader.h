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

 private:
  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override;

  bool SeekFileAndUpdateCache(file_id_t file_id, frame_id_t frame_id, ParquetFile *file) override;

 private:
  std::vector<std::string> filenames_;
  std::shared_ptr<arrow::fs::FileSystem> root_;
};

}  // namespace xodb
