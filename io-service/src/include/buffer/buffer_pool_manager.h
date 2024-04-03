#pragma once

#include <list>
#include <memory>
#include <unordered_map>
#include "common/config.h"
#include "object_pool/file_pool_manager.h"
#include "object_pool/lru_replacer.h"
#include "storage/file_loader.h"
#include "storage/parquet_file.h"

namespace xodb {

class BufferPoolManager : public FilePoolManager {
 public:
  BufferPoolManager(size_t size, FileLoader *file_loader, std::unique_ptr<LRUReplacer<file_id_t>> replacer);

 private:
  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override;

  bool SeekFileAndUpdateCache(file_id_t file_id, frame_id_t frame_id, ParquetFile *file) override;

  std::vector<ParquetFile> files_;
  FileLoader *file_loader_{nullptr};
};

}  // namespace xodb