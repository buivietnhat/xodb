#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "common/config.h"
#include "object_pool/file_pool_manager.h"
#include "object_pool/lru_replacer.h"
#include "storage/local_disk_file_loader.h"
#include "storage/parquet_file.h"

namespace xodb {

class BufferPoolManager : public FilePoolManager {
 public:
  BufferPoolManager(size_t size, std::unique_ptr<LocalDiskFileLoader> file_loader,
                    std::unique_ptr<LRUReplacer<frame_id_t>> replacer);

  DISALLOW_COPY_AND_MOVE(BufferPoolManager);

 private:
  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override;

  bool SeekFile(const std::string &file_name, ParquetFile *file) override;

  void UpdateCache(frame_id_t frame_id, ParquetFile *file) override;

  std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const override;

  void RemoveFrame(frame_id_t frame_id) override;

  std::vector<ParquetFile> files_;
  mutable std::mutex mu_;  // protect files_;

  std::unique_ptr<LocalDiskFileLoader> file_loader_{nullptr};
};

}  // namespace xodb