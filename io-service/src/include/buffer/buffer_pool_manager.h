#pragma once

#include <list>
#include <memory>
#include <unordered_map>
#include "buffer/lru_replacer.h"
#include "common/config.h"
#include "storage/file_loader.h"
#include "storage/parquet_file.h"

namespace xodb {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t size, FileLoader *file_loader, std::unique_ptr<LRUReplacer<file_id_t>> replacer);

  ~BufferPoolManager();

  bool FetchFile(file_id_t file_id, ParquetFile *file);

 private:
  std::optional<frame_id_t> GetFrame(bool &evicted) {
    std::optional<frame_id_t> frame_id;

    if (!available_frames_.empty()) {
      frame_id = available_frames_.front();
      available_frames_.pop_front();
      evicted = false;
      return frame_id;
    }

    frame_id = 0;
    XODB_ENSURE(replacer_->Full(), "integrity check");
    XODB_ENSURE(replacer_->Evict(&(*frame_id)), "");
    evicted = true;
    return frame_id;
  }

  size_t size_{0};
  std::vector<ParquetFile> files_;
  FileLoader *file_loader_{nullptr};
  std::unique_ptr<LRUReplacer<frame_id_t>> replacer_;
  std::unordered_map<file_id_t, frame_id_t> file_table_;
  std::list<frame_id_t> available_frames_;
};

}  // namespace xodb