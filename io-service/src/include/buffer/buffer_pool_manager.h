#pragma once

#include <memory>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"
#include "storage/file_loader.h"
#include "storage/parquet_file.h"

namespace xodb {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t size, FileLoader *file_loader, std::unique_ptr<Replacer<file_id_t>> replacer);

  ~BufferPoolManager();

   bool FetchFile(file_id_t file_id, ParquetFile *file);

 private:
  size_t size_{0};
  ParquetFile *files_{nullptr};
  [[maybe_unused]] FileLoader *file_loader_{nullptr};
  std::unique_ptr<Replacer<file_id_t>> replacer_;
  std::unordered_map<file_id_t, frame_id_t> file_table_;
};

}  // namespace xodb