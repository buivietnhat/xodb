#pragma once

#include <memory>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"
#include "storage/file.h"
#include "storage/file_loader.h"

namespace xodb {

class BufferPoolManager {
 public:
  BufferPoolManager(size_t size, std::unique_ptr<FileLoader> file_loader);

  ~BufferPoolManager();

  std::unique_ptr<File> FetchFile(file_id_t file_id);

 private:
  size_t size_;
  File *files_;
  std::unique_ptr<FileLoader> file_loader_;
  std::unordered_map<file_id_t, frame_id_t> file_table_;
};

}  // namespace xodb