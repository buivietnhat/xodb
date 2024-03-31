#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, FileLoader *file_loader,
                                     std::unique_ptr<Replacer<file_id_t>> replacer)
    : size_(size), file_loader_(file_loader), replacer_(std::move(replacer)) {
  XODB_ASSERT(size > 0, "buffer size must greater than zero");
  XODB_ASSERT(file_loader != nullptr, "file loader must not be null");
  XODB_ASSERT(replacer != nullptr, "replacer must not be null");

  files_ = new ParquetFile[size_];
}

BufferPoolManager::~BufferPoolManager() { delete[] files_; }

bool BufferPoolManager::FetchFile(file_id_t file_id, ParquetFile *file) {
  // first to check if the file is already in my cache
  if (file_table_.contains(file_id)) {
    auto frame_id = file_table_[file_id];
    XODB_ENSURE(frame_id < size_, "must be in bound");
  }
  return false;
}

}  // namespace xodb
