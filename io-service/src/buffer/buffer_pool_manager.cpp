#include "buffer/buffer_pool_manager.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, std::unique_ptr<FileLoader> file_loader)
    : size_(size), file_loader_(std::move(file_loader)) {
  files_ = new File[size_];
}

BufferPoolManager::~BufferPoolManager() { delete[] files_; }

std::unique_ptr<File> BufferPoolManager::FetchFile(xodb::file_id_t page_id) { return std::unique_ptr<File>(); }

}  // namespace xodb
