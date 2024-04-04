#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, LocalDiskFileLoader *file_loader,
                                     std::unique_ptr<LRUReplacer<frame_id_t>> replacer)
    : FilePoolManager(size, std::move(replacer)), file_loader_(file_loader) {
  XODB_ASSERT(file_loader != nullptr, "file loader must not be null");
  files_.resize(size);
}

void BufferPoolManager::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");
  XODB_ASSERT(file != nullptr, "");
  *file = files_[frame_id];
}

bool BufferPoolManager::SeekFileAndUpdateCache(const std::string &file_name, frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_, "");

  if (!file_loader_->FetchFile(file_name, file)) {
    return false;
  }

  files_[frame_id] = *file;
  return true;
}

std::optional<std::string> BufferPoolManager::GetFileNameOfFrame(frame_id_t frame_id) const {
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");

  return files_[frame_id].GetFileName();
}

void BufferPoolManager::RemoveFrame(frame_id_t frame_id) {
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");
  files_[frame_id].Invalidate();
}

}  // namespace xodb
