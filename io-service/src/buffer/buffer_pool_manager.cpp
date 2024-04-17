#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, std::unique_ptr<LocalDiskFileLoader> file_loader,
                                     std::unique_ptr<LRUReplacer<frame_id_t>> replacer)
    : FilePoolManager(size, std::move(replacer)), file_loader_(std::move(file_loader)) {
  XODB_ASSERT(file_loader_ != nullptr, "file loader must not be null");
  files_.resize(size);
}

void BufferPoolManager::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  std::lock_guard files_latch(mu_);
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");
  XODB_ASSERT(file != nullptr, "");
  *file = files_[frame_id];
}

std::optional<std::string> BufferPoolManager::GetFileNameOfFrame(frame_id_t frame_id) const {
  std::lock_guard files_latch(mu_);
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");

  return files_[frame_id].GetFileName();
}

void BufferPoolManager::RemoveFrame(frame_id_t frame_id) {
  std::lock_guard files_latch(mu_);
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");
  files_[frame_id].Invalidate();
}

bool BufferPoolManager::SeekFile(const std::string &file_name, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "");

  return file_loader_->FetchFile(file_name, file);
}

void BufferPoolManager::UpdateCache(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "");
  std::lock_guard files_latch(mu_);
  files_[frame_id] = *file;
}

}  // namespace xodb
