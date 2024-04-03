#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, FileLoader *file_loader,
                                     std::unique_ptr<LRUReplacer<file_id_t>> replacer)
    : FilePoolManager(size, std::move(replacer)), file_loader_(file_loader) {
  XODB_ASSERT(file_loader != nullptr, "file loader must not be null");
  files_.resize(size);
}

void BufferPoolManager::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_ && files_[frame_id].Valid(), "validate check");
  XODB_ASSERT(file != nullptr, "");
  *file = files_[frame_id];
}

bool BufferPoolManager::SeekFileAndUpdateCache(file_id_t file_id, frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(file_id != INVALID_FILE_ID, "");
  XODB_ASSERT(frame_id < size_, "");

  if (!file_loader_->SeekFile(file_id, file)) {
    return false;
  }

  files_[frame_id] = *file;
  return true;
}

// bool BufferPoolManager::FetchFile(file_id_t file_id, ParquetFile *file) {
//   XODB_ASSERT(file != nullptr, "input file must not be null");
//
//   // 1. first to check if the file is already in my cache
//   if (file_table_.contains(file_id)) {
//     auto frame_id = file_table_[file_id];
//     XODB_ENSURE(frame_id < size_, "must be in bound");
//     *file = files_[frame_id];
//
//     // record access so it won't be evicted in near future
//     XODB_ENSURE(replacer_->RecordAccess(frame_id), "");
//
//     // update the file table entry
//     file_table_[file_id] = frame_id;
//
//     return true;
//   }
//
//   // 2. otherwise try to fetch it (either in local or remote)
//   // 2.1. first grab an availabe frame
//   bool evicted = false;
//   auto free_frame = GetFrame(evicted);
//   XODB_ENSURE(free_frame.has_value(), "must success");
//   if (evicted) {
//     file_table_.erase(*free_frame);
//   }
//
//   file_loader_->SeekFile(file_id, &files_[*free_frame]);
//   file_table_[file_id] = *free_frame;
//
//   return true;
// }

}  // namespace xodb
