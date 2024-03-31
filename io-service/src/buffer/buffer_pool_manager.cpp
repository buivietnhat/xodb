#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace xodb {

BufferPoolManager::BufferPoolManager(size_t size, FileLoader *file_loader,
                                     std::unique_ptr<LRUReplacer<file_id_t>> replacer)
    : size_(size), file_loader_(file_loader), replacer_(std::move(replacer)) {
  XODB_ASSERT(size > 0, "buffer size must greater than zero");
  XODB_ASSERT(file_loader != nullptr, "file loader must not be null");
  XODB_ASSERT(replacer != nullptr, "replacer must not be null");

  files_.resize(size);

  // construct available frames
  for (frame_id_t frame = 0; frame < size_; frame++) {
    available_frames_.push_back(frame);
  }
}

BufferPoolManager::~BufferPoolManager() {}

bool BufferPoolManager::FetchFile(file_id_t file_id, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "input file must not be null");

  // 1. first to check if the file is already in my cache
  if (file_table_.contains(file_id)) {
    auto frame_id = file_table_[file_id];
    XODB_ENSURE(frame_id < size_, "must be in bound");
    *file = files_[frame_id];

    // record access so it won't be evicted in near future
    XODB_ENSURE(replacer_->RecordAccess(frame_id), "");

    // update the file table entry
    file_table_[file_id] = frame_id;

    return true;
  }

  // 2. otherwise try to fetch it (either in local or remote)
  // 2.1. first grab an availabe frame
  bool evicted = false;
  auto free_frame = GetFrame(evicted);
  XODB_ENSURE(free_frame.has_value(), "must success");
  if (evicted) {
    file_table_.erase(*free_frame);
  }

  file_loader_->SeekFile(file_id, &files_[*free_frame]);
  XODB_ENSURE(replacer_->RecordAccess(*free_frame), "");
  file_table_[file_id] = *free_frame;

  return true;
}

}  // namespace xodb
