#include "object_pool/file_pool_manager.h"

namespace xodb {

bool FilePoolManager::FetchFile(file_id_t file_id, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "input file must not be null");

  // 1. first to check if the file is already in my cache
  if (file_table_.contains(file_id)) {
    auto frame_id = file_table_[file_id];
    XODB_ENSURE(frame_id < size_, "must be in bound");

    LoadFileCachedCorrespondToFrame(file_id, file);

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

  if (!SeekFileAndUpdateCache(file_id, *free_frame, file)) {
    return false;
  }

  file_table_[file_id] = *free_frame;
  return true;
}

}  // namespace xodb