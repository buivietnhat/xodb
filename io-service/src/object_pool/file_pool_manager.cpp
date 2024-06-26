#include "object_pool/file_pool_manager.h"

namespace xodb {

bool FilePoolManager::FetchFile(const std::string &file_name, ParquetFile *file) {
  XODB_ASSERT(file != nullptr, "input file must not be null");

  std::unique_lock file_table_latch(mu_);
  // 1. first to check if the file is already in my cache
  if (file_table_.contains(file_name)) {
    auto frame_id = file_table_[file_name];
    XODB_ENSURE(frame_id < size_, "must be in bound")
    file_table_latch.unlock();

    LoadFileCachedCorrespondToFrame(frame_id, file);

    // record access so it won't be evicted in near future
    XODB_ENSURE(replacer_->RecordAccess(frame_id), "")

    return true;
  }

  file_table_latch.unlock();
  // 2. otherwise try to fetch it (either in local or remote)
  // 2.1 check if I really have that file
  bool file_existed = SeekFile(file_name, file);
  if (!file_existed) {
    return false;
  }
  // 2.1. first grab an availabe frame
  bool evicted = false;
  auto free_frame = GetFrame(evicted);
  XODB_ENSURE(free_frame.has_value(), "must success")

  // erase the old entry corresponding to this frame
  if (evicted) {
    std::optional<std::string> old_frame_name = GetFileNameOfFrame(*free_frame);
    XODB_ENSURE(old_frame_name.has_value(), "must exist")

    file_table_latch.lock();
    file_table_.erase(*old_frame_name);
    file_table_latch.unlock();

    RemoveFrame(*free_frame);
  }

  UpdateCache(*free_frame, file);

  file_table_latch.lock();
  file_table_[file_name] = *free_frame;
  file_table_latch.unlock();
  return true;
}

frame_id_t FilePoolManager::AddFileToPool(const std::string &file_name) {
  XODB_ENSURE(!available_frames_.empty(), "must still have free slot");

  bool evicted = false;
  auto free_frame = GetFrame(evicted);
  XODB_ENSURE(free_frame.has_value(), "must success")
  XODB_ENSURE(evicted == false, "should not do");

  std::unique_lock file_table_latch(mu_);
  file_table_[file_name] = *free_frame;

  return *free_frame;
}

}  // namespace xodb