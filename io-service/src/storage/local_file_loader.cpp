#include "storage/local_disk_file_loader.h"

namespace xodb {

LocalDiskFileLoader::LocalDiskFileLoader(size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                                 std::shared_ptr<arrow::fs::FileSystem> root)
    : FilePoolManager(max_size, std::move(replacer)), root_(std::move(root)) {
  XODB_ASSERT(root_ != nullptr, "root fs must not be null");
}

void LocalDiskFileLoader::LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) {
  XODB_ASSERT(frame_id < size_, "");
  XODB_ASSERT(file != nullptr, "");

  auto file_name = filenames_[frame_id];
}

bool LocalDiskFileLoader::SeekFileAndUpdateCache(file_id_t file_id, frame_id_t frame_id, ParquetFile *file) {
  return false;
}


}  // namespace xodb