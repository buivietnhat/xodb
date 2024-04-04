#pragma once

#include "common/macros.h"
#include "object_pool/object_pool_manager.h"
#include "storage/parquet_file.h"

namespace xodb {

class FilePoolManager : public ObjectPoolManager {
 public:
  using ObjectPoolManager::ObjectPoolManager;

  bool FetchFile(file_id_t file_id, ParquetFile *file);

 private:
  virtual void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) = 0;

  // the file is not cached, seek it somewhere
  virtual bool SeekFileAndUpdateCache(file_id_t file_id, frame_id_t frame_id, ParquetFile *file) = 0;

  std::unordered_map<file_id_t, frame_id_t> file_table_;
};

}  // namespace xodb