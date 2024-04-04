#pragma once

#include "common/macros.h"
#include "object_pool/object_pool_manager.h"
#include "storage/parquet_file.h"

namespace xodb {

class FilePoolManager : public ObjectPoolManager {
 public:
  using ObjectPoolManager::ObjectPoolManager;

  bool FetchFile(const std::string &file_id, ParquetFile *file);

 private:
  virtual void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) = 0;

  // the file is not cached, seek it somewhere
  virtual bool SeekFileAndUpdateCache(const std::string &file_name, frame_id_t frame_id, ParquetFile *file) = 0;

  virtual std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const = 0;

  virtual void RemoveFrame(frame_id_t frame_id) = 0;

  std::unordered_map<std::string, frame_id_t> file_table_;
};

}  // namespace xodb