#pragma once

#include "common/macros.h"
#include "object_pool/object_pool_manager.h"
#include "storage/parquet_file.h"

namespace xodb {

class FilePoolManager : public ObjectPoolManager {
 public:
  using ObjectPoolManager::ObjectPoolManager;

  bool FetchFile(const std::string &file_name, ParquetFile *file);

 protected:
  frame_id_t AddFileToPool(const std::string &file_names);

 private:
  virtual void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) = 0;

  // the file is not cached, seek it somewhere
  virtual bool SeekFile(const std::string &file_name, ParquetFile *file) = 0;

  virtual void UpdateCache(frame_id_t frame_id, ParquetFile *file) = 0;

  virtual std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const = 0;

  virtual void RemoveFrame(frame_id_t frame_id) = 0;

  std::unordered_map<std::string, frame_id_t> file_table_;
  std::mutex mu_;  // protect file_table_
};

}  // namespace xodb