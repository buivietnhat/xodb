#pragma once

#include "common/config.h"
#include "s3_file_loader.h"
#include "storage/local_disk_file_loader.h"
#include "storage/parquet_file.h"

namespace xodb {

class FileLoader {
 public:
  FileLoader(std::unique_ptr<LocalDiskFileLoader> local_loader, std::unique_ptr<S3FileLoader> remote_loader)
      : local_loader_(std::move(local_loader)), remote_loader_(std::move(remote_loader)) {}

  bool SeekFile(file_id_t file_id, ParquetFile *file) const { return false; }

 private:
  std::unique_ptr<LocalDiskFileLoader> local_loader_;
  std::unique_ptr<S3FileLoader> remote_loader_;
};

}  // namespace xodb
