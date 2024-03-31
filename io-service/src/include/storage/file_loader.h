#pragma once

#include "common/config.h"
#include "storage/parquet_file.h"

namespace xodb {

class FileLoader {
 public:
  virtual bool SeekFile(file_id_t file_id, ParquetFile *file) const = 0;

  virtual ~FileLoader() = default;
};

}  // namespace xodb
