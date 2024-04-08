#pragma once

#include "storage/file_loader.h"

namespace xodb {

class S3FileLoader {
 public:
  virtual bool FetchFile(const std::string &file_name, ParquetFile *file) {
    return false;
  }

  virtual ~S3FileLoader() = default;
};

}  // namespace xodb
