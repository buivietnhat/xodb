#pragma once

#include "storage/file.h"
#include "common/config.h"

namespace xodb {

class FileLoader {
 public:
  virtual void SeekFile(file_id_t file_id, File *file) const = 0;

  virtual ~FileLoader() = default;
};

}  // namespace xodb
