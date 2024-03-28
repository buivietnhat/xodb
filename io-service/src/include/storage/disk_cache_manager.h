#pragma once

#include <buffer/replacer.h>
#include <memory>
#include "common/config.h"

namespace xodb {

class DiskCacheManager {
 public:
 private:
  std::unique_ptr<Replacer<file_id_t>> replacer_;
};

}  // namespace xodb
