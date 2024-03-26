#pragma once

#include <memory>
#include "buffer/replacer.h"
#include "common/config.h"
#include "storage/page.h"

namespace xodb {

class BufferPoolManager {
 public:
  std::unique_ptr<Page> FetchPage(page_id_t page_id);

 private:
};

}  // namespace xodb