#pragma once

#include <client/io_service_proxy.h>
#include "common/macros.h"

namespace xodb::execution {

class ExecutionContext {
 public:
  IOServiceProxy *GetIOServiceProxy() const {
    XODB_ASSERT(io_service_proxy_ != nullptr, "");
    return io_service_proxy_.get();
  }

 private:
  std::shared_ptr<IOServiceProxy> io_service_proxy_;
};

}  // namespace xodb::execution
