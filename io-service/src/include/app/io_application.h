#pragma once

#include <arrow/api.h>
#include <memory>
#include "controller/io_controller.h"

namespace xodb {

class IOApplication {
 public:
  explicit IOApplication(std::unique_ptr<IOController> controller) : controller_(std::move(controller)) {}

  arrow::Status Start(const std::string &host, int port) {
    ARROW_ASSIGN_OR_RAISE(location_, arrow::flight::Location::ForGrpcTcp(host, port));

    arrow::flight::FlightServerOptions options(location_);

    ARROW_RETURN_NOT_OK(controller_->Init(options));
    std::cout << "Listening on port " << controller_->port() << std::endl;

    return arrow::Status::OK();
  }

 private:
  std::unique_ptr<IOController> controller_;
  arrow::flight::Location location_;
};

}  // namespace xodb
