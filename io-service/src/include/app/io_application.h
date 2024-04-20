#pragma once

#include <arrow/api.h>
#include <memory>
#include "Poco/Util/ServerApplication.h"
#include "controller/io_controller.h"

namespace xodb {

class IOApplication : public Poco::Util::ServerApplication {
 public:
  explicit IOApplication(std::unique_ptr<IOController> controller, int port, std::string host)
      : controller_(std::move(controller)), port_(port), host_(std::move(host)) {}

 protected:
  void initialize(Application &self) override { XODB_ASSERT(StartListening() == arrow::Status::OK(), ""); }

  void uninitialize() override { XODB_ASSERT(ShutDown() == arrow::Status::OK(), ""); }

  int main(const std::vector<std::string> &args) override {
    waitForTerminationRequest();
    return 0;
  }

 private:
  arrow::Status StartListening() {
    ARROW_ASSIGN_OR_RAISE(location_, arrow::flight::Location::ForGrpcTcp(host_, port_));

    arrow::flight::FlightServerOptions options(location_);

    ARROW_RETURN_NOT_OK(controller_->Init(options));
    std::cout << "Listening on port " << controller_->port() << std::endl;

    return arrow::Status::OK();
  }

  arrow::Status ShutDown() {
    ARROW_RETURN_NOT_OK(controller_->Shutdown());
    return arrow::Status::OK();
  }

  std::unique_ptr<IOController> controller_;
  arrow::flight::Location location_;
  const int port_;
  std::string host_;
};

}  // namespace xodb
