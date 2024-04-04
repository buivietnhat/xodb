#pragma once

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include "service/io_service.h"

namespace xodb {

class IOController : public arrow::flight::FlightServerBase {
 public:
  explicit IOController(std::unique_ptr<IOService> service);

  //  arrow::Status ListFlights(const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *,
  //                              std::unique_ptr<arrow::flight::FlightListing> *listings) override {
  //    return service_->ListFlights(listings, port());
  //  }
  //
  //  arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &,
  //                                const arrow::flight::FlightDescriptor &descriptor,
  //                                std::unique_ptr<arrow::flight::FlightInfo> *info) override {
  //    return service_->GetFlightInfo(descriptor, info, port());
  //  }
  //
  //  arrow::Status DoPut(const arrow::flight::ServerCallContext &,
  //                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
  //                        std::unique_ptr<arrow::flight::FlightMetadataWriter>) override {
  //    return service_->DoPut(reader.get());
  //  }
  //
  //  arrow::Status DoGet(const arrow::flight::ServerCallContext &, const arrow::flight::Ticket &request,
  //                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
  //    return service_->DoGet(request, stream);
  //  }
  //
  //  arrow::Status ListActions(const arrow::flight::ServerCallContext &,
  //                              std::vector<arrow::flight::ActionType> *actions) override {
  //    return service_->ListActions(actions);
  //  }
  //
  //  arrow::Status DoAction(const arrow::flight::ServerCallContext &, const arrow::flight::Action &action,
  //                           std::unique_ptr<arrow::flight::ResultStream> *result) override {
  //    return service_->DoAction(action, result);
  //  }

 private:
  std::unique_ptr<IOService> service_;
};

}  // namespace xodb
