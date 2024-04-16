#pragma once

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include "nlohmann/json.hpp"
#include "service/io_service.h"

namespace xodb {

class IOController : public arrow::flight::FlightServerBase {
 public:
  explicit IOController(std::unique_ptr<IOService> service) : service_(std::move(service)) {
    XODB_ASSERT(service_ != nullptr, "");
    service_->Recover();
  }

  //  arrow::Status DoPut(const arrow::flight::ServerCallContext &,
  //                      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
  //                      std::unique_ptr<arrow::flight::FlightMetadataWriter>) override {
  //    return service_->DoPut(reader.get());
  //  }

  arrow::Status DoGet(const arrow::flight::ServerCallContext &, const arrow::flight::Ticket &request,
                      std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
    XODB_ASSERT(stream != nullptr, "input validation");

    std::string table_info_string = request.ticket;
    nlohmann::json table_info_json;
    try {
      table_info_json = nlohmann::json::parse(table_info_string);
    } catch (...) {
      std::cout << "Error: couldn't parse the request info for ticket " << table_info_string << " to json" << std::endl;
      return arrow::Status::Invalid("wrong ticket format, couldn't parse json");
    }

    if (!table_info_json.contains(TABLE_KEY_NAME)) {
      return arrow::Status::Invalid("wrong ticket format, missing column's name");
    }

    std::shared_ptr<::arrow::Table> table_out;
    std::string table_name = table_info_json[TABLE_KEY_NAME];
    if (!table_info_json.contains(COLUMNS_KEY_NAME)) {
      table_out = service_->ReadTable(table_name);
    } else {
      auto colums = table_info_json[COLUMNS_KEY_NAME].get<std::vector<std::string>>();
      table_out = service_->ReadTable(table_name, colums);
    }

    if (table_out == nullptr) {
      return arrow::Status::Invalid("cannot found table for " + table_name);
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::TableBatchReader batch_reader(*table_out);
    ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());

    ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(std::move(batches), table_out->schema()));
    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(owning_reader));
    return arrow::Status::OK();
  }

 private:
  std::unique_ptr<IOService> service_;
};

}  // namespace xodb
