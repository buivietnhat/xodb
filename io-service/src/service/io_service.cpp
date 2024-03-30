#include "service/io_service.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace xodb {

IOService::IOService(std::unique_ptr<arrow::fs::FileSystem> root) : root_(std::move(root)) {}

arrow::Status IOService::ListFlights(std::unique_ptr<arrow::flight::FlightListing> *listings, int server_port) {
  arrow::fs::FileSelector selector;
  selector.base_dir = "/";
  ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));

  std::vector<arrow::flight::FlightInfo> flights;
  for (const auto &file_info : listing) {
    if (!file_info.IsFile() || file_info.extension() != "parquet") {
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info, server_port));
    flights.push_back(std::move(info));
  }

  *listings = std::unique_ptr<arrow::flight::FlightListing>(new arrow::flight::SimpleFlightListing(std::move(flights)));
  return arrow::Status::OK();
}

arrow::Status IOService::GetFlightInfo(const arrow::flight::FlightDescriptor &descriptor,
                                       std::unique_ptr<arrow::flight::FlightInfo> *info, int server_port) {
  ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
  ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info, server_port));
  *info = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(std::move(flight_info)));
  return arrow::Status::OK();
}

arrow::Status IOService::DoPut(arrow::flight::FlightMessageReader *reader) {
  ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(reader->descriptor()));
  ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  return arrow::Status::OK();
}

arrow::Status IOService::DoGet(const arrow::flight::Ticket &request,
                               std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
  ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
  // Note that we can't directly pass TableBatchReader to
  // RecordBatchStream because TableBatchReader keeps a non-owning
  // reference to the underlying Table, which would then get freed
  // when we exit this function
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  arrow::TableBatchReader batch_reader(*table);
  ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());

  ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(std::move(batches), table->schema()));
  *stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(owning_reader));

  return arrow::Status::OK();
}

arrow::Status IOService::ListActions(std::vector<arrow::flight::ActionType> *actions) {
  *actions = {action_drop_dataset};
  return arrow::Status::OK();
}

arrow::Status IOService::DoAction(const arrow::flight::Action &action,
                                  std::unique_ptr<arrow::flight::ResultStream> *result) {
  if (action.type == action_drop_dataset.type) {
    *result = std::unique_ptr<arrow::flight::ResultStream>(new arrow::flight::SimpleResultStream({}));
    return DoActionDropDataset(action.body->ToString());
  }

  return arrow::Status::NotImplemented("Unknown action type: ", action.type);
}

arrow::Result<arrow::flight::FlightInfo> IOService::MakeFlightInfo(const arrow::fs::FileInfo &file_info,
                                                                   int server_port) {
  ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

  auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});

  arrow::flight::FlightEndpoint endpoint;
  endpoint.ticket.ticket = file_info.base_name();
  arrow::flight::Location location;
  ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp("localhost", server_port));
  endpoint.locations.push_back(location);

  int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
  int64_t total_bytes = file_info.size();

  return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records, total_bytes);
}

arrow::Result<arrow::fs::FileInfo> IOService::FileInfoFromDescriptor(
    const arrow::flight::FlightDescriptor &descriptor) {
  if (descriptor.type != arrow::flight::FlightDescriptor::PATH) {
    return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
  } else if (descriptor.path.size() != 1) {
    return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor with one path component");
  }
  return root_->GetFileInfo(descriptor.path[0]);
}

arrow::Status IOService::DoActionDropDataset(const std::string &key) { return root_->DeleteFile(key); }

}  // namespace xodb
