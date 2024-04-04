#include "service/io_service.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "common/macros.h"
#include "fmt/format.h"

namespace xodb {

IOService::IOService(std::unique_ptr<arrow::fs::FileSystem> root, BufferPoolManager *buffer_pool_manager)
    : root_(std::move(root)), buffer_pool_manager_(buffer_pool_manager) {
  XODB_ASSERT(buffer_pool_manager_ != nullptr, "");
}

// arrow::Status IOService::ListFlights(std::unique_ptr<arrow::flight::FlightListing> *listings, int server_port) {
//   arrow::fs::FileSelector selector;
//   selector.base_dir = "/";
//   ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));
//
//   std::vector<arrow::flight::FlightInfo> flights;
//   for (const auto &file_info : listing) {
//     if (!file_info.IsFile() || file_info.extension() != "parquet") {
//       continue;
//     }
//
//     ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info, server_port));
//     flights.push_back(std::move(info));
//   }
//
//   *listings = std::unique_ptr<arrow::flight::FlightListing>(new
//   arrow::flight::SimpleFlightListing(std::move(flights))); return arrow::Status::OK();
// }
//
// arrow::Status IOService::GetFlightInfo(const arrow::flight::FlightDescriptor &descriptor,
//                                        std::unique_ptr<arrow::flight::FlightInfo> *info, int server_port) {
//   ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
//   ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info, server_port));
//   *info = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(std::move(flight_info)));
//   return arrow::Status::OK();
// }
//
// arrow::Status IOService::DoPut(arrow::flight::FlightMessageReader *reader) {
//   ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(reader->descriptor()));
//   ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
//   ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());
//
//   ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
//   return arrow::Status::OK();
// }
//
// arrow::Status IOService::DoGet(const arrow::flight::Ticket &request,
//                                std::unique_ptr<arrow::flight::FlightDataStream> *stream) {
//   ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
//   std::unique_ptr<parquet::arrow::FileReader> reader;
//   ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));
//
//   std::shared_ptr<arrow::Table> table;
//   ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
//   // Note that we can't directly pass TableBatchReader to
//   // RecordBatchStream because TableBatchReader keeps a non-owning
//   // reference to the underlying Table, which would then get freed
//   // when we exit this function
//   std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
//   arrow::TableBatchReader batch_reader(*table);
//   ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());
//
//   ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(std::move(batches), table->schema()));
//   *stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(owning_reader));
//
//   return arrow::Status::OK();
// }
//
// arrow::Status IOService::ListActions(std::vector<arrow::flight::ActionType> *actions) {
//   *actions = {action_drop_dataset};
//   return arrow::Status::OK();
// }
//
// arrow::Status IOService::DoAction(const arrow::flight::Action &action,
//                                   std::unique_ptr<arrow::flight::ResultStream> *result) {
//   if (action.type == action_drop_dataset.type) {
//     *result = std::unique_ptr<arrow::flight::ResultStream>(new arrow::flight::SimpleResultStream({}));
//     return DoActionDropDataset(action.body->ToString());
//   }
//
//   return arrow::Status::NotImplemented("Unknown action type: ", action.type);
// }
//
// arrow::Result<arrow::flight::FlightInfo> IOService::MakeFlightInfo(const arrow::fs::FileInfo &file_info,
//                                                                    int server_port) {
//   ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
//   std::unique_ptr<parquet::arrow::FileReader> reader;
//   ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));
//
//   std::shared_ptr<arrow::Schema> schema;
//   ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
//
//   auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});
//
//   arrow::flight::FlightEndpoint endpoint;
//   endpoint.ticket.ticket = file_info.base_name();
//   arrow::flight::Location location;
//   ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::ForGrpcTcp("localhost", server_port));
//   endpoint.locations.push_back(location);
//
//   int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
//   int64_t total_bytes = file_info.size();
//
//   return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records, total_bytes);
// }
//
// arrow::Result<arrow::fs::FileInfo> IOService::FileInfoFromDescriptor(
//     const arrow::flight::FlightDescriptor &descriptor) {
//   if (descriptor.type != arrow::flight::FlightDescriptor::PATH) {
//     return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
//   } else if (descriptor.path.size() != 1) {
//     return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor with one path component");
//   }
//   return root_->GetFileInfo(descriptor.path[0]);
// }
//
// arrow::Status IOService::DoActionDropDataset(const std::string &key) { return root_->DeleteFile(key); }

arrow::Status IOService::ReadTable(const std::string &table_name, std::shared_ptr<::arrow::Table> *out) {
  if (!table_to_files_.contains(table_name)) {
    // TODO(nhat): get files info from Catalog, load it from S3 server
    return arrow::Status::NotImplemented("load from Catalog and S3");
  }

  const auto &file_list = table_to_files_[table_name];
  std::vector<std::shared_ptr<arrow::Table>> tables_read;
  tables_read.reserve(file_list.size());

  ARROW_RETURN_NOT_OK(RetrieveFiles(file_list, {}, &tables_read));

  ARROW_ASSIGN_OR_RAISE(*out, arrow::ConcatenateTables(tables_read));

  return arrow::Status::OK();
}

arrow::Status IOService::ReadTable(const std::string &table_name, const std::vector<std::string> &columns,
                                   std::shared_ptr<::arrow::Table> *out) {
  if (!table_to_files_.contains(std::string(table_name))) {
    // TODO(nhat): get files info from Catalog, load it from S3 server
    return arrow::Status::NotImplemented("load from Catalog and S3");
  }

  // first validate the schema
  auto schema = schema_[table_name];
  std::vector<int> indices;
  indices.reserve(columns.size());
  for (const auto &col : columns) {
    auto index = schema->GetFieldIndex(col);
    if (index == -1) {
      return arrow::Status::Invalid(fmt::format("couldn't read colum {} of table {}", col, table_name));
    }
    indices.push_back(index);
  }

  XODB_ENSURE(table_to_files_.contains(table_name), "it should be");

  const auto &file_list = table_to_files_[table_name];
  std::vector<std::shared_ptr<arrow::Table>> tables_read;
  tables_read.reserve(file_list.size());

  // retrieve files with selected indices
  ARROW_RETURN_NOT_OK(RetrieveFiles(file_list, indices, &tables_read));

  ARROW_ASSIGN_OR_RAISE(*out, arrow::ConcatenateTables(tables_read));

  return arrow::Status::OK();
}

arrow::Status IOService::RetrieveFiles(const std::vector<std::string> &file_list, std::optional<std::vector<int>> indices,
                                       std::vector<std::shared_ptr<arrow::Table>> *out) {
  ParquetFile file;
  bool select_colums = indices.has_value();

  for (const auto &filename  : file_list) {
    if (!buffer_pool_manager_->FetchFile(filename, &file)) {
      return arrow::Status::Invalid(fmt::format("couldn't load file {}", filename));
    }

    std::shared_ptr<arrow::Table> table;
    if (select_colums) {
      table = file.GetTable(*indices);
    } else {
      table = file.GetTable();
    }

    if (table == nullptr) {
      return arrow::Status::Invalid(fmt::format("couldn't load file {} with indices", filename));
    }

    out->push_back(std::move(table));
  }

  return arrow::Status::OK();
}

}  // namespace xodb
