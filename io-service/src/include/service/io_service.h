#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/flight/api.h>
#include "buffer/buffer_pool_manager.h"

namespace xodb {

class IOService {
 public:
  explicit IOService(std::unique_ptr<arrow::fs::FileSystem> root, BufferPoolManager *buffer_pool_manager);

  arrow::Status ReadTable(const std::string &table_name, std::shared_ptr<::arrow::Table> *out);

  arrow::Status ReadTable(const std::string &table_name, const std::vector<std::string> &columns,
                          std::shared_ptr<::arrow::Table> *out);

  //  arrow::Status ListFlights(std::unique_ptr<arrow::flight::FlightListing> *listings, int server_port);
  //
  //  arrow::Status GetFlightInfo(const arrow::flight::FlightDescriptor &descriptor,
  //                              std::unique_ptr<arrow::flight::FlightInfo> *info,
  //                              int server_port);
  //
  //  arrow::Status DoPut(arrow::flight::FlightMessageReader *reader);
  //
  //  arrow::Status DoGet(const arrow::flight::Ticket &request, std::unique_ptr<arrow::flight::FlightDataStream>
  //  *stream);
  //
  //  arrow::Status ListActions(std::vector<arrow::flight::ActionType> *actions);

  //  arrow::Status DoAction(const arrow::flight::Action &action, std::unique_ptr<arrow::flight::ResultStream> *result);

 private:
  //  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(const arrow::fs::FileInfo &file_info, int server_port);
  //
  //  arrow::Result<arrow::fs::FileInfo> FileInfoFromDescriptor(const arrow::flight::FlightDescriptor &descriptor);
  //
  //  arrow::Status DoActionDropDataset(const std::string &key);
  arrow::Status RetrieveFiles(const std::vector<std::string> &file_list, std::optional<std::vector<int>> indices,
                              std::vector<std::shared_ptr<arrow::Table>> *out);

  std::unique_ptr<arrow::fs::FileSystem> root_;

  BufferPoolManager *buffer_pool_manager_{nullptr};

  // map table_name -> list of file corresponding to that table
  std::unordered_map<std::string, std::vector<std::string>> table_to_files_;

  // map table_name -> its schema
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schema_;

  //  const arrow::flight::ActionType action_drop_dataset{"drop_dataset", "Delete a dataset."};
};

}  // namespace xodb
