#include "controller/io_controller.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <memory>
#include "common/config.h"
#include "common/util/arrow_file_util.h"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"
#include "service/io_service.h"
#include "storage/file_ultil_for_test.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {

class IoControllerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    ASSERT_TRUE(fs_->CreateDir("./test/") == arrow::Status::OK());

    for (const auto &name : table_names_) {
      auto table_dir = "./test/" + name + "/";
      ASSERT_TRUE(fs_->CreateDir(table_dir) == arrow::Status::OK());
      CreateDummyFiles(name, table_dir);
    }
  }

  void CreateDummyFiles(std::string_view tablename, const std::string &table_path) {
    if (tablename == "foo") {
      // int-int
      ASSERT_EQ(arrow::Status::OK(), CreateParquetFileIntInt(fs_.get(), table_path + "foo1.parquet", {1, 2}, {1, 2}));
      ASSERT_EQ(arrow::Status::OK(), CreateParquetFileIntInt(fs_.get(), table_path + "foo2.parquet", {3, 4}, {3, 4}));
      ASSERT_EQ(arrow::Status::OK(), CreateParquetFileIntInt(fs_.get(), table_path + "foo3.parquet", {5, 6}, {5, 6}));
    } else if (tablename == "bar") {
      // int-string
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileIntString(fs_.get(), table_path + "bar1.parquet", {1, 2}, {"1", "2"}));
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileIntString(fs_.get(), table_path + "bar2.parquet", {3, 4}, {"3", "4"}));
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileIntString(fs_.get(), table_path + "bar3.parquet", {5, 6}, {"5", "6"}));

    } else if (tablename == "zoo") {
      // string-string
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileStringString(fs_.get(), table_path + "zoo1.parquet", {"1", "2"}, {"1", "2"}));
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileStringString(fs_.get(), table_path + "zoo2.parquet", {"3", "4"}, {"3", "4"}));
      ASSERT_EQ(arrow::Status::OK(),
                CreateParquetFileStringString(fs_.get(), table_path + "zoo3.parquet", {"5", "6"}, {"5", "6"}));
    }
  }

  std::unique_ptr<IOService> CreateIOService(size_t buffer_mem_size, size_t buffer_disk_size,
                                             std::shared_ptr<arrow::fs::FileSystem> root) {
    auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_disk_size);
    static LocalDiskFileLoader file_loader{buffer_disk_size, std::move(disk_file_replacer), std::move(root),
                                           std::make_unique<MockS3Loader>(root)};

    auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_mem_size);
    static BufferPoolManager bpm{buffer_mem_size, &file_loader, std::move(buffer_pool_replacer)};

    return std::make_unique<IOService>(root, &bpm);
  }

  // this function is called after every test
  void TearDown() override { ASSERT_TRUE(fs_->DeleteDirContents("./test/") == arrow::Status::OK()); }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  const std::vector<std::string> table_names_{"foo", "bar", "zoo"};
};

class Client {
 public:
  Client(std::string_view host, const int port) {
    XODB_ASSERT(arrow::Status::OK() == ConnectToServer(host, port), "must be able to");
  }

  arrow::Status ReadTable(std::string_view table_name, const std::optional<std::vector<std::string>> &columns,
                          std::shared_ptr<arrow::Table> *out) {
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    arrow::flight::Ticket ticket;

    nlohmann::json json_ticket;
    json_ticket[TABLE_KEY_NAME] = std::string(table_name);

    if (columns.has_value()) {
      json_ticket[COLUMNS_KEY_NAME] = *columns;
    }

    ticket.ticket = json_ticket.dump();

    ARROW_ASSIGN_OR_RAISE(stream, client_->DoGet(ticket));
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());

    //    arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
    //    ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &std::cout));
    FileUtil::PrintPretty(table);

    return arrow::Status::OK();
  }

 private:
  arrow::Status ConnectToServer(std::string_view host, const int port) {
    ARROW_ASSIGN_OR_RAISE(location_, arrow::flight::Location::ForGrpcTcp(std::string(host), port));
    ARROW_ASSIGN_OR_RAISE(client_, arrow::flight::FlightClient::Connect(location_));
    std::cout << "Connected to " << location_.ToString() << std::endl;
    return arrow::Status::OK();
  }

  arrow::flight::Location location_;
  std::unique_ptr<arrow::flight::FlightClient> client_;
};

TEST_F(IoControllerTest, ReadTableTest) {}

}  // namespace xodb