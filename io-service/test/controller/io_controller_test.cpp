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

  // this function is called after every test
  void TearDown() override { ASSERT_TRUE(fs_->DeleteDirContents("./test/") == arrow::Status::OK()); }

  static std::unique_ptr<IOService> CreateIOService(size_t buffer_mem_size, size_t buffer_disk_size,
                                                    std::shared_ptr<arrow::fs::FileSystem> root) {
    auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_disk_size);
    auto file_loader = std::make_unique<LocalDiskFileLoader>(buffer_disk_size, std::move(disk_file_replacer), root,
                                                             std::make_unique<MockS3Loader>(root));

    auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_mem_size);
    auto bpm =
        std::make_unique<BufferPoolManager>(buffer_mem_size, std::move(file_loader), std::move(buffer_pool_replacer));

    return std::make_unique<IOService>(root, std::move(bpm));
  }

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

//      FileUtil::PrintPretty(table);
      *out = std::move(table);

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

  class Server {
   public:
    Server(std::string_view host, int port, std::shared_ptr<arrow::fs::FileSystem> root) {
      XODB_ASSERT(Listen(host, port, std::move(root)) == arrow::Status::OK(), "");
    }

   private:
    arrow::Status Listen(std::string_view host, int port, std::shared_ptr<arrow::fs::FileSystem> root) {
      arrow::flight::Location server_location;
      ARROW_ASSIGN_OR_RAISE(server_location, arrow::flight::Location::ForGrpcTcp(std::string(host), port));
      arrow::flight::FlightServerOptions options(server_location);

      const int buffer_mem_size = 5;
      const int buffer_disk_size = 9;
      auto service = CreateIOService(buffer_mem_size, buffer_disk_size, std::move(root));
      server_ = std::make_unique<IOController>(std::move(service));

      ARROW_RETURN_NOT_OK(server_->Init(options));
      std::cout << "Listening on port " << server_->port() << std::endl;

      return arrow::Status::OK();
    }

    std::unique_ptr<arrow::flight::FlightServerBase> server_;
  };

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  const std::vector<std::string> table_names_{"foo", "bar", "zoo"};
};

TEST_F(IoControllerTest, ReadTableTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  std::string host = "localhost";
  int port = 8080;

  Server server{host, port, std::move(root)};
  Client client{host, port};

  std::shared_ptr<arrow::Table> table_out;
  EXPECT_EQ(arrow::Status::OK(), client.ReadTable("foo", {}, &table_out));

  ASSERT_TRUE(table_out != nullptr);

  std::vector<std::vector<int>> columns(2, std::vector<int>());
  for (int i = 0; i < 2; i++) {
    // we have three chunks since the table is concatenated by 3 files
    auto num_chunks = table_out->column(i)->num_chunks();
    EXPECT_EQ(3, num_chunks);
    for (int c = 0; c < num_chunks; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(table_out->column(i)->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        columns[i].push_back(chunk->Value(j));
      }
    }
  }

  for (int i = 0; i < 2; i++) {
    std::sort(columns[i].begin(), columns[i].end());
  }

  std::vector<int> ground_truth{1, 2, 3, 4, 5, 6};
  EXPECT_EQ(ground_truth, columns[0]);
  EXPECT_EQ(ground_truth, columns[1]);
}

TEST_F(IoControllerTest, ReadTableWithSelectedColumnsTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  std::string host = "localhost";
  int port = 8080;

  Server server{host, port, std::move(root)};
  Client client{host, port};

  std::shared_ptr<arrow::Table> table_out;
  std::vector<std::string> column_names{"string_column"};
  ASSERT_TRUE(client.ReadTable("bar", column_names, &table_out) == arrow::Status::OK());
  ASSERT_TRUE(table_out != nullptr);

//
  EXPECT_EQ(1, table_out->num_columns());
  std::vector<std::string> ground_truth{"1", "2", "3", "4", "5", "6"};

  EXPECT_EQ(3, table_out->column(0)->num_chunks());

  std::vector<std::string> actual_string_column;

  for (int i = 0; i < 3; i++) {
    auto chunk = std::static_pointer_cast<arrow::StringArray>(table_out->column(0)->chunk(i));
    for (int j = 0; j < chunk->length(); j++) {
      actual_string_column.push_back(std::string(chunk->Value(j)));
    }
  }

  std::sort(actual_string_column.begin(), actual_string_column.end());
  EXPECT_EQ(ground_truth, actual_string_column);
}

}  // namespace xodb