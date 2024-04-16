#include "service/io_service.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <memory>
#include "gtest/gtest.h"
#include "storage/file_ultil_for_test.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {

class IoServiceTest : public ::testing::Test {
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

  std::unique_ptr<BufferPoolManager> CreateBufferPoolManager(size_t buffer_mem_size, size_t buffer_disk_size,
                                            std::shared_ptr<arrow::fs::FileSystem> root) {
    auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_disk_size);
    auto file_loader = std::make_unique<LocalDiskFileLoader>(buffer_disk_size, std::move(disk_file_replacer), std::move(root),
                                           std::make_unique<MockS3Loader>(root));

    auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_mem_size);
    return std::make_unique<BufferPoolManager>(buffer_mem_size, std::move(file_loader), std::move(buffer_pool_replacer));
  }

  // this function is called after every test
  void TearDown() override { ASSERT_TRUE(fs_->DeleteDirContents("./test/") == arrow::Status::OK()); }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  const std::vector<std::string> table_names_{"foo", "bar", "zoo"};
};

TEST_F(IoServiceTest, RecoverTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  std::unordered_map<std::string, std::vector<std::string>> table_info_ground_truth = {
      {"foo", {"foo/foo1.parquet", "foo/foo2.parquet", "foo/foo3.parquet"}},
      {"bar", {"bar/bar1.parquet", "bar/bar2.parquet", "bar/bar3.parquet"}},
      {"zoo", {"zoo/zoo1.parquet", "zoo/zoo2.parquet", "zoo/zoo3.parquet"}}};

  const int buffer_mem_size = 5;
  const int buffer_disk_size = 9;

  auto bmp = CreateBufferPoolManager(buffer_mem_size, buffer_disk_size, root);
  auto io_service = std::make_unique<IOService>(root, std::move(bmp));
  EXPECT_EQ(arrow::Status::OK(), io_service->Recover());

  auto tableinfo = io_service->GetTableInfos();
  EXPECT_EQ(table_info_ground_truth.size(), tableinfo.size());

  for (auto [tablename, filelist] : tableinfo) {
    EXPECT_TRUE(table_info_ground_truth.contains(tablename));
    auto filelist_ground_truth = table_info_ground_truth[tablename];

    std::sort(filelist_ground_truth.begin(), filelist_ground_truth.end());
    std::sort(filelist.begin(), filelist.end());
    EXPECT_EQ(filelist_ground_truth, filelist);
  }
}

TEST_F(IoServiceTest, ReadFullTableTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  const int buffer_mem_size = 5;
  const int buffer_disk_size = 9;

  auto bmp = CreateBufferPoolManager(buffer_mem_size, buffer_disk_size, root);
  auto io_service = std::make_unique<IOService>(root, std::move(bmp));
  EXPECT_EQ(arrow::Status::OK(), io_service->Recover());

  auto table_out = io_service->ReadTable("foo");
  EXPECT_TRUE(table_out != nullptr);

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

TEST_F(IoServiceTest, ReadTableWithSelectedColumnTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  const int buffer_mem_size = 5;
  const int buffer_disk_size = 9;

  auto bmp = CreateBufferPoolManager(buffer_mem_size, buffer_disk_size, root);
  auto io_service = std::make_unique<IOService>(root, std::move(bmp));
  EXPECT_EQ(arrow::Status::OK(), io_service->Recover());

  std::vector<std::string> column_names{"string_column"};
  auto table_out = io_service->ReadTable("bar", column_names);
  EXPECT_TRUE(table_out != nullptr);

  //  FileUtil::PrintPretty(table_out);

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
