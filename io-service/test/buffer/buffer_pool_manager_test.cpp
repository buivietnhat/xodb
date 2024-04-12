#include "buffer/buffer_pool_manager.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <memory>
#include "gtest/gtest.h"
#include "storage/file_ultil_for_test.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {

TEST(BufferPoolManagerTest, FetchFileTest) {
  const int size = 5;

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ASSERT_TRUE(fs->CreateDir("./test/") == arrow::Status::OK());
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs);

  std::vector<std::string> file_names{"file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet",
                                      "file5.parquet"};
  EXPECT_TRUE(CreateParquetFileIntString(fs.get(), "./test/file1.parquet", {1, 2}, {"1", "2"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateParquetFileIntString(fs.get(), "./test/file2.parquet", {3, 4}, {"3", "4"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateParquetFileIntString(fs.get(), "./test/file3.parquet", {5, 6}, {"5", "6"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateParquetFileIntString(fs.get(), "./test/file4.parquet", {7, 8}, {"7", "8"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateParquetFileIntString(fs.get(), "./test/file5.parquet", {9, 10}, {"9", "10"}) ==
              arrow::Status::OK());

  auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(size);
  LocalDiskFileLoader file_loader{size, std::move(disk_file_replacer), root, std::make_unique<MockS3Loader>(root)};

  auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(size);
  BufferPoolManager bpm{size, &file_loader, std::move(buffer_pool_replacer)};

  std::vector<ParquetFile> files(size);

  // first fetch all the files, warm up the cache
  for (size_t i = 0; i < size; i++) {
    EXPECT_TRUE(bpm.FetchFile(file_names[i], &files[i]));
    EXPECT_EQ(file_names[i], files[i].GetFileName());
    EXPECT_TRUE(files[i].Valid());
  }

  // now fetch file that're already in local cache
  std::vector<ParquetFile> files1(size);
  for (size_t i = 0; i < size; i++) {
    EXPECT_TRUE(bpm.FetchFile(file_names[i], &files1[i]));
    EXPECT_EQ(file_names[i], files1[i].GetFileName());
    EXPECT_TRUE(files1[i].Valid());
    EXPECT_TRUE(AreTablesEqual(files[i].GetTable(), files1[i].GetTable()));
  }

  ASSERT_TRUE(fs->DeleteDirContents("./test/") == arrow::Status::OK());
}

}  // namespace xodb