#include "buffer/buffer_pool_manager.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <memory>
#include "gtest/gtest.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {

arrow::Status CreateDummyParquetFile(arrow::fs::FileSystem *fs, const std::string &file_path,
                                     const std::vector<int32_t> &int_data,
                                     const std::vector<std::string> &string_data) {
  XODB_ASSERT(int_data.size() == string_data.size(), "two columns must have equal size");

  ARROW_ASSIGN_OR_RAISE(auto sink, fs->OpenOutputStream(file_path));
  auto int_field = std::make_shared<arrow::Field>("int_column", arrow::int32());
  auto string_field = std::make_shared<arrow::Field>("string_column", arrow::utf8());
  auto schema = arrow::schema({int_field, string_field});

  arrow::Int32Builder int_builder;
  ARROW_RETURN_NOT_OK(int_builder.AppendValues(int_data));

  arrow::StringBuilder string_builder;
  ARROW_RETURN_NOT_OK(string_builder.AppendValues(string_data));

  std::shared_ptr<arrow::Array> int_array, string_array;
  ARROW_ASSIGN_OR_RAISE(int_array, int_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(string_array, string_builder.Finish());

  auto table = arrow::Table::Make(schema, {int_array, string_array});
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
  return arrow::Status::OK();
}

TEST(BufferPoolManagerTest, FetchFileTest) {
  const int size = 5;

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ASSERT_TRUE(fs->CreateDir("./test/") == arrow::Status::OK());
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs);

  std::vector<std::string> file_names{"file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet",
                                      "file5.parquet"};
  EXPECT_TRUE(CreateDummyParquetFile(fs.get(), "./test/file1.parquet", {1, 2}, {"1", "2"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile(fs.get(), "./test/file2.parquet", {3, 4}, {"3", "4"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile(fs.get(), "./test/file3.parquet", {5, 6}, {"5", "6"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile(fs.get(), "./test/file4.parquet", {7, 8}, {"7", "8"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile(fs.get(), "./test/file5.parquet", {9, 10}, {"9", "10"}) == arrow::Status::OK());

  auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(size);
  LocalDiskFileLoader file_loader{size, std::move(disk_file_replacer), root, std::make_unique<MockS3Loader>(root)};

  auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(size);
  BufferPoolManager bpm{size, &file_loader, std::move(buffer_pool_replacer)};

  EXPECT_TRUE(file_loader.WarmUp() == arrow::Status::OK());

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