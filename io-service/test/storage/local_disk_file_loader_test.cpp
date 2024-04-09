#include "storage/local_disk_file_loader.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include "gtest/gtest.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {


class LocalDiskFileLoaderTest : public ::testing::Test {
 protected:
  // this function is called before every test
  void SetUp() override {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    ASSERT_TRUE(fs_->CreateDir("./test/") == arrow::Status::OK());
    ASSERT_TRUE(CreateDummyParquetFile(std::string("./test/") + DUMMY_FILE, {1, 2, 3, 4, 5},
                                       {"apple", "banana", "orange", "grape", "kiwi"}) == arrow::Status::OK());
  }

  arrow::Status CreateDummyParquetFile(const std::string &file_path, const std::vector<int32_t> &int_data,
                                       const std::vector<std::string> &string_data) {
    XODB_ASSERT(int_data.size() == string_data.size(), "two columns must have equal size");

    ARROW_ASSIGN_OR_RAISE(auto sink, fs_->OpenOutputStream(file_path));
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

    table_ = arrow::Table::Make(schema, {int_array, string_array});
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table_, arrow::default_memory_pool(), sink, /*chunk_size=*/65536));
    return arrow::Status::OK();
  }

  // this function is called after every test
  void TearDown() override { ASSERT_TRUE(fs_->DeleteDirContents("./test/") == arrow::Status::OK()); }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  const char *DUMMY_FILE = "test.parquet";
  std::shared_ptr<arrow::Table> table_;
};

TEST_F(LocalDiskFileLoaderTest, LoadFileTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);
  const size_t max_size = 5;
  auto replacer = std::make_unique<LRUReplacer<frame_id_t>>(max_size);
  LocalDiskFileLoader ldfl{max_size, std::move(replacer), root, std::make_unique<MockS3Loader>(root)};

  ParquetFile file;
  EXPECT_EQ(ldfl.LoadFile(DUMMY_FILE, &file), arrow::Status::OK());
  EXPECT_EQ(file.GetFileName(), DUMMY_FILE);
  EXPECT_TRUE(AreTablesEqual(file.GetTable(), table_));

  //  arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
  //  EXPECT_EQ(arrow::PrettyPrint(*file.GetTable(), print_options, &std::cout), arrow::Status::OK());
}

TEST_F(LocalDiskFileLoaderTest, FetchFileTest) {
  std::vector<std::string> file_names{"file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet",
                                      "file5.parquet"};
  EXPECT_TRUE(CreateDummyParquetFile("./test/file1.parquet", {1, 2}, {"1", "2"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile("./test/file2.parquet", {3, 4}, {"3", "4"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile("./test/file3.parquet", {5, 6}, {"5", "6"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile("./test/file4.parquet", {7, 8}, {"7", "8"}) == arrow::Status::OK());
  EXPECT_TRUE(CreateDummyParquetFile("./test/file5.parquet", {9, 10}, {"9", "10"}) == arrow::Status::OK());

  auto s3_root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  ASSERT_TRUE(fs_->CreateDir("./local/") == arrow::Status::OK());
  auto local_root = std::make_shared<arrow::fs::SubTreeFileSystem>("./local/", fs_);
  const size_t max_size = 5;
  auto replacer = std::make_unique<LRUReplacer<frame_id_t>>(max_size);
  auto s3_loader = std::make_unique<MockS3Loader>(std::move(s3_root));
  auto *s3_loader_ptr = s3_loader.get();

  LocalDiskFileLoader ldfl{max_size, std::move(replacer), std::move(local_root), std::move(s3_loader)};

  EXPECT_TRUE(s3_root == nullptr);
  EXPECT_TRUE(local_root == nullptr);

  std::vector<ParquetFile> files(max_size);

  // first fetch all the files, warm up the cache
  for (size_t i = 0; i < max_size; i++) {
    EXPECT_TRUE(ldfl.FetchFile(file_names[i], &files[i]));
    EXPECT_EQ(file_names[i], files[i].GetFileName());
    EXPECT_TRUE(files[i].Valid());
  }

  // should call `max_size` number of times to fetch the file from S3
  EXPECT_EQ(max_size, s3_loader_ptr->GetCallNumber());
  auto num_s3_call = max_size;

  // fetch a file that is not existed
  ParquetFile file;
  EXPECT_FALSE(file.Valid());
  EXPECT_FALSE(ldfl.FetchFile("file6.parquet", &file));
  EXPECT_FALSE(file.Valid());
  // should make a call to s3 loader
  EXPECT_EQ(num_s3_call + 1, s3_loader_ptr->GetCallNumber());
  num_s3_call += 1;

  // now fetch file that're already in local disk
  std::vector<ParquetFile> files1(max_size);
  for (size_t i = 0; i < max_size; i++) {
    EXPECT_TRUE(ldfl.FetchFile(file_names[i], &files1[i]));
    EXPECT_EQ(file_names[i], files1[i].GetFileName());
    EXPECT_TRUE(files1[i].Valid());
    EXPECT_TRUE(AreTablesEqual(files[i].GetTable(), files1[i].GetTable()));
  }

  // should not make any call to s3
  EXPECT_EQ(num_s3_call, s3_loader_ptr->GetCallNumber());

  // now fetch another file, must evict the oldest one
  EXPECT_TRUE(CreateDummyParquetFile("./test/file6.parquet", {11, 12}, {"11", "12"}) == arrow::Status::OK());
  EXPECT_FALSE(file.Valid());
  EXPECT_TRUE(ldfl.FetchFile("file6.parquet", &file));
  EXPECT_TRUE(file.Valid());

  EXPECT_EQ(num_s3_call + 1, s3_loader_ptr->GetCallNumber());
  num_s3_call += 1;

  // try to fetch the oldest one again, should fetch from s3 since the entry was evicted
  file.Invalidate();
  EXPECT_TRUE(ldfl.FetchFile(file_names[0], &file));
  EXPECT_EQ(num_s3_call + 1, s3_loader_ptr->GetCallNumber());

  ASSERT_TRUE(fs_->DeleteDir("./local/") == arrow::Status::OK());
}

}  // namespace xodb