#include "common/util/arrow_file_util.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include "gtest/gtest.h"
#include "storage/file_ultil_for_test.h"

namespace xodb {

class ArrowFileUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    EXPECT_TRUE(fs_->CreateDir("./test/") == arrow::Status::OK());
    const std::string dummy_path = "./test/" + DUMMY_FILE;

    EXPECT_TRUE(CreateParquetFileIntString(fs_.get(), dummy_path, {1, 2, 3, 4, 5},
                                           {"apple", "banana", "orange", "grape", "kiwi"}) == arrow::Status::OK());

    EXPECT_TRUE(fs_->CreateDir("./test/foo") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CopyFile(dummy_path, "./test/foo/foo.parquet") == arrow::Status::OK());

    EXPECT_TRUE(fs_->CreateDir("./test/foo/foo1") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CopyFile(dummy_path, "./test/foo/foo1/foo1.parquet") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CreateDir("./test/foo/foo2") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CopyFile(dummy_path, "./test/foo/foo2/foo2.parquet") == arrow::Status::OK());

    EXPECT_TRUE(fs_->CreateDir("./test/bar") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CopyFile(dummy_path, "./test/bar/bar.parquet") == arrow::Status::OK());

    EXPECT_TRUE(fs_->CreateDir("./test/zoo") == arrow::Status::OK());
    EXPECT_TRUE(fs_->CopyFile(dummy_path, "./test/zoo/zoo.parquet") == arrow::Status::OK());
  }

  void TearDown() override { EXPECT_TRUE(fs_->DeleteDirContents("./test/") == arrow::Status::OK()); }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  const std::string DUMMY_FILE{"dummy.parquet"};
};

TEST_F(ArrowFileUtilTest, ListDirTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  std::vector<std::string> dirs;
  EXPECT_EQ(arrow::Status::OK(), FileUtil::ListAllDirectory(std::move(root), dirs));

  std::vector<std::string> ground_truth{"foo", "bar", "zoo"};
  EXPECT_EQ(ground_truth, dirs);
}

TEST_F(ArrowFileUtilTest, ListFileTest) {
  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./test/", fs_);

  bool recursive = false;
  std::vector<std::string> files;
  EXPECT_EQ(arrow::Status::OK(), FileUtil::ListAllFiles(root, "parquet", files, recursive));
  EXPECT_EQ(std::vector<std::string>{DUMMY_FILE}, files);

  recursive = true;
  files.clear();
  std::vector<std::string> ground_truth{DUMMY_FILE,
                                        "foo/foo.parquet",
                                        "foo/foo1/foo1.parquet",
                                        "foo/foo2/foo2"
                                        ".parquet",
                                        "bar/bar.parquet",
                                        "zoo/zoo.parquet"};
  EXPECT_EQ(arrow::Status::OK(), FileUtil::ListAllFiles(root, "parquet", files, recursive));

  std::sort(ground_truth.begin(), ground_truth.end());
  std::sort(files.begin(), files.end());
  EXPECT_EQ(ground_truth, files);
}

}  // namespace xodb