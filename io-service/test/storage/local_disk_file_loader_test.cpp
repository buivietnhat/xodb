#include "storage/local_disk_file_loader.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include "gtest/gtest.h"

namespace xodb {

bool AreTablesEqual(const std::shared_ptr<arrow::Table> &table1, const std::shared_ptr<arrow::Table> &table2) {
  // Check if schemas are equal
  if (!table1->schema()->Equals(*table2->schema())) {
    return false;
  }

  // Check if record batches are equal
  if (table1->num_rows() != table2->num_rows()) {
    return false;
  }

  // Iterate over each column
  for (int i = 0; i < table1->num_columns(); ++i) {
    // Get the arrays for each column in both tables
    auto array1 = table1->column(i)->chunk(0);
    auto array2 = table2->column(i)->chunk(0);

    // Check if arrays are equal
    if (!array1->Equals(array2)) {
      return false;
    }
  }

  // Tables are equal
  return true;
}

class LocalDiskFileLoaderTest : public ::testing::Test {
 protected:
  // this function is called before every test
  void SetUp() override {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    ASSERT_TRUE(fs_->CreateDir("./test/") == arrow::Status::OK());
    ASSERT_TRUE(CreateDummyParquetFile(std::string("./test/") + DUMMY_FILE) == arrow::Status::OK());
  }

  arrow::Status CreateDummyParquetFile(const std::string &file_path) {
    ARROW_ASSIGN_OR_RAISE(auto sink, fs_->OpenOutputStream(file_path));
    //    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());
    auto int_field = std::make_shared<arrow::Field>("int_column", arrow::int32());
    auto string_field = std::make_shared<arrow::Field>("string_column", arrow::utf8());
    auto schema = arrow::schema({int_field, string_field});

    // Create data arrays for each column
    std::vector<int32_t> int_data = {1, 2, 3, 4, 5};
    std::vector<std::string> string_data = {"apple", "banana", "orange", "grape", "kiwi"};

    arrow::Int32Builder int_builder;
    ARROW_RETURN_NOT_OK(int_builder.AppendValues(int_data));

    arrow::StringBuilder string_builder;
    ARROW_RETURN_NOT_OK(string_builder.AppendValues(string_data));

    std::shared_ptr<arrow::Array> int_array, string_array;
    ARROW_ASSIGN_OR_RAISE(int_array, int_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(string_array, string_builder.Finish());

    table_ = arrow::Table::Make(schema, {int_array, string_array});
    //    table_ = table;
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
  LocalDiskFileLoader ldfl{max_size, std::move(replacer), root};

  ParquetFile file;
  EXPECT_EQ(ldfl.LoadFile(DUMMY_FILE, &file), arrow::Status::OK());
  EXPECT_EQ(file.GetFileName(), DUMMY_FILE);
  EXPECT_TRUE(AreTablesEqual(file.GetTable(), table_));

  arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
  EXPECT_EQ(arrow::PrettyPrint(*file.GetTable(), print_options, &std::cout), arrow::Status::OK());
}

}  // namespace xodb