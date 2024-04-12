#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <iostream>

namespace xodb {

class FileUtil {
 public:
  static arrow::Status ListAllDirectory(std::shared_ptr<arrow::fs::FileSystem> root, std::vector<std::string> &out);

  static arrow::Status ListAllFiles(std::shared_ptr<arrow::fs::FileSystem> root, std::string_view file_extension,
                                    std::vector<std::string> &out, bool recursive = false,
                                    std::optional<arrow::fs::FileSelector> = {}, const std::string &path = "");

  template <typename T>
  static void PrintPretty(const std::shared_ptr<T> &object) {
    arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
    XODB_ASSERT(arrow::PrettyPrint(*object, print_options, &std::cout) == arrow::Status::OK(), "");
  }
};

}  // namespace xodb