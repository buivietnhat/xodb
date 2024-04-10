#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>

namespace xodb {

class FileUtil {
 public:
  static arrow::Status ListAllDirectory(std::shared_ptr<arrow::fs::FileSystem> root, std::vector<std::string> &out);

  static arrow::Status ListAllFiles(std::shared_ptr<arrow::fs::FileSystem> root, std::string_view file_extension,
                                    std::vector<std::string> &out, bool recursive = false,
                                    std::optional<arrow::fs::FileSelector> = {}, const std::string &path = "");
};

}  // namespace xodb