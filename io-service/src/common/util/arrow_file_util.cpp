#include "common/util/arrow_file_util.h"
#include "common/macros.h"

namespace xodb {

arrow::Status FileUtil::ListAllDirectory(std::shared_ptr<arrow::fs::FileSystem> root, std::vector<std::string> &out) {
  XODB_ASSERT(root != nullptr, "");

  arrow::fs::FileSelector selector;
  selector.base_dir = "/";

  ARROW_ASSIGN_OR_RAISE(auto listing, root->GetFileInfo(selector));

  for (const auto &dir_info : listing) {
    if (!dir_info.IsDirectory()) {
      continue;
    }

    out.push_back(dir_info.base_name());
  }

  return arrow::Status::OK();
}

arrow::Status FileUtil::ListAllFiles(std::shared_ptr<arrow::fs::FileSystem> root, std::string_view file_extension,
                                     std::vector<std::string> &out, bool recursive,
                                     std::optional<arrow::fs::FileSelector> selector, const std::string &path) {
  XODB_ASSERT(root != nullptr, "");

  if (!selector.has_value()) {
    selector = arrow::fs::FileSelector();
  }

  ARROW_ASSIGN_OR_RAISE(auto listing, root->GetFileInfo(*selector));

  for (const auto &file_info : listing) {
    if (file_info.IsDirectory() && recursive) {
      auto sub_root = std::make_shared<arrow::fs::SubTreeFileSystem>(file_info.base_name(), root);
      XODB_ASSERT(ListAllFiles(std::move(sub_root), file_extension, out, recursive, selector,
                               path + file_info.base_name() + "/") == arrow::Status::OK(),
                  "");
    }

    if (file_info.IsFile()) {
      if (file_info.extension() != file_extension) {
        continue;
      }

      out.push_back(path + file_info.base_name());
    }
  }

  return arrow::Status::OK();
}



}  // namespace xodb