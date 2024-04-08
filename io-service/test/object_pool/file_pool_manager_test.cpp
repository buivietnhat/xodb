#include "gtest/gtest.h"
#include "object_pool/file_pool_manager.h"

namespace xodb {

class FilePoolManagerDerive : public FilePoolManager {
 public:
  using FilePoolManager::FilePoolManager;

  void LoadFileCachedCorrespondToFrame(frame_id_t frame_id, ParquetFile *file) override {}

  bool SeekFile(const std::string &file_name, ParquetFile *file) override { return false; }

  void UpdateCache(frame_id_t frame_id, ParquetFile *file) override {}

  std::optional<std::string> GetFileNameOfFrame(frame_id_t frame_id) const override {
    return std::optional<std::string>();
  }

  void RemoveFrame(frame_id_t frame_id) override {}
};

TEST(FilePoolManagerTest, FetchFileTest) {
  const size_t max_size = 5;
  auto replacer = std::make_unique<LRUReplacer<frame_id_t>>(max_size);
  FilePoolManagerDerive fp{max_size, std::move(replacer)};
}

}