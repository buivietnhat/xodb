#include "common/io_service_instance.h"
#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <chrono>
#include <thread>
#include "common/macros.h"
#include "storage/mock_s3_file_loader.h"

namespace xodb {

IOServiceInstance::IOServiceInstance() {
  fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
  XODB_ASSERT(fs_->CreateDir(data_dir_) == arrow::Status::OK(), "");

  auto root = std::make_shared<arrow::fs::SubTreeFileSystem>(data_dir_, fs_);
  auto s3_loader = std::make_unique<MockS3Loader>(root);

  auto disk_file_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_disk_size_);
  auto disk_cache = MakeLocalDiskCache(buffer_disk_size_, std::move(disk_file_replacer), root, std::move(s3_loader));

  auto buffer_pool_replacer = std::make_unique<LRUReplacer<frame_id_t>>(buffer_mem_size_);
  auto buffer_pool = MakeBufferPool(buffer_mem_size_, std::move(disk_cache), std::move(buffer_pool_replacer));

  auto service = MakeIOService(root, std::move(buffer_pool));

  auto controller = MakeController(std::move(service));

  app_ = MakeApp(std::move(controller));
}

void IOServiceInstance::Run() {
  XODB_ASSERT(app_->Start(host_, port_) == arrow::Status::OK(), "");

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

std::unique_ptr<IOService> IOServiceInstance::MakeIOService(std::shared_ptr<arrow::fs::FileSystem> root,
                                                            std::unique_ptr<BufferPoolManager> buffer_pool_manager) {
  return std::make_unique<IOService>(root, std::move(buffer_pool_manager));
}

std::unique_ptr<BufferPoolManager> IOServiceInstance::MakeBufferPool(
    size_t size, std::unique_ptr<LocalDiskFileLoader> file_loader, std::unique_ptr<LRUReplacer<frame_id_t>> replacer) {
  return std::make_unique<BufferPoolManager>(size, std::move(file_loader), std::move(replacer));
}

std::unique_ptr<LocalDiskFileLoader> IOServiceInstance::MakeLocalDiskCache(
    size_t max_size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer, std::shared_ptr<arrow::fs::FileSystem> root,
    std::unique_ptr<S3FileLoader> s3_file_loader) {
  return std::make_unique<LocalDiskFileLoader>(max_size, std::move(replacer), root, std::move(s3_file_loader));
}

std::unique_ptr<IOController> IOServiceInstance::MakeController(std::unique_ptr<IOService> service) {
  return std::make_unique<IOController>(std::move(service));
}

std::unique_ptr<IOApplication> IOServiceInstance::MakeApp(std::unique_ptr<IOController> controller) {
  return std::make_unique<IOApplication>(std::move(controller));
}

}  // namespace xodb