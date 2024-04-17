#pragma once

#include "app/io_application.h"
#include "buffer/buffer_pool_manager.h"
#include "controller/io_controller.h"
#include "service/io_service.h"
#include "storage/local_disk_file_loader.h"

namespace xodb {

class IOServiceInstance {
 public:
  IOServiceInstance();

  void Run();

 private:
  std::unique_ptr<IOService> MakeIOService(std::shared_ptr<arrow::fs::FileSystem> root,
                                           std::unique_ptr<BufferPoolManager> buffer_pool_manager);

  std::unique_ptr<BufferPoolManager> MakeBufferPool(size_t size, std::unique_ptr<LocalDiskFileLoader> file_loader,
                                                    std::unique_ptr<LRUReplacer<frame_id_t>> replacer);

  std::unique_ptr<LocalDiskFileLoader> MakeLocalDiskCache(size_t max_size,
                                                          std::unique_ptr<LRUReplacer<frame_id_t>> replacer,
                                                          std::shared_ptr<arrow::fs::FileSystem> root,
                                                          std::unique_ptr<S3FileLoader> s3_file_loader);

  std::unique_ptr<IOController> MakeController(std::unique_ptr<IOService> service);

  std::unique_ptr<IOApplication> MakeApp(std::unique_ptr<IOController> controller);

  std::unique_ptr<IOApplication> app_;
  const int buffer_mem_size_ = 5000;    // 5000 files on mem
  const int buffer_disk_size_ = 10000;  // 10000 files on disk
  const std::string host_ = "localhost";
  const std::string data_dir_ = "./data/";
  int port_ = 8080;

  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

}  // namespace xodb