#include "common/io_service_instance.h"

int main(int argc, char **argv) {
  xodb::IOServiceInstance instance;
  return instance.Run(argc, argv);
}
