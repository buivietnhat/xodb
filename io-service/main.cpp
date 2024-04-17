#include <iostream>
#include "common/io_service_instance.h"

int main() {
  xodb::IOServiceInstance instance;
  instance.Run();

  return 0;
}
