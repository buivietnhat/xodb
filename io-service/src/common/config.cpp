#include "common/config.h"
#include <atomic>

namespace xodb {

std::atomic<bool> global_disable_execution_exception_print{false};

}