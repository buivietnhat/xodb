#pragma once

#include <vector>

namespace xodb::execution {

class Pipeline;

class ExecutionEngine {
 public:
  explicit ExecutionEngine(std::vector<Pipeline> pipelines);

  void Run();

 private:
  std::vector<Pipeline> pipelines_;
};

}  // namespace xodb::execution