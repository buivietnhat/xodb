#pragma once

#include <memory>
#include <optional>
#include <vector>

namespace xodb::execution {

class ExecutionContext;
class PrimitiveRepository;

class Executor {
 public:
  Executor(std::shared_ptr<PrimitiveRepository> primitive_repository, std::shared_ptr<ExecutionContext> context)
      : primitive_repository_(std::move(primitive_repository)), context_(std::move(context)) {}

  virtual void Execute() const = 0;

  virtual ~Executor() = default;

 protected:
  std::shared_ptr<PrimitiveRepository> primitive_repository_;
  std::shared_ptr<ExecutionContext> context_;
};

}  // namespace xodb::execution
