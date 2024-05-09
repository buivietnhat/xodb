#pragma once

#include <memory>
#include <optional>
#include <vector>

namespace xodb::data_model {
class TableIndex;
}

namespace xodb::execution {

class ExecutionContext;
class PrimitiveRepository;

class AbstractExecutor {
 public:
  AbstractExecutor(std::shared_ptr<PrimitiveRepository> primitive_repository, std::shared_ptr<ExecutionContext> context)
      : primitive_repository_(std::move(primitive_repository)), context_(std::move(context)) {}

  virtual void Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const = 0;

  virtual ~AbstractExecutor() = default;

 protected:
  std::shared_ptr<PrimitiveRepository> primitive_repository_;
  std::shared_ptr<ExecutionContext> context_;
};

}  // namespace xodb::execution
