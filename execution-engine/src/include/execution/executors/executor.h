#pragma once

#include <memory>
#include <optional>
#include <vector>
#include "client/catalog_proxy.h"
#include "execution/execution_context.h"
#include "execution/primitive_repository.h"

namespace xodb::execution {

class Executor {
 public:
  Executor(std::shared_ptr<CatalogProxy> catalog, std::shared_ptr<PrimitiveRepository> primitive_repository,
           std::shared_ptr<ExecutionContext> context)
      : catalog_(std::move(catalog)),
        primitive_repository_(std::move(primitive_repository)),
        context_(std::move(context)) {}


  virtual void Execute() = 0;

  virtual ~Executor() = default;

 protected:
  std::shared_ptr<CatalogProxy> catalog_;
  std::shared_ptr<PrimitiveRepository> primitive_repository_;
  std::shared_ptr<ExecutionContext> context_;
};

}  // namespace xodb::execution
