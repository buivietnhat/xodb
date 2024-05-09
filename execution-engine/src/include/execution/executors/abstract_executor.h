#pragma once

#include <memory>
#include <optional>
#include <vector>

namespace xodb::data_model {
struct TableIndex;
struct TableMetaList;
}

namespace xodb::execution {

class ExecutionContext;
class PrimitiveRepository;

class AbstractExecutor {
 public:
  AbstractExecutor(std::shared_ptr<PrimitiveRepository> primitive_repository, std::shared_ptr<ExecutionContext> context,
                   std::shared_ptr<data_model::TableMetaList> table_meta_infos)
      : primitive_repository_(std::move(primitive_repository)),
        context_(std::move(context)),
        table_meta_infos_(std::move(table_meta_infos)) {}

  virtual void Execute(const data_model::TableIndex &in, data_model::TableIndex &out) const = 0;

  virtual ~AbstractExecutor() = default;

 protected:
  std::shared_ptr<PrimitiveRepository> primitive_repository_;
  std::shared_ptr<ExecutionContext> context_;
  std::shared_ptr<data_model::TableMetaList> table_meta_infos_;
};

}  // namespace xodb::execution
