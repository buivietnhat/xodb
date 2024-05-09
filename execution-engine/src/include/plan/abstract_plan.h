#pragma once

#include <optional>
#include "data_model/schema.h"

namespace xodb::plan {

enum class PlanType : uint8_t {
  RESERVED,
  SEQUENCE_SCAN,
  PROBE_PHASE_HASH_JOIN,
  BUILD_PHASE_HASH_JOIN,
  AGGREGATE,
  FILTER
};

struct Cardinality {
  size_t start_index;
  size_t lenght;
};

class AbstractPlan {
 public:
  explicit AbstractPlan(PlanType type) : type_(type) {}

  AbstractPlan(PlanType type, Cardinality cardinality) : AbstractPlan(type) { cardinality_ = cardinality; }

  PlanType GetType() const { return type_; }

  const std::optional<Cardinality> &GetCardinality() const { return cardinality_; }

  virtual std::shared_ptr<AbstractPlan> CreateSubPlan(Cardinality cardinality) const = 0;

  virtual ~AbstractPlan() = default;

 protected:
  PlanType type_;
  std::optional<Cardinality> cardinality_;
};

}  // namespace xodb::plan
