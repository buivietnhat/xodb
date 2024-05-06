#pragma once

#include <optional>
#include "data/schema.h"

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

class Plan {
 public:
  Plan(PlanType type, data::Schema output_schema) : type_(type), output_schema_(std::move(output_schema)) {}

  Plan(PlanType type, data::Schema output_schema, Cardinality cardinality) : Plan(type, std::move(output_schema)) {
    cardinality_ = cardinality;
  }

  PlanType GetType() const { return type_; }

  const std::optional<Cardinality> &GetCardinality() const { return cardinality_; }

  virtual std::shared_ptr<Plan> CreateSubPlan(Cardinality cardinality) const = 0;

  virtual ~Plan() = default;

 private:
  PlanType type_;
  data::Schema output_schema_;
  std::optional<Cardinality> cardinality_;
};

}  // namespace xodb::plan
