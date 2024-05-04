#pragma once

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

class Plan {
 public:
  PlanType GetType() const { return type_; }

 private:
  PlanType type_;
  data::Schema output_schema_;
};

}
