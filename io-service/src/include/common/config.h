#pragma once

#include <cstdio>

namespace xodb {

using file_id_t = size_t;
using frame_id_t = size_t;
using table_oid_t = size_t;

static constexpr file_id_t INVALID_FILE_ID = INT32_MAX;
static constexpr frame_id_t INVALID_FRAME_ID = INT32_MAX;
static constexpr table_oid_t INVALID_TABLE_ID = INT32_MAX;

}  // namespace xodb