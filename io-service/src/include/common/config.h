#pragma once

#include <cstdio>

namespace xodb {

// using file_id_t = std::string;
using frame_id_t = size_t;
using table_oid_t = size_t;

// static const char *INVALID_FILE_ID = "invalid";
static constexpr frame_id_t INVALID_FRAME_ID = INT32_MAX;
static constexpr table_oid_t INVALID_TABLE_ID = INT32_MAX;

}  // namespace xodb