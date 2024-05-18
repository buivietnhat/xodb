#pragma once

#include <arrow/api.h>

namespace xodb::execution {

class PrimitiveRepository {
 public:
  //////////////////////////// Int32 ////////////////////////////
  static size_t Select_Equal_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                      const std::vector<int> &input_index, void *val, std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) == *int_val) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_LessThan_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                      const std::vector<int> &input_index, void *val, std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) < *int_val) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_LessThanOrEqual_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                             const std::vector<int> &input_index, void *val,
                                             std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) <= *int_val) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_GreaterThan_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                         const std::vector<int> &input_index, void *val, std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) > *int_val) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_GreaterThanOrEqual_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                                const std::vector<int> &input_index, void *val,
                                                std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) >= *int_val) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_InRangeInclusive_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                              const std::vector<int> &input_index, void *val,
                                              std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) >= int_val[0] && chunk->Value(j) <= int_val[1]) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_InRangeExclusive_Int32(const std::shared_ptr<arrow::ChunkedArray> &col,
                                              const std::vector<int> &input_index, void *val,
                                              std::vector<int> &index_out) {
    int32_t *int_val = static_cast<int32_t *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::Int32Array>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) > int_val[0] && chunk->Value(j) < int_val[1]) {
            index_out[k] = 1;
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  //////////////////////////// String ////////////////////////////
  static size_t Select_LessThan_String(const std::shared_ptr<arrow::ChunkedArray> &col,
                                       const std::vector<int> &input_index, void *val, std::vector<int> &index_out) {
    std::string *string_val = static_cast<std::string *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::StringArray>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) < *string_val) {
            index_out.push_back(k);
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_LessThan_Or_Equal_String(const std::shared_ptr<arrow::ChunkedArray> &col,
                                                const std::vector<int> &input_index, void *val,
                                                std::vector<int> &index_out) {
    std::string *string_val = static_cast<std::string *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::StringArray>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) <= *string_val) {
            index_out.push_back(k);
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_GreaterThan_String(const std::shared_ptr<arrow::ChunkedArray> &col,
                                          const std::vector<int> &input_index, void *val, std::vector<int> &index_out) {
    std::string *string_val = static_cast<std::string *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::StringArray>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) > *string_val) {
            index_out.push_back(k);
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }

  static size_t Select_GreaterThan_Or_Equal_String(const std::shared_ptr<arrow::ChunkedArray> &col,
                                                   const std::vector<int> &input_index, void *val,
                                                   std::vector<int> &index_out) {
    std::string *string_val = static_cast<std::string *>(val);
    int k = 0;
    int count = 0;
    auto num_chunk = col->num_chunks();
    for (int c = 0; c < num_chunk; c++) {
      auto chunk = std::static_pointer_cast<arrow::StringArray>(col->chunk(c));
      for (int j = 0; j < chunk->length(); j++) {
        if (input_index[k] == 1) {
          if (chunk->Value(j) >= *string_val) {
            index_out.push_back(k);
            count++;
          }
        }
        k++;
      }
    }
    return count;
  }
};

}  // namespace xodb::execution