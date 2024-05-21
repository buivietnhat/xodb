#pragma once

#include <optional>
#include <shared_mutex>
#include "common/hash_util.h"
#include "common/macros.h"

namespace xodb::common {

template <typename K, typename V>
class LinearProbeHashTable {
  struct Item {
    K k;
    V v;
  };

 public:
  explicit LinearProbeHashTable(size_t table_size, size_t latch_group_size)
      : max_size_(table_size), latch_group_size_(latch_group_size) {
    auto num_latch_group = table_size / latch_group_size_;
    if (table_size % latch_group_size_ != 0) {
      num_latch_group += 1;
    }

    latches_.reserve(num_latch_group);
    for (size_t i = 0; i < num_latch_group; i++) {
      latches_.push_back(std::make_unique<std::shared_mutex>());
    }

    table_.resize(max_size_);
  }

  DISALLOW_COPY_AND_MOVE(LinearProbeHashTable);

  std::optional<V> Get(const K &key) {
    auto hash_index = common::HashUtil::Hash(key) % max_size_;

    size_t num_item_within_group{0};
    auto *shared_latch = FindGroupLatch(hash_index, num_item_within_group);
    XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");

    while (true) {
      {
        std::shared_lock lock(*shared_latch);
        for (size_t i = 0; i < num_item_within_group; i++) {
          if (table_.at(hash_index).has_value() && table_.at(hash_index)->k != key) {
            hash_index = (hash_index + 1) % max_size_;
            continue;
          }

          if (table_.at(hash_index).has_value()) {
            return table_.at(hash_index)->v;
          }

          return {};
        }
      }

      shared_latch = FindGroupLatch(hash_index, num_item_within_group);
      XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");
    }

    UNREACHABLE("should either find the item or stop when encoutering an empty slot");
  }

  bool AddOrUpdate(K key, V value) {
    // we always leave an empty slot as a termination marker for search
    // TODO: might need to reallocate the hash table instead of just returning false;
    if (current_size_ == max_size_ - 1) {
      return false;
    }

    auto hash_index = common::HashUtil::Hash(key) % max_size_;

    size_t num_item_within_group{0};
    auto *shared_latch = FindGroupLatch(hash_index, num_item_within_group);
    XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");

    while (true) {
      {
        std::lock_guard lock(*shared_latch);
        for (size_t i = 0; i < num_item_within_group; i++) {
          if (table_.at(hash_index).has_value()) {
            hash_index = (hash_index + 1) % max_size_;
            continue;
          }

          table_[hash_index] = {std::move(key), std::move(value)};
          return true;
        }
      }

      shared_latch = FindGroupLatch(hash_index, num_item_within_group);
      XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");
    }

    UNREACHABLE("should be able to find an empty slot");
  }

  bool Delete(const K &key) {
    auto hash_index = common::HashUtil::Hash(key) % max_size_;

    size_t num_item_within_group{0};
    auto *shared_latch = FindGroupLatch(hash_index, num_item_within_group);
    XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");

    while (true) {
      {
        std::lock_guard lock(*shared_latch);
        for (size_t i = 0; i < num_item_within_group; i++) {
          if (table_.at(hash_index).has_value() && table_.at(hash_index)->k != key) {
            hash_index = (hash_index + 1) % max_size_;
            continue;
          }

          if (table_.at(hash_index).has_value()) {
            table_[hash_index] = {};
            return true;
          }

          return false;
        }
      }

      shared_latch = FindGroupLatch(hash_index, num_item_within_group);
      XODB_ASSERT(shared_latch != nullptr && num_item_within_group > 0, "");
    }

    UNREACHABLE("should be able to find an empty slot");
  }

 private:
  std::shared_mutex *FindGroupLatch(size_t index, size_t &num_item) const {
    auto latch_index = index / latch_group_size_;
    XODB_ASSERT(latch_index < latches_.size(), "index must be inbound");

    num_item = std::min(latch_group_size_, max_size_ - latch_index);

    return latches_[latch_index].get();
  }

  size_t latch_group_size_{100};
  size_t max_size_{0};
  size_t current_size_{0};
  std::vector<std::unique_ptr<std::shared_mutex>> latches_;
  std::vector<std::optional<Item>> table_;
};

}  // namespace xodb::common
