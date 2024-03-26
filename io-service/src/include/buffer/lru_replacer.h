#pragma once

#include <list>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/concepts.h"
#include "common/macros.h"

namespace xodb {

template <typename Item>
  requires Hashable<Item>
class LRUReplacer : public Replacer<Item> {
  using Pos = typename std::list<Item>::iterator;

 public:
  LRUReplacer(size_t max_item) : max_num_items_(max_item) {}

  void RecordAccess(const Item &item) override {
    // if the record is still in my history
    if (index_.contains(item)) {
      auto pos = index_[item];

      history_.erase(pos);
      RecordNewest(item);

      return;
    }

    // otherwise insert as a new item
    // if our cache is already full, evict one
    if (history_.size() == max_num_items_) {
      Item evicted_item;
      XODB_ENSURE(Evict(&evicted_item), "must be succeeded");
    }

    RecordNewest(item);
  }

  bool Evict(Item *item) override {
    XODB_ASSERT(item != nullptr, "item must not be null");

    if (history_.empty()) {
      return false;
    }

    *item = history_.back();

    history_.pop_back();
    index_.erase(*item);

    return true;
  }

  void Remove(const Item &item) override {
    if (!index_.contains(item)) {
      return;
    }

    auto pos = index_[item];
    history_.erase(pos);
    index_.erase(item);
  }

 private:

  void RecordNewest(const Item &item) {
    auto new_pos = history_.insert(history_.begin(), item);
    index_[item] = new_pos;
  }

  size_t max_num_items_;
  // represen this history of access, newly accessed item will be put to the front of the list
  std::list<Item> history_;
  std::unordered_map<Item, Pos> index_;
};

}  // namespace xodb