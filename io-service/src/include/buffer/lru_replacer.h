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
 public:
  LRUReplacer(size_t max_item) : max_num_items_(max_item) {}

  bool RecordAccess(const Item &item) override {
    // if the record is still in my history
    auto pos = std::find(history_.begin(), history_.end(), item);
    if (pos != history_.end()) {
      history_.erase(pos);
      history_.push_front(item);
      return true;
    }

    if (Full()) {
      return false;
    }

    // otherwise insert as a new item
    history_.push_front(item);
    return true;
  }

  bool Evict(Item *item) override {
    XODB_ASSERT(item != nullptr, "item must not be null");

    if (history_.empty()) {
      return false;
    }

    *item = history_.back();
    history_.pop_back();

    return true;
  }

  bool Remove(const Item &item) override {
    auto pos = std::find(history_.begin(), history_.end(), item);
    if (pos == history_.end()) {
      return false;
    }

    history_.erase(pos);
    return true;
  }

  size_t Size() const { return history_.size(); }

  bool Full() const { return history_.size() == max_num_items_; }

 private:
  size_t max_num_items_;
  // represen this history of access, newly accessed item will be put to the front of the list
  std::list<Item> history_;
};

}  // namespace xodb