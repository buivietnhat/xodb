#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include "common/concepts.h"
#include "common/macros.h"
#include "object_pool/replacer.h"

namespace xodb {

template <typename Item>
requires Hashable<Item>
class LRUReplacer : public Replacer<Item> {
  using Pos = typename std::list<Item>::iterator;

  DISALLOW_COPY_AND_MOVE(LRUReplacer);

 public:
  LRUReplacer(size_t max_item) : max_num_items_(max_item) {}

  bool RecordAccess(const Item &item) override {
    std::lock_guard l(mu_);
    // if the record is still in my history
    if (index_.contains(item)) {
      auto pos = index_[item];

      history_.erase(pos);
      RecordNewest(item);

      return true;
    }

    if (FullUnlocked()) {
      return false;
    }

    // otherwise insert as a new item
    RecordNewest(item);
    return true;
  }

  bool Evict(Item *item) override {
    XODB_ASSERT(item != nullptr, "item must not be null");

    std::lock_guard l(mu_);

    if (history_.empty()) {
      return false;
    }

    *item = history_.back();

    history_.pop_back();
    index_.erase(*item);

    return true;
  }

  bool Remove(const Item &item) override {
    std::lock_guard l(mu_);

    if (!index_.contains(item)) {
      return false;
    }

    auto pos = index_[item];
    history_.erase(pos);
    index_.erase(item);

    return true;
  }

  size_t Size() const {
    std::lock_guard l(mu_);
    return history_.size();
  }

  bool Full() const {
    std::lock_guard l(mu_);
    return FullUnlocked();
  }

 private:
  void RecordNewest(const Item &item) {
    auto new_pos = history_.insert(history_.begin(), item);
    index_[item] = new_pos;
  }

  bool FullUnlocked() const { return history_.size() == max_num_items_; }

  size_t max_num_items_;
  // represen this history of access, newly accessed item will be put to the front of the list
  std::list<Item> history_;
  std::unordered_map<Item, Pos> index_;
  mutable std::mutex mu_;
};

}  // namespace xodb