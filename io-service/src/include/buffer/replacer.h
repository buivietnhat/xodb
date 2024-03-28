#pragma once

namespace xodb {

template <typename Item>
class Replacer {
 public:
  // return true if we've recorded successfully
  // false otherwise, i.e the replacer is full
  virtual bool RecordAccess(const Item &item) = 0;

  virtual bool Evict(Item *item) = 0;

  virtual bool Remove(const Item &item) = 0;

  virtual ~Replacer() = default;
};

}  // namespace xodb