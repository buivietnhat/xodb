#pragma once

namespace xodb {

template <typename Item>
class Replacer {
 public:
  virtual void RecordAccess(const Item &item) = 0;

  virtual bool Evict(Item *item) = 0;

  virtual void Remove(const Item &item) = 0;
};

}  // namespace xodb