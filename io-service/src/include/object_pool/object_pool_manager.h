#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include "common/config.h"
#include "object_pool/lru_replacer.h"

namespace xodb {

class ObjectPoolManager {
 public:
  ObjectPoolManager(size_t size, std::unique_ptr<LRUReplacer<frame_id_t>> replacer)
      : size_(size), replacer_(std::move(replacer)) {
    XODB_ASSERT(size > 0, "buffer size must greater than zero");
    XODB_ASSERT(replacer_ != nullptr, "replacer must not be null");

    // construct available frames
    for (frame_id_t frame = 0; frame < size_; frame++) {
      available_frames_.push_back(frame);
    }
  }

  virtual ~ObjectPoolManager() = default;

  std::optional<frame_id_t> GetFrame(bool &evicted) {
    std::optional<frame_id_t> frame_id;

    ON_SCOPE_EXIT {
      if (frame_id.has_value()) {
        replacer_->RecordAccess(*frame_id);
      }
    };

    std::unique_lock l(mu_);
    if (!available_frames_.empty()) {
      frame_id = available_frames_.front();
      available_frames_.pop_front();
      evicted = false;
      return frame_id;
    }
    l.unlock();

    frame_id = 0;
    XODB_ENSURE(replacer_->Full(), "integrity check");
    XODB_ENSURE(replacer_->Evict(&(*frame_id)), "");
    evicted = true;
    return frame_id;
  }

 protected:
  size_t size_{0};
  std::unique_ptr<LRUReplacer<frame_id_t>> replacer_;
  std::list<frame_id_t> available_frames_;
  std::mutex mu_;  // protect available_frames_
};

}  // namespace xodb