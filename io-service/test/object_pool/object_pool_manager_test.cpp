#include "object_pool/object_pool_manager.h"
#include "object_pool/lru_replacer.h"
#include "gtest/gtest.h"

namespace xodb {

TEST(ObjectPoolManagerTest, GetFrameTest) {
  const size_t size = 5;
  auto replacer = std::make_unique<LRUReplacer<frame_id_t>>(size);
  ObjectPoolManager opm{size, std::move(replacer)};

  // should be able to get 5 frames
  bool evicted{false};
  for (size_t f = 0; f < size; f++) {
    std::optional<frame_id_t> frame = opm.GetFrame(evicted);
    EXPECT_TRUE(frame.has_value());
    EXPECT_FALSE(evicted);
    EXPECT_EQ(f, *frame);
  }

  // now there are no free frames, must've evicted the oldest one
  for (size_t f = 0; f < size; f++) {
    std::optional<frame_id_t> frame = opm.GetFrame(evicted);
    EXPECT_TRUE(frame.has_value());
    EXPECT_TRUE(evicted);
    EXPECT_EQ(f, *frame);
  }
}

}  // namespace xodb