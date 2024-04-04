#include "object_pool/lru_replacer.h"
#include "gtest/gtest.h"

namespace xodb {

TEST(LRUReplacerTest, BasicTest) {
  LRUReplacer<int> replacer{3};

  EXPECT_TRUE(replacer.RecordAccess(1));
  EXPECT_TRUE(replacer.RecordAccess(2));
  EXPECT_TRUE(replacer.RecordAccess(3));

  EXPECT_EQ(3, replacer.Size());
  EXPECT_TRUE(replacer.Full());

  // evict the first victim, should be 1
  int victem{-1};
  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(1, victem);

  EXPECT_TRUE(replacer.RecordAccess(4));
  EXPECT_TRUE(replacer.RecordAccess(2));

  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(3, victem);

  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(4, victem);

  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(2, victem);

  // now we no longer be able to evict a new item
  EXPECT_FALSE(replacer.Evict(&victem));
}

}  // namespace xodb