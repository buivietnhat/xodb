
#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"

namespace xodb {

TEST(LRUReplacerTest, BasicTest) {
  LRUReplacer<int> replacer{3};

  replacer.RecordAccess(1);
  replacer.RecordAccess(2);
  replacer.RecordAccess(3);

  ASSERT_EQ(3, replacer.Size());

  // evict the first victim, should be 1
  int victem{-1};
  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(1, victem);

  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(2, victem);

  EXPECT_TRUE(replacer.Evict(&victem));
  EXPECT_EQ(3, victem);

  // now we no longer able to evict a new item
  EXPECT_FALSE(replacer.Evict(&victem));
}

}  // namespace xodb