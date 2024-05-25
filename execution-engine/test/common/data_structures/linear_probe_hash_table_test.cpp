#include "common/data_structures/linear_probe_hash_table.h"
#include <random>
#include <thread>
#include <unordered_map>
#include "gtest/gtest.h"

namespace xodb::common {

std::string RandomString(size_t length) {
  const std::string characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<> dist(0, characters.size() - 1);

  std::string random_string;
  random_string.reserve(length);

  for (size_t i = 0; i < length; ++i) {
    random_string += characters[dist(rng)];
  }

  return random_string;
}

TEST(LinearProbeHashTableTest, BasicTest) {
  LinearProbeHashTable<std::string, std::string> ht{100, 10};

  std::unordered_map<std::string, std::string> ground_truth;
  // put data
  for (int i = 0; i < 99; i++) {
    auto rd = RandomString(5);
    auto k = std::to_string(i);
    EXPECT_TRUE(ht.AddOrUpdate(k, rd));
    ground_truth[k] = rd;
  }

  // get back
  for (int i = 0; i < 99; i++) {
    auto k = std::to_string(i);
    EXPECT_EQ(ground_truth.at(k), *ht.Get(k));
  }

  // the table is full, cannot add anymore
  EXPECT_FALSE(ht.AddOrUpdate("99", "99"));

  // but update an existed key should be ok
  EXPECT_TRUE(ht.AddOrUpdate("5", "5"));

  EXPECT_TRUE(ht.Delete("5"));

  EXPECT_FALSE(ht.Get("5").has_value());
}

TEST(LinearProbeHashTableTest, MutithreadedTest) {
  const size_t thread_num = 5;
  std::vector<std::thread> threads;

  LinearProbeHashTable<std::string, std::string> ht{1000, 50};
  std::vector<std::unordered_map<std::string, std::string>> gts(thread_num);

  auto put_loop = [&](size_t index, size_t first, size_t second) {
    for (size_t i = first; i < second; i++) {
      auto k = std::to_string(i);
      auto rd = RandomString(5);
      gts[index][k] = rd;
      EXPECT_TRUE(ht.AddOrUpdate(k, rd));
    }
  };

  auto get_loop = [&](size_t index, size_t first, size_t second) {
    for (size_t i = first; i < second; i++) {
      auto k = std::to_string(i);
      std::optional<std::string> val = ht.Get(k);
      EXPECT_TRUE(val.has_value());
      EXPECT_EQ(gts[index][k], *val);
    }
  };

  for (size_t i = 0; i < thread_num; i++) {
    auto f = i * 100;
    auto s = f + 100;
    threads.emplace_back([&, i, f, s] {
      put_loop(i, f, s);
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::vector<std::thread> get_threads;
  for (size_t i = 0; i < thread_num; i++) {
    auto f = i * 100;
    auto s = f + 100;
    get_threads.emplace_back([&, i, f, s] {
      get_loop(i, f, s);
    });
  }

  for (auto &t : get_threads) {
    t.join();
  }
}

}  // namespace xodb::common