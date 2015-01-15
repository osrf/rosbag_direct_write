#include <gtest/gtest.h>

#include "rosbag_direct_write/aligned_allocator.h"

TEST(AlignedAllocatorTestSuite, testAlignmentAndComparators)
{
  typedef std::vector<uint8_t, aligned_allocator<uint8_t, 4096>> TestType;
  TestType vec = {0, 1, 2};
  ASSERT_EQ(vec[0], 0);
  ASSERT_EQ(vec[1], 1);
  ASSERT_EQ(vec[2], 2);
  int alignment_offset = reinterpret_cast<uintptr_t>(vec.data()) % 4096;
  // This could be false positive iff the alignment is not working, but it
  // happens to allocate on a 4096 boundary by chance.
  // Either way it should never fail.
  ASSERT_EQ(alignment_offset, 0);
  // Test comparators
  aligned_allocator<int, 4096> a;
  aligned_allocator<double, 4096> b;
  ASSERT_TRUE(a == b);
  ASSERT_FALSE(a != b);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
