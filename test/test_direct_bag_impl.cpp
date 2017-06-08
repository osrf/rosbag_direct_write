#include <gtest/gtest.h>

#include "rosbag_direct_write/direct_bag_impl.h"

TEST(DirectBagImplTestSuite, test_to_header_string)
{
  int16_t a = 42;
  ASSERT_EQ(std::string("\x2A\x00", 2),
            rosbag_direct_write::impl::to_header_string(&a));

  double b = 3.14;
  // Expected value calculated with Python: struct.pack('<d', 3.14)
  ASSERT_EQ(std::string("\x1f\x85\xebQ\xb8\x1e\t@"),
            rosbag_direct_write::impl::to_header_string(&b));

  std::string c = "foo";
  ASSERT_EQ(std::string("foo"),
            rosbag_direct_write::impl::to_header_string(&c));

  // Test case is 1 second and 1 nanosecond.
  ros::Time d(1.0 + 1e-9);
  // Expected result is a pair of int32s, one for secs and one for nanosecs.
  ASSERT_EQ(std::string("\x1\0\0\0\x1\0\0\0", 8),
            rosbag_direct_write::impl::to_header_string(&d));
}

TEST(DirectBagImplTestSuite, test_write_to_buffer)
{
  rosbag_direct_write::VectorBuffer buffer({0});
  ASSERT_EQ(1u, buffer.size());
  // Test implicit size (must be types with .begin and .end functions)
  std::string a("test");
  rosbag_direct_write::VectorBuffer b;
  rosbag_direct_write::impl::write_to_buffer(b, a);
  ASSERT_EQ(rosbag_direct_write::VectorBuffer({'t', 'e', 's', 't'}), b);
  rosbag_direct_write::impl::write_to_buffer(buffer, b);
  ASSERT_EQ(5u, buffer.size());
  // Test explicit size (implicitly cast to buffer's type)
  uint32_t c = 1;
  rosbag_direct_write::VectorBuffer d;
  rosbag_direct_write::impl::write_to_buffer(d, c, 4);
  ASSERT_EQ(rosbag_direct_write::VectorBuffer({0x1, 0x0, 0x0, 0x0}), d);
  // Test pointer version and extending a buffer
  rosbag_direct_write::impl::write_ptr_to_buffer(buffer, &c, 4);
  ASSERT_EQ(9u, buffer.size());
  rosbag_direct_write::VectorBuffer expected_buffer(
    {0, 't', 'e', 's', 't', 0x1, 0x0, 0x0, 0x0});
  ASSERT_EQ(expected_buffer, buffer);
}

TEST(DirectBagImplTestSuite, test_write_version)
{
  rosbag_direct_write::VectorBuffer buffer;
  rosbag_direct_write::impl::write_version(buffer);
  rosbag_direct_write::VectorBuffer expected;
  rosbag_direct_write::impl::write_to_buffer(
    expected, std::string("#ROSBAG V2.0\n"));
  ASSERT_EQ(expected, buffer);
}

TEST(DirectBagImplTestSuite, test_write_header)
{
  std::map<std::string, std::string> faux_header;
  faux_header["field1"] = "value1";
  faux_header["field2"] = "value2";
  rosbag_direct_write::VectorBuffer buffer;
  rosbag_direct_write::impl::write_header(buffer, faux_header);
  // Construct expected serialized header
  rosbag_direct_write::VectorBuffer expected;
  // The format is (from rosbag spec):
  //   total header length then
  //   for each key-value pair:
  //     pair length then
  //     key then
  //     '=' then
  //     value
  std::string pair1("field1=value1");
  std::string pair2("field2=value2");
  size_t header_len = 4 + pair1.length() + 4 + pair2.length();
  rosbag_direct_write::impl::write_to_buffer(expected, header_len, 4);
  rosbag_direct_write::impl::write_to_buffer(expected, pair1.length(), 4);
  rosbag_direct_write::impl::write_to_buffer(expected, pair1);
  rosbag_direct_write::impl::write_to_buffer(expected, pair2.length(), 4);
  rosbag_direct_write::impl::write_to_buffer(expected, pair2);
  // Compare expected with result
  ASSERT_EQ(expected, buffer);
}

TEST(DirectBagImplTestSuite, test_write_file_header_record)
{
  rosbag_direct_write::VectorBuffer buffer;
  rosbag_direct_write::impl::write_file_header_record(buffer, 4, 2, 4096);
  // Construct expected serialized file header
  rosbag_direct_write::VectorBuffer expected;
  rosbag_direct_write::impl::write_version(expected);
  // The format is (from rosbag spec):
  //   total header length then
  //   for each key-value pair:
  //     pair length then
  //     key then
  //     '=' then
  //     value
  std::vector<std::string> expected_pairs({
    std::string("chunk_count=\x2\0\0\0", 16),
    std::string("conn_count=\x4\0\0\0", 15),
    std::string("index_pos=\0\x10\0\0\0\0\0\0", 18),
    std::string("op=\x3", 4),
  });
  size_t header_len = 0;
  for (auto &pair : expected_pairs)
  {
    header_len += 4;
    header_len += pair.length();
  }
  rosbag_direct_write::impl::write_to_buffer(expected, header_len, 4);
  for (auto &pair : expected_pairs)
  {
    rosbag_direct_write::impl::write_to_buffer(expected, pair.length(), 4);
    rosbag_direct_write::impl::write_to_buffer(expected, pair);
  }
  // Add the padding up to 4096 to the expected
  size_t padding_len = 4096 - 4 - expected.size();
  rosbag_direct_write::impl::write_to_buffer(expected, padding_len, 4);
  if (padding_len > 0)
  {
    std::string padding(padding_len, ' ');
    rosbag_direct_write::impl::write_to_buffer(expected, padding);
  }
  ASSERT_EQ(4096u, expected.size());
  ASSERT_EQ(4096u, buffer.size());
  // Compare expected with result
  ASSERT_EQ(expected, buffer);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
