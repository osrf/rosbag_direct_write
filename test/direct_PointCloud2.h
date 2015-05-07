#include <std_msgs/Header.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/PointField.h>

#include "rosbag_direct_write/direct_bag.h"

#ifndef STD_MSGS_HEADER
#define STD_MSGS_HEADER std_msgs::Header
#endif

#ifndef ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
#define ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE ros
#endif

namespace test_direct_bag
{
struct AlignedPointCloud2
{
  STD_MSGS_HEADER header;
  uint32_t height;
  uint32_t width;
  std::vector<sensor_msgs::PointField> fields;
  bool is_bigendian;
  uint32_t point_step;
  uint32_t row_step;
  rosbag_direct_write::VectorBuffer data;
  bool is_dense;
};
}  // namespace test_direct_bag

namespace rosbag_direct_write
{
template <> bool
has_direct_data(const test_direct_bag::AlignedPointCloud2 &msg)
{
  return msg.data.size() >= 4096;
}

template<> size_t
alignment_adjustment(const test_direct_bag::AlignedPointCloud2 &msg)
{
  size_t adjustment = 1;  // Start with a byte for the trailing is_dense byte.
  adjustment += msg.data.size() % 4096;
  return adjustment;
}

size_t
calculate_direct_writable_bytes(const test_direct_bag::AlignedPointCloud2 &msg)
{
  return (msg.data.size() / 4096) * 4096;
}

template <> SerializationReturnCode
serialize_to_buffer(VectorBuffer& buffer,
                    const test_direct_bag::AlignedPointCloud2& msg,
                    size_t step)
{
  size_t ser_len = 0;
  size_t start_offset = 0;
  size_t direct_writable_bytes = 0;
  size_t left_over_bytes = 0;
  shared_ptr<ros::serialization::OStream> s;
  switch (step)
  {
    case 0:  // First stage of the serialization
      // Calculate how much additional buffer we will need for the message
      ser_len = ros::serialization::serializationLength(msg);
      ser_len -= msg.data.size();
      // Subtract for the size of size of image data (an int32)
      ser_len -= 4;
      // Subtract the size of the fields which come after the "data" field
      ser_len -= ros::serialization::serializationLength(msg.is_dense);
      // Save correct buffer position and resize
      start_offset = buffer.size();
      buffer.resize(buffer.size() + ser_len);
      // Create a OStream
      s.reset(new ros::serialization::OStream(buffer.data() + start_offset,
                                              ser_len));
      // Write out everything but image data
      ros::serialization::serialize(*s, msg.header);
      ros::serialization::serialize(*s, msg.height);
      ros::serialization::serialize(*s, msg.width);
      ros::serialization::serialize(*s, msg.fields);
      ros::serialization::serialize(*s, (uint8_t)msg.is_bigendian);
      ros::serialization::serialize(*s, msg.point_step);
      ros::serialization::serialize(*s, msg.row_step);
      // Write the size of the data which comes next in serialize_to_file
      impl::write_to_buffer(buffer, msg.data.size(), 4);
      return SerializationReturnCode::SERIALIZE_TO_FILE_NEXT;
    case 2:  // Second call, after file writing
      // First account for left over bytes from the data field which could
      // not be directly written.
      direct_writable_bytes = calculate_direct_writable_bytes(msg);
      left_over_bytes = msg.data.size() - direct_writable_bytes;
      if (left_over_bytes != 0)
      {
        // Write left over bytes to the buffer.
        impl::write_ptr_to_buffer(buffer,
                                  msg.data.data() + direct_writable_bytes,
                                  left_over_bytes);
      }
      // Calculate the size of the buffer needed
      ser_len = ros::serialization::serializationLength(msg.is_dense);
      // Save correct buffer position and resize
      start_offset = buffer.size();
      buffer.resize(buffer.size() + ser_len);
      // Create a OStream
      s.reset(new ros::serialization::OStream(buffer.data() + start_offset,
                                              ser_len));
      // Write out the last item
      ros::serialization::serialize(*s, (uint8_t)msg.is_dense);
      return SerializationReturnCode::DONE;
    default:
      // This should not occur
      throw std::runtime_error(
        "serialize_to_buffer<test_direct_bag::AlignedPointCloud2> "
        "called out of order.");
  }
  // This should not occur
  return SerializationReturnCode::DONE;
}

template <> SerializationReturnCode
serialize_to_file(DirectFile& file,
                  const test_direct_bag::AlignedPointCloud2& msg,
                  size_t step)
{
  ROSBAG_DIRECT_WRITE_UNUSED(step);
  assert((file.get_offset() % 4096) == 0);
  assert(step == 1);  // Make sure this is step 2 of 3
  size_t size_to_write = msg.data.size();
  if ((msg.data.size() % 4096) != 0)
  {
    // In this case we need to write as much as possible and leave the
    // rest for tail buffering.
    size_to_write = calculate_direct_writable_bytes(msg);
  }
  // Write the data directly to the file from the given memory address.
  file.write_data(msg.data.data(), size_to_write);
  // Pass back to serialize_to_buffer<test_direct_bag::AlignedPointCloud2>
  return SerializationReturnCode::SERIALIZE_TO_BUFFER_NEXT;
}
} /* namespace rosbag_direct_write */

namespace ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
{
namespace message_traits
{
template<>
struct MD5Sum<test_direct_bag::AlignedPointCloud2>
{
  static const char* value()
  {
    return MD5Sum<sensor_msgs::PointCloud2>::value();
  }
  static const char* value(const test_direct_bag::AlignedPointCloud2&)
  {
    return value();
  }

  static const uint64_t static_value1 = \
    MD5Sum<sensor_msgs::PointCloud2>::static_value1;
  static const uint64_t static_value2 = \
    MD5Sum<sensor_msgs::PointCloud2>::static_value2;
};

template<>
struct DataType<test_direct_bag::AlignedPointCloud2>
{
  static const char* value()
  {
    return DataType<sensor_msgs::PointCloud2>::value();
  }
  static const char* value(const test_direct_bag::AlignedPointCloud2&)
  {
    return value();
  }
};

template<>
struct Definition<test_direct_bag::AlignedPointCloud2>
{
  static const char* value()
  {
    return Definition<sensor_msgs::PointCloud2>::value();
  }
  static const char* value(const test_direct_bag::AlignedPointCloud2&)
  {
    return value();
  }
};

template<> struct HasHeader<test_direct_bag::AlignedPointCloud2> : TrueType {};
} /* namespace message_traits */
namespace serialization
{
template<>
struct Serializer<test_direct_bag::AlignedPointCloud2>
{
template<typename Stream>
  inline static void
  write(Stream &stream, const test_direct_bag::AlignedPointCloud2& m)
  {
    ROSBAG_DIRECT_WRITE_UNUSED(stream);
    ROSBAG_DIRECT_WRITE_UNUSED(m);
    throw std::runtime_error("write shouldn't get called");
  }
  inline static uint32_t
  serializedLength(const test_direct_bag::AlignedPointCloud2& m)
  {
    sensor_msgs::PointCloud2 pc2_msg;
    pc2_msg.header = m.header;
    pc2_msg.fields = m.fields;
    return ros::serialization::serializationLength(pc2_msg) + m.data.size();
  }
};
} /* namespace serialization */
} /* namespace ros */
