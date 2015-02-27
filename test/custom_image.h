#include <rosbag/bag.h>
#include <sensor_msgs/Image.h>
#include <std_msgs/Header.h>

#include "rosbag_direct_write/direct_bag.h"

#ifndef STD_MSGS_HEADER
#define STD_MSGS_HEADER std_msgs::Header
#endif

#ifndef ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
#define ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE ros
#endif

// Custom version of sensor_msgs::Image
typedef struct __custom_image
{
  ros::Time stamp;
  rosbag_direct_write::VectorBuffer data;
} __custom_image;

namespace rosbag_direct_write
{
template <>
bool has_direct_data(const __custom_image &msg)
{
  ROSBAG_DIRECT_WRITE_UNUSED(msg);
  return true;
}

static const size_t ksize_of_empty_image_message = \
  ros::serialization::serializationLength(sensor_msgs::Image());

template <> SerializationReturnCode
serialize_to_buffer(VectorBuffer& buffer, const __custom_image& msg,
                    size_t step)
{
  ROSBAG_DIRECT_WRITE_UNUSED(step);
  // Calculate how much additional buffer we will need for the message
  size_t needed_buffer = ksize_of_empty_image_message;
  // No need to subtract the size of data, since data is empty in the
  // temporary image message.
  // Subtract for the size of size of image data (an int32)
  needed_buffer -= 4;
  // Save currect buffer position and resize
  size_t start_offset = buffer.size();
  buffer.resize(buffer.size() + needed_buffer);
  // Create a OStream
  ros::serialization::OStream s(buffer.data() + start_offset, needed_buffer);
  // Write out everything but image data
  STD_MSGS_HEADER header;
  header.stamp = msg.stamp;
  ros::serialization::serialize(s, header);
  ros::serialization::serialize(s, 0/* msg.height */);
  ros::serialization::serialize(s, 0/* msg.width */);
  ros::serialization::serialize(s, std::string("")/* msg.encoding */);
  ros::serialization::serialize(s, (uint8_t)false/* msg.is_bigendian */);
  ros::serialization::serialize(s, 1/* msg.step */);
  // Write the size of the data which comes next in serialize_to_file
  impl::write_to_buffer(buffer, msg.data.size(), 4);
  return SerializationReturnCode::SERIALIZE_TO_FILE_NEXT;
}

template <> SerializationReturnCode
serialize_to_file(DirectFile& file, const __custom_image& msg, size_t step)
{
  ROSBAG_DIRECT_WRITE_UNUSED(step);
  assert((file.get_offset() % 4096) == 0);
  // Write the data directly to the file from the memory
  file.write_data(msg.data.data(), msg.data.size());
  return SerializationReturnCode::DONE;
}
} /* namespace rosbag_direct_write */

namespace ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
{
namespace message_traits
{
template<>
struct MD5Sum<__custom_image>
{
  static const char* value()
  {
    return MD5Sum<sensor_msgs::Image>::value();
  }
  static const char* value(const __custom_image&)
  {
    return value();
  }

  static const uint64_t static_value1 = \
    MD5Sum<sensor_msgs::Image>::static_value1;
  static const uint64_t static_value2 = \
    MD5Sum<sensor_msgs::Image>::static_value2;
};

template<>
struct DataType<__custom_image>
{
  static const char* value()
  {
    return DataType<sensor_msgs::Image>::value();
  }
  static const char* value(const __custom_image&)
  {
    return value();
  }
};

template<>
struct Definition<__custom_image>
{
  static const char* value()
  {
    return Definition<sensor_msgs::Image>::value();
  }
  static const char* value(const __custom_image&)
  {
    return value();
  }
};

template<> struct HasHeader<__custom_image> : TrueType {};
} /* namespace message_traits */
namespace serialization
{
template<>
struct Serializer<__custom_image>
{
template<typename Stream>
  inline static void
  write(Stream &stream, const __custom_image& m)
  {
    ROSBAG_DIRECT_WRITE_UNUSED(stream);
    ROSBAG_DIRECT_WRITE_UNUSED(m);
    throw std::runtime_error("write shouldn't get called");
  }
  inline static uint32_t
  serializedLength(const __custom_image& m)
  {
    sensor_msgs::Image image_msg;
    return ros::serialization::serializationLength(image_msg) + m.data.size();
  }
};
} /* namespace serialization */
} /* namespace ros */
