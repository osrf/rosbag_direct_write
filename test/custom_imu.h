#include <rosbag/bag.h>
#include <geometry_msgs/PointStamped.h>

#include "rosbag_direct_write/direct_bag.h"

// Custom version of geometry_msgs::PointStamped
typedef struct __custom_imu
{
  ros::Time stamp;
  double x;
  double y;
  double z;
} __custom_imu;

namespace ros
{
namespace message_traits
{
template<>
struct MD5Sum<__custom_imu>
{
  static const char* value()
  {
    return MD5Sum<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_imu&)
  {
    return value();
  }

  static const uint64_t static_value1 = \
    MD5Sum<geometry_msgs::PointStamped>::static_value1;
  static const uint64_t static_value2 = \
    MD5Sum<geometry_msgs::PointStamped>::static_value2;
};

template<>
struct DataType<__custom_imu>
{
  static const char* value()
  {
    return DataType<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_imu&)
  {
    return value();
  }
};

template<>
struct Definition<__custom_imu>
{
  static const char* value()
  {
    return Definition<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_imu&)
  {
    return value();
  }
};

template<> struct HasHeader<__custom_imu> : TrueType {};
} /* namespace message_traits */
namespace serialization
{
template<>
struct Serializer<__custom_imu>
{
template<typename Stream>
  inline static void
  write(Stream &stream, const __custom_imu& m)
  {
    std_msgs::Header header;
    header.stamp = m.stamp;
    stream.next(header);
    stream.next(m.x);
    stream.next(m.y);
    stream.next(m.z);
  }
  inline static uint32_t
  serializedLength(const __custom_imu& m)
  {
    geometry_msgs::PointStamped ps_msg;
    return ros::serialization::serializationLength(ps_msg);
  }
};
} /* namespace serialization */
} /* namespace ros */

template<> void
rosbag_direct_write::serialize_to_buffer<__custom_imu>(
  VectorBuffer &buffer,
  const __custom_imu &msg)
{
  geometry_msgs::PointStamped ps_msg;
  size_t buffer_length = ros::serialization::serializationLength(ps_msg);
  // Save currect buffer position and resize
  size_t start_offset = buffer.size();
  buffer.resize(buffer.size() + buffer_length);
  // Create a OStream
  ros::serialization::OStream s(buffer.data() + start_offset, buffer_length);
  // Write out everything but image data
  std_msgs::Header header;
  header.stamp = msg.stamp;
  ros::serialization::serialize(s, header);
  ros::serialization::serialize(s, msg.x);
  ros::serialization::serialize(s, msg.y);
  ros::serialization::serialize(s, msg.z);
}
