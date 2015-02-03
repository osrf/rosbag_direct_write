#include <rosbag/bag.h>
#include <geometry_msgs/PointStamped.h>

#include "rosbag_direct_write/direct_bag.h"
#include <std_msgs/Header.h>

#ifndef STD_MSGS_HEADER
#define STD_MSGS_HEADER std_msgs::Header
#endif

#ifndef ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
#define ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE ros
#endif

// Custom version of geometry_msgs::PointStamped
typedef struct __custom_point_stamped
{
  ros::Time stamp;
  double x;
  double y;
  double z;
} __custom_point_stamped;

namespace ROSBAG_DIRECT_WRITE_TEST_ROS_NAMESPACE
{
namespace message_traits
{
template<>
struct MD5Sum<__custom_point_stamped>
{
  static const char* value()
  {
    return MD5Sum<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_point_stamped&)
  {
    return value();
  }

  static const uint64_t static_value1 = \
    MD5Sum<geometry_msgs::PointStamped>::static_value1;
  static const uint64_t static_value2 = \
    MD5Sum<geometry_msgs::PointStamped>::static_value2;
};

template<>
struct DataType<__custom_point_stamped>
{
  static const char* value()
  {
    return DataType<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_point_stamped&)
  {
    return value();
  }
};

template<>
struct Definition<__custom_point_stamped>
{
  static const char* value()
  {
    return Definition<geometry_msgs::PointStamped>::value();
  }
  static const char* value(const __custom_point_stamped&)
  {
    return value();
  }
};

template<> struct HasHeader<__custom_point_stamped> : TrueType {};
} /* namespace message_traits */
namespace serialization
{
template<>
struct Serializer<__custom_point_stamped>
{
template<typename Stream>
  inline static void
  write(Stream &stream, const __custom_point_stamped& m)
  {
    STD_MSGS_HEADER header;
    header.stamp = m.stamp;
    stream.next(header);
    stream.next(m.x);
    stream.next(m.y);
    stream.next(m.z);
  }
  inline static uint32_t
  serializedLength(const __custom_point_stamped& m)
  {
    ROSBAG_DIRECT_WRITE_UNUSED(m);
    geometry_msgs::PointStamped ps_msg;
    return ros::serialization::serializationLength(ps_msg);
  }
};
} /* namespace serialization */
} /* namespace ros */
