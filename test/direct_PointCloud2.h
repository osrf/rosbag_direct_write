#include <sensor_msgs/PointCloud2.h>

#include "rosbag_direct_write/direct_bag.h"

namespace rosbag_direct_write
{
template <>
bool has_direct_data<sensor_msgs::PointCloud2>() {
  return true;
}

template <> SerializationReturnCode
serialize_to_buffer(VectorBuffer& buffer, const sensor_msgs::PointCloud2& msg,
                    size_t step)
{
  size_t ser_len = 0;
  size_t start_offset = 0;
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
      ros::serialization::serialize(*s, msg.is_bigendian);
      ros::serialization::serialize(*s, msg.point_step);
      ros::serialization::serialize(*s, msg.row_step);
      // Write the size of the data which comes next in serialize_to_file
      impl::write_to_buffer(buffer, msg.data.size(), 4);
      return SerializationReturnCode::SERIALIZE_TO_FILE_NEXT;
    case 2:  // Second call, after file writing
      // Calculate the size needed
      ser_len = ros::serialization::serializationLength(msg.is_dense);
      // Save correct buffer position and resize
      start_offset = buffer.size();
      buffer.resize(buffer.size() + ser_len);
      // Create a OStream
      s.reset(new ros::serialization::OStream(buffer.data() + start_offset,
                                              ser_len));
      // Write out the last item
      ros::serialization::serialize(*s, msg.is_dense);
      return SerializationReturnCode::DONE;
    default:
      // This should not occur
      throw std::runtime_error(
        "serialize_to_buffer<sensor_msgs::PointCloud2> called out of order.");
  }
  return SerializationReturnCode::SERIALIZE_TO_FILE_NEXT;
}

template <> SerializationReturnCode
serialize_to_file(DirectFile& file, const sensor_msgs::PointCloud2& msg,
                  size_t step)
{
  assert((file.get_offset() % 4096) == 0);
  assert(step == 1);  // Make sure this is step 2 of 3
  // Write the data directly to the file from the memory
  file.write_data(msg.data.data(), msg.data.size());
  // Pass back to serialize_to_buffer<sensor_msgs::PointCloud2>
  return SerializationReturnCode::SERIALIZE_TO_BUFFER_NEXT;
}
} /* namespace rosbag_direct_write */
