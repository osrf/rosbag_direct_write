# rosbag_direct_write

This repository contains one package, `rosbag_direct_write`, which provides a similar interface to the `rosbag::Bag` interface, but only for writing rosbag files.
The purpose of this package is to allow for high performance rosbag file creation by:

- avoiding seeking during writing
- using `O_DIRECT` and writing in 4KB aligned memory segments
- by allowing the user to write directly to the rosbag file in special cases to prevent `memcpy`'s

## Basic API usage

This package depends on the `rosbag_storage` package and provieds the `DirectBag` class.
The `DirectBag` class provides a few important functions:

- `open`: opens a rosbag, overwriting any existing rosbag
 - `filename`: path to the rosbag to open
 - `chunk_threshold`: size at which to break up rosbag chunks, default 768KB
- `write`: writes a given message, to a given topic, with a given received time
 - `topic`: ROS topic to associate the message with
 - `time`: ros::Time when the message was received
 - `msg`: message or structure to be written to the bag
- `close`: closes the rosbag, immediately finishing any open chunks

These are the most important functions, but there are a few others.
See the header at `include/rosbag_direct_write/direct_bag.h` for more details.

## Features

`rosbag_direct_write` will always open rosbag files with `O_DIRECT`, or `F_NOCACHE` where needed, to make use of the more efficient writing.
In the default case, messages are serialized and stored in an in-memory buffer until the chunk threshold is met.
When the chunk threshold is met, the chunk is "finished" and written to the disk, taking advantage of `O_DIRECT`.
This will be faster out-of-the-box than the `rosbag::Bag` API in most cases, even if the Zero-copy direct write mechanism is not used.

### Zero-copy Direct Writing

If the user of the API so chooses, they may make use of the zero-copy direct writing of messages to the rosbag file.
This is most useful when the user has a large segment of data, like in an image or in a point cloud, and they do not wish to copy that data into an intermediate in-memory buffer.
This can help avoid necessary `memcpy`'s and it can also allow the kernel to make use of DMA optimizations, especially if the data to be recorded does not start in main memory.

In order to take advantage of this feature, the user must define three function template specializations for their message type.
Here is an example for `sensor_msgs/PointCloud2`:

```c++
#include <sensor_msgs/PointCloud2.h>

#include <rosbag_direct_write/direct_bag.h>

namespace rosbag_direct_write
{

template <>
bool has_direct_data(const sensor_msgs::PointCloud2 &point_cloud)
{
  ROSBAG_DIRECT_WRITE_UNUSED(point_cloud);  // Prevents unused argument warning
  return true;
}

template <>
size_t alignment_adjustment(const sensor_msgs::PointCloud2 &point_cloud)
{
  ROSBAG_DIRECT_WRITE_UNUSED(point_cloud);
  return 1;  // To account for the trailing is_dense boolean.
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
  return SerializationReturnCode::DONE;
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
```

Let's break that down, first the user must create the function `has_direct_data`.
This function is used by `rosbag_direct_write` to determine if a message should be handled as a normal message and be serialized to an in-memory buffer, or if it should be treated like a direct write message.
To use this feature, you'll need to provide a specialization of this function which returns `true` for messages which have elements which can be written with `O_DIRECT`.

Next, you'll need to define the two serialization functions `serialize_to_buffer` and `serialize_to_file`.
`serialize_to_buffer` is always called first, but what happens next is determined by what `serialize_to_buffer` returns.
If `serialize_to_buffer` returns `SerializationReturnCode::DONE`, then no other functions will be called, but if `SerializationReturnCode::SERIALIZE_TO_FILE_NEXT` is returned, then the `serialize_to_file` function will be called, and if `SerializationReturnCode::SERIALIZE_TO_BUFFER_NEXT` is returned, then the `serialize_to_buffer` function will be called again.
Each time one of these functions is called, the `step` parameter is incremented, allowing the functions to know which step of the message serialization they are in.
In this respect the `sensor_msgs/PointCloud2` message is a good example, because it requires two calls to `serialize_to_buffer` and one call to `serialize_to_file`.

The `serialize_to_buffer` function gets a memory aligned buffer (a `std::vector`), the message to be serialized, and the step (`0` indexed).
The user is responsible for serializing the incoming message structure correctly, but typically the first call to `serialize_to_buffer` writes the message data up to the part of the message the user wants to direct write, then returns `SerializationReturnCode::SERIALIZE_TO_FILE_NEXT`.

The `serialize_to_file` function gets a `DirectFile` object (it has a `write` function), the message being serialized, and the step.
At this point it is important to note what the rules about using `O_DIRECT` are:

- The address of the object passed to `DirectFile.write` must be a multiple of `4096`
- The size of data to write must be a multiple of `4096`
- The current position in the `DirectFile` (use `DirectFile.get_offset` to check) must be a multiple of `4096`

The number `4096` is the typical page size for most systems you will encounter.

The user is responsible for ensuring these constraints are met.
To ensure that the `DirectFile`'s offset is a multiple of `4096`, `rosbag_direct_write` will make sure that the last message in a chunk ends on a `4096` boundary, but this can be adjusted using the optional `alignment_adjustment` template specialization.
This adjustment necessary when the direct written part of the message is not at the end of the message, like for PointCloud2.
However, sensor_msgs::Image's last field is the image data and therefore does not require any adjustment to the alignment.
Getting the alignment right is accomplished by using a bogus message header entry to adjust the start of the last message in the chunk to be exactly the length of the last message short of a `4096` boundary.

At this point it is worth mentioning the other special condition for ending chunks in `rosbag_direct_write`'s implementation.
Normally, a chunk is only ended if the chunk threshold is reached or if the rosbag is closed, but in `rosbag_direct_write` they are additionally finished anytime a direct write message has been written.
This requirement is in place to allow direct messages to end on a `4096` boundary, allowing the user to more easily meet the above `O_DIRECT` constraints.

So, if all of the user's messages are using direct write, then there will be one message per chunk.
Therefore, there is a trade-off between the use of direct write and the number of chunks, which becomes a trade-off between write performance and playback performance.
The more chunks there are, the more work that has to be done when playing a rosbag back.
Therefore, typically it only makes sense to use the direct write mechanism when very large data needs to be written to the rosbag file, like in the case of Images and PointCloud2's.
