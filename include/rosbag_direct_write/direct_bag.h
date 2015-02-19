/* Copyright 2014 Open Source Robotics Foundation, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ROSBAG_BAG_DIRECT_BAG_DIRECT_H
#define ROSBAG_BAG_DIRECT_BAG_DIRECT_H

#include <atomic>
#include <memory>
#include <rosbag_direct_write/direct_bag_dependencies.h>

namespace rosbag_direct_write
{

#include "aligned_allocator.h"

// Default chunk size.
static const size_t kdefault_chunk_threshold = 768 * 1024;  // 768kb

// Defined in dependencies header and is a shared_ptr from either boost or std
using rosbag_direct_write::shared_ptr;

/// This is a uint8_t based, 4096 aligned std::vector container
typedef std::vector<uint8_t, aligned_allocator<uint8_t, 4096>> VectorBuffer;

/// This DirectFile class provides a simplified interface for writing to files
/// using the O_DIRECT (or F_NOCACHE) flag for high performance writing.
class ROSBAG_DIRECT_WRITE_DECL DirectFile
{
public:
  /// Opens the given file with O_DIRECT, overwriting an existing file.
  DirectFile(std::string filename);
  ~DirectFile();

  /// Returns the cursor location as a number of bytes from the file start.
  size_t get_offset() const;
  /// Moves the cursor location to the given position.
  void seek(uint64_t pos) const;

  /// Writes the given VectorBuffer to the file.
  size_t write_buffer(VectorBuffer &buffer);
  /// Writes the given uint8_t array for given length bytes to the file.
  size_t write_data(const uint8_t *start, size_t length);

  /// Returns the current file's name.
  std::string get_filename() const {return this->filename_;}

  /// Returns true if the file is open, otherwise false.
  bool is_open();

  /// Closes the file, flushing the output.
  void close();

private:
  std::string filename_;
#if __APPLE__
  FILE *file_pointer_;
#else
  int file_descriptor_;
#endif
  bool open_;

};

/// The DirectBag class provides an interface for writing ROS messages or
/// serializable data structures to a ROS bag file using O_DIRECT.
class ROSBAG_DIRECT_WRITE_DECL DirectBag
{
public:
  /// Creates a new ROS bag file, overwriting an existing one.
  DirectBag(std::string filename,
            size_t chunk_threshold=kdefault_chunk_threshold);
  /// Creates an uninitialized DirectBag; open must be called before use.
  DirectBag();
  ~DirectBag();

  /// Opens a ROS bag file at the given location for writing; overwrites.
  /**
   * Opens a ROS bag file at the given location, overwriting any existing
   * ROS bag file with the same name.
   *
   * Also optionally sets the chunk threshold.
   * The chunk threshold is the size which when met or exceeded, prompts the
   * close of the current chunk and start of the next one.
   * The chunk threshold is just one of two scenarios in which a chunk will
   * be finished.
   * The other scenario in which chunks are finished is when ever a message
   * which utilizes the zero-copy write optimization is written to the bag.
   * This limitation exists because the size of the chunk must be written
   * at the beginning of the chunk, so by writing directly to the file these
   * messages force the size of the chunk to be determined, so no future
   * messages can be added to the chunk.
   *
   * @param filename The path to the ROS bag file to be created.
   * @param chunk_threshold The size at which chunks are closed,
   *                        defaults to 768kb.
   *
   */
  void open(std::string filename,
            size_t chunk_threshold=kdefault_chunk_threshold);

  /// Writes a given data structure to the bag file.
  /**
   * The write function takes a topic to write to, a time that is was received,
   * a shared pointer to the data structure, and an optional connection_header.
   *
   * @param topic The topic to write the message to in the bag file.
   * @param time The received time of the message (used for playback).
   * @param msg A shared pointer to the message object to serialize and write.
   * @param connection_header Optional ROS connection header.
   *
   */
  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             shared_ptr<T const> const& msg,
             shared_ptr<ros::M_string> connection_header = 0)
  {
    write(topic, time, *msg, connection_header);
  }

  /// Writes a given data structure to the bag file.
  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             shared_ptr<T> const& msg,
             shared_ptr<ros::M_string> connection_header = 0)
  {
    write(topic, time, *msg, connection_header);
  }

  /// Writes a given data structure to the bag file.
  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             T const& msg,
             shared_ptr<ros::M_string> connection_header = 0);

  /// Closes the current file, finishing the file with indexing and flushing.
  void close();
  /// Returns true if the file is open, otherwise false.
  bool is_open() const;
  /// Returns the size, in bytes, of the chunk threshold.
  size_t get_chunk_threshold() const;
  /// Returns the bag file name, returns "" if the bag is not open.
  std::string get_bag_file_name() const;

private:
  template<class T> shared_ptr<rosbag::ConnectionInfo>
  get_connection_info(std::string const &topic,
                      T const& msg,
                      shared_ptr<ros::M_string> connection_header);

  template<class T> void
  write_message_data_record(uint32_t conn_id,
                            ros::Time const& time,
                            T const& msg,
                            rosbag::ChunkInfo &chunk_info);

  std::string filename_;
  shared_ptr<DirectFile> file_;

  std::atomic<bool> open_;

  std::map<std::string, uint32_t> topic_connection_ids_;
  std::map<ros::M_string, uint32_t> header_connection_ids_;

  std::map<uint32_t, shared_ptr<rosbag::ConnectionInfo>> connections_;
  std::map<uint32_t, std::multiset<rosbag::IndexEntry> > connection_indexes_;
  std::vector<rosbag::ChunkInfo> chunk_infos_;

  VectorBuffer chunk_buffer_;  // Buffer for chunk data
  size_t current_chunk_position_;  // pos of current chunk in the chunk_buffer_
  shared_ptr<rosbag::ChunkInfo> current_chunk_info_;  // current ChunkInfo
  std::map<uint32_t, std::multiset<rosbag::IndexEntry>>
    current_chunk_connection_indexes_;  // Connection indexes for current chunk
  size_t chunk_threshold_;  // Size at which chunks are ended

  uint32_t next_conn_id_;
};

/// Macro used to prevent compiler warnings, should optimize out.
#define ROSBAG_DIRECT_WRITE_UNUSED(x) (void)(x)

/// Default template to indicate if a message type will use direct data or not.
template<class T> bool
has_direct_data()
{
  return false;
}

/// Exception which is raised when a default serialize function is used.
class not_implemented_exception : public std::logic_error
{
public:
  explicit not_implemented_exception(const char * what_arg)
  : std::logic_error(what_arg) {}
};

enum class SerializationReturnCode
{
  DONE,
  SERIALIZE_TO_BUFFER_NEXT,
  SERIALIZE_TO_FILE_NEXT,
};

/// Default serialize_to_buffer template function, should always be overridden.
template<class T> SerializationReturnCode
serialize_to_buffer(VectorBuffer &buffer, const T &msg,
                    size_t serialization_step)
{
  ROSBAG_DIRECT_WRITE_UNUSED(buffer);
  ROSBAG_DIRECT_WRITE_UNUSED(msg);
  ROSBAG_DIRECT_WRITE_UNUSED(serialization_step);
  throw not_implemented_exception("serialize_to_buffer not implemented");
  return SerializationReturnCode::DONE;
}

/// Default serialize_to_file template function, should always be overridden.
template<class T> SerializationReturnCode
serialize_to_file(DirectFile &file, const T &msg,
                  size_t serialization_step)
{
  ROSBAG_DIRECT_WRITE_UNUSED(file);
  ROSBAG_DIRECT_WRITE_UNUSED(msg);
  ROSBAG_DIRECT_WRITE_UNUSED(serialization_step);
  throw not_implemented_exception("serialize_to_file not implemented");
  return SerializationReturnCode::DONE;
}

} // namespace rosbag_direct_write

#include "direct_bag_impl.h"

#endif  /* ROSBAG_BAG_DIRECT_BAG_DIRECT_H */
