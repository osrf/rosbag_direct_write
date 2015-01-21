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

// Defined in dependencies header and is a shared_ptr from either boost or std
using rosbag_direct_write::shared_ptr;

typedef std::vector<uint8_t, aligned_allocator<uint8_t, 4096>> VectorBuffer;

class ROSBAG_DIRECT_WRITE_DECL DirectFile
{
public:
  DirectFile(std::string filename);
  ~DirectFile();

  size_t get_offset() const;
  void seek(uint64_t pos) const;

  size_t write_buffer(VectorBuffer &buffer);
  size_t write_data(const uint8_t *start, size_t length);

  std::string get_filename() {return this->filename_;}

  bool is_open();

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

class ROSBAG_DIRECT_WRITE_DECL DirectBag
{
public:
  DirectBag(std::string filename);
  DirectBag();

  ~DirectBag();

  void open(std::string filename);

  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             shared_ptr<T const> const& msg,
             shared_ptr<ros::M_string> connection_header = 0)
  {
    write(topic, time, *msg, connection_header);
  }

  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             shared_ptr<T> const& msg,
             shared_ptr<ros::M_string> connection_header = 0)
  {
    write(topic, time, *msg, connection_header);
  }

  template<class T>
  void write(std::string const& topic,
             ros::Time const& time,
             T const& msg,
             shared_ptr<ros::M_string> connection_header = 0);

  void close();
  bool is_open() const;

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

  size_t file_header_record_offset_;

  std::map<std::string, uint32_t> topic_connection_ids_;
  std::map<ros::M_string, uint32_t> header_connection_ids_;

  std::map<uint32_t, shared_ptr<rosbag::ConnectionInfo>> connections_;
  std::map<uint32_t, std::multiset<rosbag::IndexEntry> > connection_indexes_;
  std::vector<rosbag::ChunkInfo> chunk_infos_;

  VectorBuffer chunk_buffer_;

  uint32_t next_conn_id_;
};

} // namespace rosbag_direct_write

#include "direct_bag_impl.h"

#endif  /* ROSBAG_BAG_DIRECT_BAG_DIRECT_H */
