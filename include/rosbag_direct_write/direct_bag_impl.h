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

#ifndef ROSBAG_BAG_DIRECT_BAG_DIRECT_IMPL_H
#define ROSBAG_BAG_DIRECT_BAG_DIRECT_IMPL_H

#include <errno.h>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

#include <rosbag_direct_write/direct_bag_impl_dependencies.h>

#include "direct_bag.h"

namespace rosbag_direct_write
{

using rosbag::compression::CompressionType;
using namespace rosbag;

namespace impl
{

using std::string;

template<typename T> string
to_header_string(T const* field)
{
    return string((char*) field, sizeof(T));
}

template<> string
to_header_string<std::string>(std::string const* field)
{
  return string(*field);
}

template<> string
to_header_string<ros::Time>(ros::Time const* field)
{
    uint64_t packed_time = (((uint64_t) field->nsec) << 32) + field->sec;
    return to_header_string(&packed_time);
}

template<typename T> void
write_to_buffer(VectorBuffer &buffer, const T &data)
{
  buffer.insert(buffer.end(), data.begin(), data.end());
}

template<typename T> void
write_to_buffer(VectorBuffer &buffer, const T &data, size_t data_length)
{
  buffer.insert(buffer.end(),
                reinterpret_cast<const uint8_t *>(&data),
                reinterpret_cast<const uint8_t *>(&data) + data_length);
}

template<typename T> void
write_ptr_to_buffer(VectorBuffer &buffer, const T *data, size_t data_length)
{
  buffer.insert(buffer.end(),
                reinterpret_cast<const uint8_t *>(data),
                reinterpret_cast<const uint8_t *>(data) + data_length);
}

inline void
write_version(VectorBuffer &buffer)
{
  // Yes... rosbag uses all upper case variable names for non-macro constants.
  // Even ones like VERSION which are likely to collide, this is really stupid.
  string version = string("#ROSBAG V") + rosbag::VERSION + string("\n");

  write_to_buffer(buffer, version);
}

inline uint32_t
write_header(VectorBuffer &buffer, std::map<string, string> const &fields)
{
  ROSBAG_DIRECT_WRITE_HEADER_BUFFER_TYPE<uint8_t> header_buffer;
  uint32_t header_len;
  ros::Header::write(fields, header_buffer, header_len);
  write_to_buffer(buffer, header_len, 4);
  write_ptr_to_buffer(buffer, header_buffer.get(), header_len);
  return header_len;
}

inline void
write_file_header_record(
  VectorBuffer &buffer,
  uint32_t connection_count,
  uint32_t chunk_count,
  uint64_t index_data_position)
{
  write_version(buffer);
  size_t version_length = buffer.size();
  std::map<string, string> header;
  header[OP_FIELD_NAME]               = to_header_string(&OP_FILE_HEADER);
  header[INDEX_POS_FIELD_NAME]        = to_header_string(&index_data_position);
  header[CONNECTION_COUNT_FIELD_NAME] = to_header_string(&connection_count);
  header[CHUNK_COUNT_FIELD_NAME]      = to_header_string(&chunk_count);

  uint32_t header_len = write_header(buffer, header);
  assert(header_len < 4096);  // Anything else cannot be handled currently.
  // 4096 - header_len - version - header_len_len - data_len_len
  uint32_t data_len = 4096 - version_length - header_len - 4 - 4;
  write_to_buffer(buffer, data_len, 4);

  // Pad the file header record out
  if (data_len > 0)
  {
      string padding;
      padding.resize(data_len, ' ');
      write_to_buffer(buffer, padding);
  }
}

inline void
start_writing(VectorBuffer &buffer)
{
  write_file_header_record(buffer, 0, 0, 0);
}

inline void
write_connection_record(VectorBuffer &buffer,
                        shared_ptr<ConnectionInfo> &connection)
{
  std::map<string, string> header;
  header[OP_FIELD_NAME]         = to_header_string(&OP_CONNECTION);
  header[TOPIC_FIELD_NAME]      = connection->topic;
  header[CONNECTION_FIELD_NAME] = to_header_string(&connection->id);
  write_header(buffer, header);

  write_header(buffer, *connection->header);
}

inline void
write_connection_records(
  VectorBuffer &buffer,
  std::map<uint32_t, shared_ptr<ConnectionInfo>> &connections
)
{
  for (auto &connection_pair : connections) {
    auto connection = connection_pair.second;
    write_connection_record(buffer, connection);
  }
}

inline void
write_chunk_info_records(VectorBuffer &buffer,
                         std::vector<ChunkInfo> &chunk_infos)
{
  for(auto &chunk_info : chunk_infos)
  {
    // Write the chunk info header
    std::map<string, string> header;
    uint32_t chunk_connection_count = chunk_info.connection_counts.size();
    header[OP_FIELD_NAME]         = to_header_string(&OP_CHUNK_INFO);
    header[VER_FIELD_NAME]        = to_header_string(&CHUNK_INFO_VERSION);
    header[CHUNK_POS_FIELD_NAME]  = to_header_string(&chunk_info.pos);
    header[START_TIME_FIELD_NAME] = to_header_string(&chunk_info.start_time);
    header[END_TIME_FIELD_NAME]   = to_header_string(&chunk_info.end_time);
    header[COUNT_FIELD_NAME]      = to_header_string(&chunk_connection_count);

    write_header(buffer, header);

    // Write data length
    write_to_buffer(buffer, 8 * chunk_connection_count, 4);

    // Write the topic names and counts
    for (auto &connection_count_pair : chunk_info.connection_counts)
    {
      uint32_t connection_id = connection_count_pair.first;
      uint32_t count         = connection_count_pair.second;

      write_to_buffer(buffer, connection_id, 4);
      write_to_buffer(buffer, count, 4);
    }
  }
}

inline void
stop_writing(VectorBuffer &buffer,
             std::map<uint32_t,
             shared_ptr<ConnectionInfo>> &connections,
             std::vector<ChunkInfo> &chunk_infos)
{
  write_connection_records(buffer, connections);
  write_chunk_info_records(buffer, chunk_infos);
}

} /* namespace impl */

inline size_t
write_chunk_header(VectorBuffer &buffer,
                   uint32_t compressed_size,
                   uint32_t uncompressed_size)
{
  rosbag::ChunkHeader chunk_header;
  chunk_header.compression = rosbag::COMPRESSION_NONE;
  chunk_header.compressed_size   = compressed_size;
  chunk_header.uncompressed_size = uncompressed_size;

  std::map<std::string, std::string> header;
  header[rosbag::OP_FIELD_NAME] = impl::to_header_string(&rosbag::OP_CHUNK);
  header[rosbag::COMPRESSION_FIELD_NAME] = chunk_header.compression;
  header[rosbag::SIZE_FIELD_NAME] = \
    impl::to_header_string(&chunk_header.uncompressed_size);
  size_t header_len = impl::write_header(buffer, header);

  impl::write_to_buffer(buffer, chunk_header.compressed_size, 4);
  return header_len;
}

inline void
generate_message_record_header(uint32_t conn_id,
                               ros::Time const &time,
                               ros::M_string &header)
{
  header[rosbag::OP_FIELD_NAME]         = \
    impl::to_header_string(&rosbag::OP_MSG_DATA);
  header[rosbag::CONNECTION_FIELD_NAME] = impl::to_header_string(&conn_id);
  header[rosbag::TIME_FIELD_NAME]       = impl::to_header_string(&time);
}

inline size_t
write_data_message_record_header(VectorBuffer &buffer,
                                 uint32_t conn_id,
                                 ros::Time const &time)
{
  ros::M_string header;
  generate_message_record_header(conn_id, time, header);

  return impl::write_header(buffer, header);
}

DirectBag::DirectBag(std::string filename, size_t chunk_threshold)
: DirectBag()
{
  this->open(filename, chunk_threshold);
}

static VectorBuffer __temp_constant_buffer;
static const size_t kchunk_header_length_ = \
  write_chunk_header(__temp_constant_buffer, 0, 0);
static const size_t kmessage_header_length_ = \
  write_data_message_record_header(__temp_constant_buffer, 0, ros::Time(0));

DirectBag::DirectBag()
: filename_(""), open_(false), current_chunk_position_(0),
  current_chunk_info_(nullptr), chunk_threshold_(kdefault_chunk_threshold),
  next_conn_id_(0)
{}

void DirectBag::open(std::string filename, size_t chunk_threshold)
{
  if (this->is_open())
  {
    throw std::runtime_error(
      "open called on an already open DirectBag instance.");
  }
  file_.reset(new DirectFile(filename));
  filename_ = filename;
  chunk_threshold_ = chunk_threshold;
  VectorBuffer start_buffer;
  impl::start_writing(start_buffer);
  file_->write_buffer(start_buffer);
  chunk_buffer_.clear();
  open_.store(true);
}

DirectBag::~DirectBag()
{
  if (this->is_open())
  {
    this->close();
  }
}

inline void
write_index_records(
  VectorBuffer &buffer,
  std::map<uint32_t, std::multiset<rosbag::IndexEntry>>
    &chunk_connection_indexes
)
{
  for (auto &index_entry_pair : chunk_connection_indexes)
  {
    uint32_t connection_id = index_entry_pair.first;
    std::multiset<rosbag::IndexEntry> &index = index_entry_pair.second;

    // Write the index record header
    uint32_t index_size = index.size();
    ros::M_string header;
    header[rosbag::OP_FIELD_NAME]         = \
      impl::to_header_string(&rosbag::OP_INDEX_DATA);
    header[rosbag::CONNECTION_FIELD_NAME] = \
      impl::to_header_string(&connection_id);
    header[rosbag::VER_FIELD_NAME]        = \
      impl::to_header_string(&rosbag::INDEX_VERSION);
    header[rosbag::COUNT_FIELD_NAME]      = \
      impl::to_header_string(&index_size);
    impl::write_header(buffer, header);

    impl::write_to_buffer(buffer, index_size * 12, 4);

    // Write the index record data (pairs of timestamp and position in file)
    for(auto &entry : index)
    {
        impl::write_to_buffer(buffer, entry.time.sec, 4);
        impl::write_to_buffer(buffer, entry.time.nsec, 4);
        impl::write_to_buffer(buffer, entry.offset, 4);
    }
  }
}

inline void
write_chunk_end(VectorBuffer &buffer,
                std::map<uint32_t, std::multiset<rosbag::IndexEntry>>
                  &chunk_connection_indexes)
{
  // Write out the indexes and clear them
  write_index_records(buffer, chunk_connection_indexes);
}

void DirectBag::close()
{
  bool was_open = open_.exchange(false);
  if (!was_open)
  {
    throw std::runtime_error(
      "close called on an already closed DirectBag instance");
  }
  VectorBuffer stop_buffer;
  // Finish any open chunks
  if (current_chunk_info_ != nullptr)
  {
    // Calculate the chunk size
    size_t chunk_size = file_->get_offset() + chunk_buffer_.size();
    chunk_size -= current_chunk_info_->pos;
    //   Adjust for chunk header's length
    chunk_size -= kchunk_header_length_;
    //   Adjust for length values of the header and the data
    chunk_size -= 4;
    chunk_size -= 4;
    // Go ahead and create the end of the chunk and write it to the buffer
    write_chunk_end(chunk_buffer_, current_chunk_connection_indexes_);
    current_chunk_connection_indexes_.clear();
    // Then replace the place-holder chunk header
    VectorBuffer header_buffer;
    write_chunk_header(header_buffer, chunk_size, chunk_size);
    std::copy(header_buffer.begin(), header_buffer.end(),
              chunk_buffer_.begin() + current_chunk_position_);
    chunk_infos_.push_back(*current_chunk_info_);
    current_chunk_info_.reset();
  }
  // Write any remaining chunk_buffer_ stuff into the stop_buffer
  if (chunk_buffer_.size() != 0)
  {
    impl::write_to_buffer(stop_buffer, chunk_buffer_);
    chunk_buffer_.clear();
  }
  size_t index_data_position = file_->get_offset() + stop_buffer.size();
  impl::stop_writing(stop_buffer, connections_, chunk_infos_);
  // Pad the stop_buffer with 0x00 up to a 4096 boundary
  size_t stop_buffer_misalignment = stop_buffer.size() % 4096;
  if (stop_buffer_misalignment != 0)
  {
    stop_buffer.resize(stop_buffer.size() + (4096 - stop_buffer_misalignment),
                       0x00);
  }
  file_->write_buffer(stop_buffer);
  // Write the updated file header
  VectorBuffer file_header_buffer;
  impl::write_file_header_record(
    file_header_buffer,
    connections_.size(),
    chunk_infos_.size(),
    index_data_position
  );
  file_->seek(0);
  file_->write_buffer(file_header_buffer);
  file_->close();
  file_.reset();
  filename_.clear();
  topic_connection_ids_.clear();
  header_connection_ids_.clear();
  connections_.clear();
  connection_indexes_.clear();
  chunk_infos_.clear();
  current_chunk_position_ = 0;
  current_chunk_info_.reset();
  current_chunk_connection_indexes_.clear();
  chunk_threshold_ = 0;
  next_conn_id_ = 0;
}

bool DirectBag::is_open() const
{
  return open_.load();
}

size_t DirectBag::get_chunk_threshold() const
{
  return chunk_threshold_;
}

std::string DirectBag::get_bag_file_name() const
{
  if (this->is_open())
  {
    assert(file_ != nullptr);
    return file_->get_filename();
  }
  return std::string("");
}

size_t DirectBag::get_bag_file_size() const
{
  return file_->get_size();
}

size_t DirectBag::get_virtual_bag_size() const
{
  // It is ok to use get_offset here instead of get_size since
  // we know that the file cursor stays at the end of the file
  // except during close() where this function is not used.
  return file_->get_offset() + chunk_buffer_.size();
}

inline size_t
get_chunk_offset(size_t current_position, size_t chunk_data_position)
{
  return current_position - chunk_data_position;
}

template<class T> shared_ptr<rosbag::ConnectionInfo>
DirectBag::get_connection_info(std::string const &topic,
                               T const& msg,
                               shared_ptr<ros::M_string> connection_header)
{
  shared_ptr<rosbag::ConnectionInfo> connection_info;
  uint32_t conn_id = next_conn_id_;
  if (!connection_header)
  {
    // Lookup connection_info by topic if possible
    auto topic_connection_id_pair = topic_connection_ids_.find(topic);
    if (topic_connection_id_pair != topic_connection_ids_.end())
    {
      conn_id = topic_connection_id_pair->second;
      connection_info = connections_[conn_id];
    }
  }
  else
  {
    // Store the connection info by the address of the connection header

    // Add the topic name to the connection header, so that when we later
    // search by connection header, we can disambiguate connections that differ
    // only by topic name (i.e. same callerid, same message type), #3755.
    // This modified connection header is only used for our bookkeeping, and
    // will not appear in the resulting .bag.
    ros::M_string connection_header_copy(*connection_header);
    connection_header_copy["topic"] = topic;

    auto header_connection_id_pair = \
      header_connection_ids_.find(connection_header_copy);
    if (header_connection_id_pair != header_connection_ids_.end())
    {
      conn_id = header_connection_id_pair->second;
      connection_info = connections_[conn_id];
    }
  }

  // Write connection info record, if necessary
  if (!connection_info)
  {
    connection_info.reset(new rosbag::ConnectionInfo());
    connection_info->id       = conn_id;
    connection_info->topic    = topic;
    connection_info->datatype = \
      std::string(ros::message_traits::datatype(msg));
    connection_info->md5sum   = std::string(ros::message_traits::md5sum(msg));
    connection_info->msg_def  = \
      std::string(ros::message_traits::definition(msg));
    if (connection_header != nullptr)
    {
      connection_info->header = connection_header;
    }
    else
    {
      connection_info->header.reset(new std::map<std::string, std::string>);
      auto ci_header = connection_info->header;
      (*ci_header)["type"]               = connection_info->datatype;
      (*ci_header)["md5sum"]             = connection_info->md5sum;
      (*ci_header)["message_definition"] = connection_info->msg_def;
    }
    connections_[conn_id] = connection_info;

    // And write connection info record to the chunk buffer
    impl::write_connection_record(chunk_buffer_, connection_info);

    // Increment the conn_id for then next one that is created
    next_conn_id_ += 1;
  }

  // Always make sure that the topic -> conn_id map is up-to-date
  topic_connection_ids_[topic] = conn_id;

  return connection_info;
}

inline size_t
write_data_message_record_header_with_padding(
  VectorBuffer &buffer,
  uint32_t conn_id,
  ros::Time const &time,
  size_t current_file_offset,
  size_t serialized_message_data_len
)
{
  ros::M_string header;
  generate_message_record_header(conn_id, time, header);
  size_t buffer_starting_offset = buffer.size();
  // Write header
  size_t header_len = impl::write_header(buffer, header);
  size_t projected_offset = current_file_offset
                          + buffer.size()
                          + 4 // length of message data length
                          + serialized_message_data_len;
  size_t offset_to_4096_boundary = 4096 - (projected_offset % 4096);
  assert((offset_to_4096_boundary + projected_offset) % 4096 == 0);
  if (offset_to_4096_boundary == 4096)
  {
    // No alignment needed
    return header_len;
  }
  if (offset_to_4096_boundary < 7)
  {
    // Add 4096, since we need at least 7 char's to do the padding
    offset_to_4096_boundary += 4096;
  }
  impl::write_to_buffer(buffer, offset_to_4096_boundary - 4, 4);
  impl::write_to_buffer(buffer, "_=", 2);
  // Offset to achieve, minus len of padding, minus `_=`
  size_t number_of_pads = offset_to_4096_boundary - 4 - 2;
  buffer.resize(buffer.size() + number_of_pads, ' ');
  // Update the header len
  header_len += offset_to_4096_boundary;
  VectorBuffer temp;
  impl::write_to_buffer(temp, header_len, 4);
  std::copy(temp.begin(), temp.end(), buffer.begin() + buffer_starting_offset);
  // Return the new length
  return header_len;
}

size_t
start_chunk(VectorBuffer &buffer,
            shared_ptr<rosbag::ChunkInfo> &chunk_info,
            const ros::Time &start_time,
            size_t current_file_offset)
{
  // Initialize chunk info
  chunk_info->pos        = current_file_offset + buffer.size();
  chunk_info->start_time = start_time;
  chunk_info->end_time   = ros::Time(0);
  // This is a place holder, later we'll create the real one and replace it
  return write_chunk_header(buffer, 0, 0);
}

template<class T> void
DirectBag::write(std::string const& topic, ros::Time const& time, T const& msg,
                 shared_ptr<ros::M_string> connection_header)
{
  // Assert that the bag is open
  if (!is_open())
  {
    throw rosbag::BagException(
      "Tried to insert a message into a closed DirectBag.");
  }
  // Make sure a reasonable time is given
  if (time < ros::TIME_MIN)
  {
    throw rosbag::BagException(
      "Tried to insert a message with time < ros::MIN_TIME");
  }

  // Start a chunk if one is not in progress
  if (current_chunk_info_ == nullptr)
  {
    current_chunk_info_.reset(new rosbag::ChunkInfo());
    current_chunk_position_ = chunk_buffer_.size();
    start_chunk(chunk_buffer_, current_chunk_info_, time, file_->get_offset());
  }

  // Get ID for connection header
  auto connection_info = \
    this->get_connection_info(topic, msg, connection_header);

  // Figure out the size of the serialized message data
  size_t message_data_len = ros::serialization::serializationLength(msg);

  // Figure out if we should finish the chunk or not
  bool should_finish_chunk = false;
  if (has_direct_data<T>())
  {
    // Always finish a chunk before writing a direct data message
    should_finish_chunk = true;
  }
  else
  {
    // See if the chunk meets or exceeds the chunk threshold
    // Start with the current size of the chunk
    size_t est_chunk_size = chunk_buffer_.size() - current_chunk_position_;
    // Add the size of the data message recored header
    est_chunk_size += kmessage_header_length_;
    // Add the size of the message data length number
    est_chunk_size += 4;
    // Add the length of the message data
    est_chunk_size += message_data_len;
    if (est_chunk_size >= chunk_threshold_)
    {
      // If the predicted chunk size meets or exceeds the chunk threshold
      // then finish this chunk after this message.
      should_finish_chunk = true;
    }
  }

  // Write the message data record header
  size_t message_header_len;
  if (should_finish_chunk)
  {
    // If we are finishing the chunk, pad so that we write to a 4096 boundry
    message_header_len = write_data_message_record_header_with_padding(
        chunk_buffer_, connection_info->id, time, file_->get_offset(),
        message_data_len);
  }
  else
  {
    // Otherwise write the message header as normal, without extra padding
    message_header_len = write_data_message_record_header(
        chunk_buffer_, connection_info->id, time);
  }

  // Create an index entry for the message
  rosbag::IndexEntry index_entry;
  // Capture the time the message was received
  index_entry.time      = time;
  // Capture the position of the containing chunk
  index_entry.chunk_pos = current_chunk_info_->pos;
  // Calculate the offset of the message data in the chunk
  // Start with absolute virtual position in file for offset
  index_entry.offset    = file_->get_offset() + chunk_buffer_.size();
  // Adjust for absolute position of chunk in file
  index_entry.offset   -= current_chunk_info_->pos;
  // Adjust for the chunk header
  index_entry.offset   -= 4;  // Length of the header
  index_entry.offset   -= kchunk_header_length_;
  // Adjust for the message header
  index_entry.offset   -= 4;  // Length of the header
  index_entry.offset   -= message_header_len;
  // Adjust for the length of the chunk data
  index_entry.offset   -= 4;

  auto &chunk_connection_index = \
    current_chunk_connection_indexes_[connection_info->id];
  chunk_connection_index.insert(chunk_connection_index.end(), index_entry);
  auto &connection_index = connection_indexes_[connection_info->id];
  connection_index.insert(connection_index.end(), index_entry);

  // Increment the connection count
  current_chunk_info_->connection_counts[connection_info->id]++;

  // Write the size of the data
  impl::write_to_buffer(chunk_buffer_, message_data_len, 4);

  VectorBuffer chunk_end_buffer;  // Only used if finishing chunk
  if (should_finish_chunk)
  {
    /** The steps to "finish a chunk":
     * 1. Calculate the chunk size
     * 2. Write the post chunk items (indexes)
     * 3. Overwrite the phony chunk header with the actual one
     * 4. Write all chunk data (header, connections, messages)
     * 5. Write "post" chunk items
     */
    // Calculate the final size of the chunk
    //   Start with the current total offset
    size_t chunk_size = file_->get_offset() + chunk_buffer_.size();
    //   Add the message length for the predicted chunk size
    chunk_size += message_data_len;
    //   Adjust for the position of the chunk in the file
    chunk_size -= current_chunk_info_->pos;
    //   Adjust for chunk header's length
    chunk_size -= kchunk_header_length_;
    //   Adjust for length values of the header and the data
    chunk_size -= 4;
    chunk_size -= 4;
    // Go ahead and create the end of the chunk
    write_chunk_end(chunk_end_buffer, current_chunk_connection_indexes_);
    current_chunk_connection_indexes_.clear();
    // Then replace the place-holder chunk header
    VectorBuffer header_buffer;
    write_chunk_header(header_buffer, chunk_size, chunk_size);
    std::copy(header_buffer.begin(), header_buffer.end(),
              chunk_buffer_.begin() + current_chunk_position_);
  }

  if (has_direct_data<T>())
  {
    SerializationReturnCode ret_code = \
      SerializationReturnCode::SERIALIZE_TO_BUFFER_NEXT;
    size_t step = 0;
    while (ret_code != SerializationReturnCode::DONE)
    {
      switch (ret_code)
      {
        case SerializationReturnCode::SERIALIZE_TO_BUFFER_NEXT:
          ret_code = serialize_to_buffer<T>(chunk_buffer_, msg, step);
          break;
        case SerializationReturnCode::SERIALIZE_TO_FILE_NEXT:
          // First flush the chunk_buffer_ to the file
          file_->write_buffer(chunk_buffer_);
          chunk_buffer_.clear();
          assert(((file_->get_offset() + chunk_buffer_.size()) % 4096) == 0);
          // Then allow the user to write directly to the file
          ret_code = serialize_to_file<T>(*file_, msg, step);
          break;
        case SerializationReturnCode::DONE:
          // Should not happen.
          break;
      }
      step += 1;
    }
  }
  else
  {
    // It is a normal ROS message, serialize normally
    size_t current_position = chunk_buffer_.size();
    chunk_buffer_.resize(chunk_buffer_.size() + message_data_len);
    ros::serialization::OStream s(chunk_buffer_.data() + current_position,
                                  message_data_len);
    ros::serialization::serialize(s, msg);
    if (should_finish_chunk)
    {
      file_->write_buffer(chunk_buffer_);
      chunk_buffer_.clear();
    }
  }

  if (should_finish_chunk)
  {
    // Empty the outgoing chunk
    chunk_buffer_.insert(chunk_buffer_.end(),
                         chunk_end_buffer.begin(), chunk_end_buffer.end());
    chunk_infos_.push_back(*current_chunk_info_);
    current_chunk_info_.reset();
  }
}

class BagFileException : public rosbag::BagException
{
public:
    BagFileException(std::string const& msg) : BagException(msg) {}
    BagFileException(std::string msg, int errnum)
      : BagFileException(
        msg + std::string(": ") + std::string(strerror(errnum)))
    {}
};

DirectFile::DirectFile(std::string filename) : filename_(filename), open_(true)
{
#ifdef __APPLE__
  file_pointer_ = fopen(filename.c_str(), "w+b");
  if (file_pointer_ == nullptr)
  {
    throw BagFileException(std::string("Failed to open file: ") + filename,
                           errno);
  }
  int fd = fileno(file_pointer_);
  if (fd < 0)
  {
    throw BagFileException("Failed to get file descriptor", errno);
  }
  if (fcntl(fd, F_NOCACHE, 1) == -1)
  {
    throw BagFileException("Failed to set F_NOCACHE", errno);
  }
#else
  int fd = open(filename.c_str(), O_CREAT | O_RDWR | O_DIRECT,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
  if (fd < 0)
  {
    throw BagFileException(std::string("Failed to open file: ") + filename,
                           errno);
  }
  file_descriptor_ = fd;
#endif
}

DirectFile::~DirectFile()
{
  if (open_)
  {
    this->close();
  }
}

bool DirectFile::is_open()
{
  return open_;
}

void DirectFile::close()
{
#if __APPLE__
  fclose(file_pointer_);
#else
  ::close(file_descriptor_);
#endif
  open_ = false;
}

size_t
DirectFile::get_offset() const
{
#if __APPLE__
  ssize_t offset = ftell(file_pointer_);
  if (offset < 0)
  {
    throw BagFileException("Failed to get current offset", errno);
  }
  return offset;
#else
  off_t offset = lseek(file_descriptor_, 0, SEEK_CUR);
  if (offset < 0)
  {
    throw BagFileException("Failed to get current offset", errno);
  }
  return offset;
#endif
}

void
DirectFile::seek(uint64_t pos) const
{
#if __APPLE__
  ssize_t offset = fseek(file_pointer_, pos, SEEK_SET);
  if (offset == -1)
  {
    throw BagFileException("Failed to seek", errno);
  }
#else
  off_t offset = lseek(file_descriptor_, pos, SEEK_SET);
  if (offset == -1)
  {
    throw BagFileException("Failed to seek", errno);
  }
#endif
}

size_t DirectFile::get_size() const
{
  size_t offset = this->get_offset();
#if __APPLE__
  ssize_t end_offset = fseek(file_pointer_, 0, SEEK_END);
#else
  off_t end_offset = lseek(file_descriptor_, 0, SEEK_END);
#endif
  this->seek(offset);
  return end_offset;
}

size_t
DirectFile::write_buffer(VectorBuffer &buffer)
{
  return this->write_data(buffer.data(), buffer.size());
}

size_t
DirectFile::write_data(const uint8_t *start, size_t length)
{
  assert((reinterpret_cast<uintptr_t>(start) % 4096) == 0);
  assert((length % 4096) == 0);
#if __APPLE__
  ssize_t ret = fwrite(start, sizeof(uint8_t), length, file_pointer_);
  if (ret < 0)
  {
    throw BagFileException("Failed to write", errno);
  }
  return ret;
#else
  ssize_t ret = ::write(file_descriptor_, start, length);
  if (ret < 0)
  {
    throw BagFileException("Failed to write", errno);
  }
  return ret;
#endif
}

DirectBagCollection::DirectBagCollection()
: folder_path_(""), file_prefix_(""), chunk_threshold_(0),
  bag_size_threshold_(0), bag_number_width_(0), open_(false),
  current_bag_number_(0), current_bag_(nullptr)
{}

DirectBagCollection::~DirectBagCollection()
{
  if (this->is_open())
  {
    this->close();
  }
}

inline bool directory_exists(std::string path)
{
  struct stat info;

  if(stat(path.c_str(), &info) == 0 && info.st_mode & S_IFDIR)
  {
    return true;
  }
  else
  {
    return false;
  }
}

void make_directories(std::string path)
{
  std::vector<std::string> directories;
  std::stringstream ss(path);
  std::string item;
  while (std::getline(ss, item, '/'))
  {
    directories.push_back(item);
  }
  std::string current_path = "";
  if (0 != path.compare(0, 1, "/"))
  {
    // Path doesn't start with a / and is not absolute
    current_path = ".";
  }
  for (auto &directory : directories)
  {
    current_path += '/' + directory;
    if (!directory_exists(current_path))
    {
      auto ret = mkdir(current_path.c_str(), ACCESSPERMS);
      if (ret != 0)
      {
        std::string err_msg = "Failed to create directory ";
        err_msg += current_path;
        throw BagFileException(err_msg.c_str(), errno);
      }
    }
  }
}

void DirectBagCollection::open_directory(std::string folder_path,
                                         std::string file_prefix,
                                         size_t chunk_threshold,
                                         size_t bag_size_threshold,
                                         size_t bag_number_width)
{
  if (this->is_open())
  {
    throw std::runtime_error(
      "open called on an already open DirectBagCollection instance.");
  }
  folder_path_ = folder_path;
  file_prefix_ = file_prefix;
  chunk_threshold_ = chunk_threshold;
  bag_size_threshold_ = bag_size_threshold;
  bag_number_width_ = bag_number_width;
  // Make sure the directory path exists
  make_directories(folder_path_);
  this->open_next_bag_();
  open_.store(true);
}

inline std::string
generate_bag_name(std::string folder_path, std::string file_prefix,
                  size_t bag_number, size_t bag_number_width)
{
  std::stringstream ss;
  if (folder_path != "")
  {
    if (0 != folder_path.compare(folder_path.length() - 1, 1, "/"))
    {
      folder_path += '/';
    }
    ss << folder_path;
  }
  ss << file_prefix;
  if (file_prefix != "")
  {
    ss << "-";
  }
  ss << std::setfill('0') << std::setw(bag_number_width) << bag_number
     << ".bag";
  return ss.str();
}

std::vector<std::string> DirectBagCollection::close()
{
  bool was_open = open_.exchange(false);
  if (!was_open)
  {
    throw std::runtime_error(
      "close called on an already closed DirectBag instance");
  }
  // Calculate bag file names for return
  std::vector<std::string> filenames;
  for(size_t i = 1; i <= current_bag_number_; i++)
  {
    filenames.push_back(
      generate_bag_name(folder_path_, file_prefix_,
                        i, bag_number_width_));
  }
  // Clean up
  if (current_bag_ != nullptr)
  {
    close_current_bag_();
    current_bag_.reset();
  }
  folder_path_.clear();
  file_prefix_.clear();
  chunk_threshold_ = 0;
  bag_size_threshold_ = 0;
  bag_number_width_ = 0;
  current_bag_number_ = 0;
  return filenames;
}

template<typename ...Args>
void DirectBagCollection::write(Args && ...args)
{
  if (current_bag_ == nullptr)
  {
    open_next_bag_();
  }
  if (this->is_open())
  {
    assert(current_bag_ != nullptr);
    current_bag_->write(std::forward<Args>(args)...);
    check_bag_threshold_();
  }
  else
  {
    throw std::runtime_error(
      "write called on a closed DirectBagCollection instance");
  }
}

bool DirectBagCollection::is_open() const
{
  return open_.load();
}

std::string DirectBagCollection::get_folder_path() const
{
  return folder_path_;
}

size_t DirectBagCollection::get_chunk_threshold() const
{
  if (!this->is_open())
  {
    throw std::runtime_error(
      "get_chunk_threshold called on a closed DirectBagCollection instance");
  }
  assert(current_bag_ != nullptr);
  return current_bag_->get_chunk_threshold();
}

size_t DirectBagCollection::get_bag_size_threshold() const
{
  return bag_size_threshold_;
}

size_t DirectBagCollection::get_current_bag_number() const
{
  return current_bag_number_;
}

std::string DirectBagCollection::get_current_bag_file_name() const
{
  if (!this->is_open())
  {
    throw std::runtime_error(
      "get_current_bag_file_name called on a closed "
      "DirectBagCollection instance");
  }
  assert(current_bag_ != nullptr);
  return current_bag_->get_bag_file_name();
}

void DirectBagCollection::check_bag_threshold_()
{
  assert(current_bag_ != nullptr);
  if (current_bag_->get_virtual_bag_size() >= bag_size_threshold_)
  {
    this->close_current_bag_();
  }
}

void DirectBagCollection::close_current_bag_()
{
  assert(current_bag_ != nullptr);
  assert(current_bag_->is_open());
  current_bag_->close();
  current_bag_.reset();
}

void DirectBagCollection::open_next_bag_()
{
  if (current_bag_ != nullptr)
  {
    close_current_bag_();
  }
  current_bag_.reset(new DirectBag(
    generate_bag_name(folder_path_, file_prefix_,
                      ++current_bag_number_, bag_number_width_),
    chunk_threshold_));
}

} /* namespace rosbag_direct_write */

#endif  /* ROSBAG_BAG_DIRECT_BAG_DIRECT_IMPL_H */
