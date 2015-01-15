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

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <rosbag_direct_write/direct_bag_impl_dependencies.h>

#include "direct_bag.h"

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif

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
  string version = string("#ROSBAG V") + VERSION + string("\n");

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
  size_t starting_offset,
  uint32_t connection_count,
  uint32_t chunk_count,
  uint64_t index_data_position)
{
  std::map<string, string> header;
  header[OP_FIELD_NAME]               = to_header_string(&OP_FILE_HEADER);
  header[INDEX_POS_FIELD_NAME]        = to_header_string(&index_data_position);
  header[CONNECTION_COUNT_FIELD_NAME] = to_header_string(&connection_count);
  header[CHUNK_COUNT_FIELD_NAME]      = to_header_string(&chunk_count);

  uint32_t header_len = write_header(buffer, header);
  uint32_t data_len = 0;
  if (header_len < FILE_HEADER_LENGTH)
  {
    // 4096 - header_len - version - header_len_len - data_len_len
    data_len = FILE_HEADER_LENGTH - header_len - starting_offset - 4 - 4;
  }
  write_to_buffer(buffer, data_len, 4);

  // Pad the file header record out
  if (data_len > 0)
  {
      string padding;
      padding.resize(data_len, ' ');
      write_to_buffer(buffer, padding);
  }
}

inline size_t
start_writing(VectorBuffer &buffer)
{
  write_version(buffer);
  size_t file_header_record_offset = buffer.size();
  write_file_header_record(buffer, file_header_record_offset, 0, 0, 0);
  return file_header_record_offset;
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

DirectBag::DirectBag(std::string filename) : DirectBag() {
  this->open(filename, rosbag::bagmode::Write);
}

DirectBag::DirectBag()
    : filename_(""),
      open_(false),
      file_header_record_offset_(0),
      next_conn_id_(0) {}

void DirectBag::open(std::string filename, rosbag::bagmode::BagMode mode)
{
  UNUSED(mode);
  assert(mode == rosbag::bagmode::Write);
  file_.reset(new DirectFile(filename));
  filename_ = filename;
  VectorBuffer start_buffer;
  file_header_record_offset_ = impl::start_writing(start_buffer);
  file_->write_buffer(start_buffer);
  chunk_buffer_.clear();
  open_ = true;
}

DirectBag::~DirectBag()
{
  if (open_)
  {
    this->close();
  }
}

void DirectBag::close()
{
  if (!open_)
  {
    throw rosbag::BagException("Tried to close and already closed DirectBag.");
  }
  open_ = false;
  // Write any remaining chunk_buffer_ stuff
  if (chunk_buffer_.size() != 0)
  {
    file_->write_buffer(chunk_buffer_);
    chunk_buffer_.clear();
  }
  size_t index_data_position = file_->get_offset();
  VectorBuffer stop_buffer;
  impl::stop_writing(stop_buffer, connections_, chunk_infos_);
  file_->write_buffer(stop_buffer);
  VectorBuffer file_header_buffer;
  impl::write_file_header_record(
    file_header_buffer,
    file_header_record_offset_,
    connections_.size(),
    chunk_infos_.size(),
    index_data_position
  );
  file_->seek(file_header_record_offset_);
  file_->write_buffer(file_header_buffer);
}

bool DirectBag::is_open() const
{
  return open_;
}

inline size_t
write_chunk_header(VectorBuffer &buffer,
                   CompressionType compression,
                   uint32_t compressed_size,
                   uint32_t uncompressed_size)
{
  UNUSED(compression);
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

inline size_t
get_chunk_offset(size_t current_position, size_t chunk_data_position)
{
  return current_position - chunk_data_position;
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
    std::multiset<rosbag::IndexEntry> index = index_entry_pair.second;

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
finish_chunk(
  VectorBuffer &buffer,
  std::map<uint32_t, std::multiset<rosbag::IndexEntry>>
    &chunk_connection_indexes
)
{
  // Write out the indexes and clear them
  write_index_records(buffer, chunk_connection_indexes);
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

  return connection_info;
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

template<class T> bool
has_direct_data()
{
  return false;
}

template<class T> void
serialize_to_buffer(VectorBuffer &buffer, const T &msg);

template<class T> void
serialize_to_file(DirectFile &file, const T &msg);

template<class T> void
DirectBag::write(std::string const& topic, ros::Time const& time, T const& msg,
                 shared_ptr<ros::M_string> connection_header)
{
  // Assert that the bag is open
  if (!open_)
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

  size_t chunk_start_pos_in_buffer = chunk_buffer_.size();

  // Setup chunk info
  rosbag::ChunkInfo chunk_info;
  // Initialize chunk info
  chunk_info.pos        = file_->get_offset() + chunk_buffer_.size();
  chunk_info.start_time = time;
  chunk_info.end_time   = time;
  // This is a place holder, later we'll create the real one and replace it
  size_t chunk_header_len = write_chunk_header(
    chunk_buffer_, CompressionType::Uncompressed, 0, 0);

  // Get ID for connection header
  auto connection_info = \
    this->get_connection_info(topic, msg, connection_header);

  // Figure out the size of the serialized message data
  size_t message_data_len = ros::serialization::serializationLength(msg);

  // Write the message data record header
  size_t message_header_len;
  // Always pad so that we write to a 4096 boundry
  message_header_len = write_data_message_record_header_with_padding(
      chunk_buffer_, connection_info->id, time, file_->get_offset(),
      message_data_len);

  // Add to topic indexes
  rosbag::IndexEntry index_entry;
  index_entry.time      = time;
  index_entry.chunk_pos = chunk_info.pos;
  // Start with virtual position in file for offset
  index_entry.offset    = file_->get_offset() + chunk_buffer_.size();
  // Adjust for position of chunk in file
  index_entry.offset   -= chunk_info.pos;
  // Adjust for message and chunk header
  index_entry.offset   -= message_header_len;
  index_entry.offset   -= chunk_header_len;
  // Adjust for 3x 4-byte lengths (the two headers and the data)
  index_entry.offset   -= (3 * 4);

  std::map<uint32_t, std::multiset<rosbag::IndexEntry>>
    chunk_connection_indexes;

  auto &chunk_connection_index = chunk_connection_indexes[connection_info->id];
  chunk_connection_index.insert(chunk_connection_index.end(), index_entry);
  auto &connection_index = connection_indexes_[connection_info->id];
  connection_index.insert(connection_index.end(), index_entry);

  // Increment the connection count
  chunk_info.connection_counts[connection_info->id]++;

  // Write the size of the data
  impl::write_to_buffer(chunk_buffer_, message_data_len, 4);

  // Go ahead and create the end of the chunk
  VectorBuffer chunk_end_buffer;
  finish_chunk(chunk_end_buffer, chunk_connection_indexes);

  // Now calculate the final size of the chunk
  //   Start with the current total offset
  size_t chunk_size = file_->get_offset() + chunk_buffer_.size();
  //   Add the message length for the predicted chunk size
  chunk_size += message_data_len;
  //   Adjust for the position of the chunk in the file
  chunk_size -= chunk_info.pos;
  //   Adjust for chunk header's length
  chunk_size -= chunk_header_len;
  //   Adjust for length values of the header and the data
  chunk_size -= 4;
  chunk_size -= 4;
  // Then replace the place-holder chunk header
  VectorBuffer header_buffer;
  write_chunk_header(header_buffer,
                     CompressionType::Uncompressed,
                     chunk_size,
                     chunk_size);
  std::copy(header_buffer.begin(), header_buffer.end(),
            chunk_buffer_.begin() + chunk_start_pos_in_buffer);

  if (has_direct_data<T>())
  {
    // It has a direct write field in the message
    // Write the non-direct part to the buffer
    serialize_to_buffer<T>(chunk_buffer_, msg);
    // Write out the buffer
    file_->write_buffer(chunk_buffer_);
    chunk_buffer_.clear();
    // Now write the direct stuff
    serialize_to_file<T>(*file_, msg);
  }
  else
  {
    // It is a normal ROS message, serialize normally
    size_t current_position = chunk_buffer_.size();
    chunk_buffer_.resize(chunk_buffer_.size() + message_data_len);
    ros::serialization::OStream s(chunk_buffer_.data() + current_position,
                                  message_data_len);
    ros::serialization::serialize(s, msg);
    file_->write_buffer(chunk_buffer_);
  }
  // Write the non direct part of the message to the buffer
  // Make sure the chunk_buffer_ is clear now
  chunk_buffer_.clear();

  // Empty the outgoing chunk
  chunk_buffer_.insert(chunk_buffer_.end(),
                       chunk_end_buffer.begin(), chunk_end_buffer.end());
  chunk_infos_.push_back(chunk_info);
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

DirectFile::DirectFile(std::string filename) : filename_(filename)
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
  if (fd < 0) {
    throw BagFileException(std::string("Failed to open file: ") + filename,
                           errno);
  }
  file_descriptor_ = fd;
#endif
}

DirectFile::~DirectFile() {
#if __APPLE__
  fclose(file_pointer_);
#else
  close(file_descriptor_);
#endif
}

size_t
DirectFile::get_offset() const
{
#if __APPLE__
  ssize_t offset = ftell(file_pointer_);
  if (offset < 0) {
    throw BagFileException("Failed to get current offset", errno);
  }
  return offset;
#else
  off_t offset = lseek(file_descriptor_, 0, SEEK_CUR);
  if (offset < 0) {
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
  if (offset == -1) {
    throw BagFileException("Failed to seek", errno);
  }
#else
  off_t offset = lseek(file_descriptor_, pos, SEEK_SET);
  if (offset == -1) {
    throw BagFileException("Failed to seek", errno);
  }
#endif
}

size_t
DirectFile::write_buffer(VectorBuffer &buffer)
{
  return this->write_data(buffer.data(), buffer.size());
}

size_t
DirectFile::write_data(const uint8_t *start, size_t length)
{
#if __APPLE__
  ssize_t ret = fwrite(start, sizeof(uint8_t), length, file_pointer_);
  if (ret < 0)
  {
    throw BagFileException("Failed to write", errno);
  }
  return ret;
#else
  ssize_t ret = write(file_descriptor_, start, length);
  if (ret < 0) {
    throw BagFileException("Failed to write", errno);
  }
  return ret;
#endif
}

} /* namespace rosbag_direct_write */

#endif  /* ROSBAG_BAG_DIRECT_BAG_DIRECT_IMPL_H */
