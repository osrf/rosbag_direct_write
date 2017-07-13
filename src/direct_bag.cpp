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

#include "rosbag_direct_write/direct_bag.h"

#include <cassert>
#include <errno.h>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

#include <rosbag_direct_write/direct_bag_impl_dependencies.h>

namespace rosbag_direct_write {

size_t chunk_header_length() {
  static VectorBuffer __temp_constant_buffer;
  static const size_t kchunk_header_length = \
    write_chunk_header(__temp_constant_buffer, 0, 0);
  return kchunk_header_length;
}

size_t message_header_length() {
  static VectorBuffer __temp_constant_buffer;
  static const size_t kmessage_header_length = \
    write_data_message_record_header(__temp_constant_buffer, 0, ros::Time(0));
  return kmessage_header_length;
}

size_t write_data_message_record_header_with_padding(
  VectorBuffer &buffer,
  uint32_t conn_id,
  ros::Time const &time,
  size_t current_file_offset,
  size_t serialized_message_data_len,
  size_t alignment_adjustment
) {
  ros::M_string header;
  generate_message_record_header(conn_id, time, header);
  size_t buffer_starting_offset = buffer.size();
  // Write header
  size_t header_len = impl::write_header(buffer, header);
  size_t projected_offset = current_file_offset
                          + buffer.size()
                          + 4 // length of message data length
                          + serialized_message_data_len
                          - alignment_adjustment;
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

using rosbag::compression::CompressionType;
using namespace rosbag;
using std::string;

DirectBag::DirectBag(std::string filename, bool use_odirect,
                     size_t chunk_threshold)
    : DirectBag() {
  this->open(filename, use_odirect, chunk_threshold);
}

DirectBag::DirectBag()
    : filename_(""),
      open_(false),
      current_chunk_position_(0),
      current_chunk_info_(nullptr),
      chunk_threshold_(kdefault_chunk_threshold),
      next_conn_id_(0) {}

void DirectBag::open(std::string filename, bool use_odirect,
                     size_t chunk_threshold) {
  if (this->is_open()) {
    throw std::runtime_error(
        "open called on an already open DirectBag instance.");
  }
  file_.reset(new DirectFile(filename, use_odirect));
  filename_ = filename;
  chunk_threshold_ = chunk_threshold;
  VectorBuffer start_buffer;
  impl::start_writing(start_buffer);
  file_->write_buffer(start_buffer);
  chunk_buffer_.clear();
  open_.store(true);
}

DirectBag::~DirectBag() {
  if (this->is_open()) {
    this->close();
  }
}

void DirectBag::close() {
  bool was_open = open_.exchange(false);
  if (!was_open) {
    throw std::runtime_error(
        "close called on an already closed DirectBag instance");
  }
  VectorBuffer stop_buffer;
  // Finish any open chunks
  if (current_chunk_info_ != nullptr) {
    // Calculate the chunk size
    size_t chunk_size = file_->get_offset() + chunk_buffer_.size();
    chunk_size -= current_chunk_info_->pos;
    //   Adjust for chunk header's length
    chunk_size -= chunk_header_length();
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
  if (chunk_buffer_.size() != 0) {
    impl::write_to_buffer(stop_buffer, chunk_buffer_);
    chunk_buffer_.clear();
  }
  size_t index_data_position = file_->get_offset() + stop_buffer.size();
  impl::stop_writing(stop_buffer, connections_, chunk_infos_);
  // Pad the stop_buffer with 0x00 up to a 4096 boundary
  size_t stop_buffer_misalignment = stop_buffer.size() % 4096;
  if (stop_buffer_misalignment != 0) {
    stop_buffer.resize(stop_buffer.size() + (4096 - stop_buffer_misalignment),
                       0x00);
  }
  file_->write_buffer(stop_buffer);
  // Write the updated file header
  VectorBuffer file_header_buffer;
  impl::write_file_header_record(file_header_buffer, connections_.size(),
                                 chunk_infos_.size(), index_data_position);
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

bool DirectBag::is_open() const { return open_.load(); }

size_t DirectBag::get_chunk_threshold() const { return chunk_threshold_; }

std::string DirectBag::get_bag_file_name() const {
  if (this->is_open()) {
    assert(file_ != nullptr);
    return file_->get_filename();
  }
  return std::string("");
}

size_t DirectBag::get_bag_file_size() const { return file_->get_size(); }

size_t DirectBag::get_virtual_bag_size() const {
  // It is ok to use get_offset here instead of get_size since
  // we know that the file cursor stays at the end of the file
  // except during close() where this function is not used.
  return file_->get_offset() + chunk_buffer_.size();
}

DirectFile::DirectFile(std::string filename, bool use_odirect)
    : filename_(filename), open_(true), use_odirect_(use_odirect) {
#ifdef __APPLE__
  file_pointer_ = fopen(filename.c_str(), "w+b");
  if (file_pointer_ == nullptr) {
    throw BagFileException(std::string("Failed to open file: ") + filename,
                           errno);
  }
  int fd = fileno(file_pointer_);
  if (fd < 0) {
    throw BagFileException("Failed to get file descriptor", errno);
  }
  if (fcntl(fd, F_NOCACHE, 1) == -1) {
    throw BagFileException("Failed to set F_NOCACHE", errno);
  }
#else
  int flags = O_CREAT | O_RDWR | O_TRUNC;
  constexpr int mode =
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
  if (use_odirect) {
    flags = flags | O_DIRECT;
  }
  // Try to open in O_DIRECT mode.
  int fd = open(filename.c_str(), flags, mode);
  if (fd < 0) {
    throw BagFileException(std::string("Failed to open file: ") + filename,
                           errno);
  }
  file_descriptor_ = fd;
#endif
}

DirectFile::~DirectFile() {
  if (open_) {
    this->close();
  }
}

bool DirectFile::is_open() { return open_; }

void DirectFile::close() {
#if __APPLE__
  fclose(file_pointer_);
#else
  ::close(file_descriptor_);
#endif
  open_ = false;
}

size_t DirectFile::get_offset() const {
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

void DirectFile::seek(uint64_t pos) const {
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

size_t DirectFile::get_size() const {
  size_t offset = this->get_offset();
#if __APPLE__
  ssize_t end_offset = fseek(file_pointer_, 0, SEEK_END);
#else
  off_t end_offset = lseek(file_descriptor_, 0, SEEK_END);
#endif
  this->seek(offset);
  return end_offset;
}

size_t DirectFile::write_buffer(VectorBuffer& buffer) {
  return this->write_data(buffer.data(), buffer.size());
}

namespace {
uint8_t* AllocateAlignedBuffer(size_t buffer_size_bytes) {
#if __ANDROID__
  void* p = memalign(4096, buffer_size_bytes);
#else
  void* p;
  if (0 != posix_memalign(&p, 4096, buffer_size_bytes)) {
    p = 0;
  }
#endif
  return static_cast<uint8_t*>(p);
}
}

size_t DirectFile::write_data(const uint8_t* start, size_t length) {
  size_t current_offset = get_offset();
  if (use_odirect_) {
    assert((current_offset % 4096) == 0);
    assert((reinterpret_cast<uintptr_t>(start) % 4096) == 0);
    assert((length % 4096) == 0);
  }
#if __APPLE__
  ssize_t ret = fwrite(start, sizeof(uint8_t), length, file_pointer_);
  if (ret < 0) {
    throw BagFileException("Failed to write", errno);
  }
  return ret;
#else
  ssize_t bytes_written = ::write(file_descriptor_, start, length);
  if (errno == EFAULT) {
    // Bad address. As a fallback solution, write a copy of the data instead.
    // This can work if the original buffer is in the wrong address space.
    uint8_t* data_copy = AllocateAlignedBuffer(length);
    assert(data_copy != nullptr);
    memcpy(data_copy, start, length);
    seek(current_offset);
    bytes_written = ::write(file_descriptor_, data_copy, length);
    free(data_copy);
  }
  if (bytes_written != (int64_t)length) {
    throw BagFileException("Failed to write buffer.", errno);
  }
  return bytes_written;
#endif
}

DirectBagCollection::DirectBagCollection()
    : folder_path_(""),
      file_prefix_(""),
      chunk_threshold_(0),
      bag_size_threshold_(0),
      bag_number_width_(0),
      use_odirect_(true),
      open_(false),
      current_bag_number_(0),
      current_bag_(nullptr) {}

DirectBagCollection::~DirectBagCollection() {
  if (this->is_open()) {
    this->close();
  }
}

std::vector<std::string> DirectBagCollection::close() {
  bool was_open = open_.exchange(false);
  if (!was_open) {
    throw std::runtime_error(
        "close called on an already closed DirectBag instance");
  }
  // Calculate bag file names for return
  std::vector<std::string> filenames;
  for (size_t i = 1; i <= current_bag_number_; i++) {
    filenames.push_back(
        generate_bag_name(folder_path_, file_prefix_, i, bag_number_width_));
  }
  // Clean up
  if (current_bag_ != nullptr) {
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

void DirectBagCollection::open_directory(std::string folder_path,
                                         bool use_odirect,
                                         std::string file_prefix,
                                         size_t chunk_threshold,
                                         size_t bag_size_threshold,
                                         size_t bag_number_width) {
  if (this->is_open()) {
    throw std::runtime_error(
        "open called on an already open DirectBagCollection instance.");
  }
  use_odirect_ = use_odirect;
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

bool DirectBagCollection::is_open() const { return open_.load(); }

std::string DirectBagCollection::get_folder_path() const {
  return folder_path_;
}

size_t DirectBagCollection::get_chunk_threshold() const {
  if (!this->is_open()) {
    throw std::runtime_error(
        "get_chunk_threshold called on a closed DirectBagCollection instance");
  }
  assert(current_bag_ != nullptr);
  return current_bag_->get_chunk_threshold();
}

size_t DirectBagCollection::get_bag_size_threshold() const {
  return bag_size_threshold_;
}

size_t DirectBagCollection::get_current_bag_number() const {
  return current_bag_number_;
}

std::string DirectBagCollection::get_current_bag_file_name() const {
  if (!this->is_open()) {
    throw std::runtime_error(
        "get_current_bag_file_name called on a closed "
        "DirectBagCollection instance");
  }
  assert(current_bag_ != nullptr);
  return current_bag_->get_bag_file_name();
}

void DirectBagCollection::check_bag_threshold_() {
  assert(current_bag_ != nullptr);
  if (current_bag_->get_virtual_bag_size() >= bag_size_threshold_) {
    this->close_current_bag_();
  }
}

void DirectBagCollection::close_current_bag_() {
  assert(current_bag_ != nullptr);
  assert(current_bag_->is_open());
  current_bag_->close();
  current_bag_.reset();
}

void DirectBagCollection::open_next_bag_() {
  if (current_bag_ != nullptr) {
    close_current_bag_();
  }
  current_bag_.reset(
      new DirectBag(generate_bag_name(folder_path_, file_prefix_,
                                      ++current_bag_number_, bag_number_width_),
                    use_odirect_, chunk_threshold_));
}

} /* namespace rosbag_direct_write */
