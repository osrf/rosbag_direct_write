#include <gtest/gtest.h>

#include <geometry_msgs/PointStamped.h>
// I know this is quite strange, but I need to get to the internals of the
// rosbag::Bag class to do some easy checking.
#define private public
#define protected public
#include <rosbag/bag.h>
#undef private
#undef protected
#include <rosbag/view.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/PointField.h>
#include <std_msgs/Header.h>

#include "rosbag_direct_write/direct_bag.h"

#include "direct_PointCloud2.h"
#include "custom_image.h"
#include "custom_point_stamped.h"

class DirectBagTestSuite : public ::testing::TestWithParam<bool> {
};

TEST_P(DirectBagTestSuite, test_record_bag)
{
  std::string bag_name = "test_direct.bag";
  // Write a bag with ROS messages
  rosbag_direct_write::DirectBag bag(bag_name, GetParam());
  size_t number_of_iterations = 3;
  size_t width = 1024, height = 768;
  sensor_msgs::Image image;
  image.header.stamp.fromSec(ros::WallTime::now().toSec());
  image.header.frame_id = "/camera_frame";
  image.height = height;
  image.width = width;
  image.encoding = "Encoding 1";
  image.is_bigendian = true;
  image.step = 1;
  image.data = std::vector<uint8_t>(width * height, 0x12);
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    image.header.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("camera", image.header.stamp, image);
  }
  bag.close();
  // Read bag with normal rosbag interface
  rosbag::Bag ros_bag;
  ros_bag.open(bag_name, rosbag::bagmode::Read);
  std::vector<std::string> topics;
  topics.push_back(std::string("camera"));
  rosbag::View view(ros_bag, rosbag::TopicQuery(topics));
  size_t number_of_images = 0;
  for (auto &m : view)
  {
    ASSERT_STREQ("camera", m.getTopic().c_str());
    auto msg = m.instantiate<sensor_msgs::Image>();
    ASSERT_NE(nullptr, msg);
    ASSERT_STREQ("/camera_frame", msg->header.frame_id.c_str());
    ASSERT_EQ(width, msg->width);
    ASSERT_EQ(height, msg->height);
    ASSERT_STREQ("Encoding 1", msg->encoding.c_str());
    ASSERT_TRUE(msg->is_bigendian);
    ASSERT_EQ(1u, msg->step);
    ASSERT_EQ(width * height, msg->data.size());
    for (auto &e : msg->data)
    {
      ASSERT_EQ(0x12, e);
    }
    number_of_images += 1;
  }
  ASSERT_EQ(number_of_iterations, number_of_images);
  ros_bag.close();
}

TEST_P(DirectBagTestSuite, test_record_bag_small_messages)
{
  std::string bag_name = "test_direct_small_messages.bag";
  // Write a bag with many small ROS messages
  // Explicitly set the chunk size to 768kb
  rosbag_direct_write::DirectBag bag(bag_name, 768 * 1024);
  size_t number_of_iterations = 2500;  // This is enough to make two chunks
  sensor_msgs::Imu imu;
  imu.header.stamp.fromSec(ros::WallTime::now().toSec());
  imu.header.frame_id = "/imu_frame";
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    imu.header.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("imu", imu.header.stamp, imu);
  }
  bag.close();
  // Read bag with normal rosbag interface
  rosbag::Bag ros_bag;
  ros_bag.open(bag_name, rosbag::bagmode::Read);
  ASSERT_EQ(ros_bag.chunk_count_, 2u);
  std::vector<std::string> topics;
  topics.push_back(std::string("imu"));
  rosbag::View view(ros_bag, rosbag::TopicQuery(topics));
  size_t number_of_imu_messages = 0;
  for (auto &m : view)
  {
    ASSERT_STREQ("imu", m.getTopic().c_str());
    auto msg = m.instantiate<sensor_msgs::Imu>();
    ASSERT_NE(nullptr, msg);
    ASSERT_STREQ("/imu_frame", msg->header.frame_id.c_str());
    number_of_imu_messages += 1;
  }
  ASSERT_EQ(number_of_iterations, number_of_imu_messages);
  ros_bag.close();
}

TEST_P(DirectBagTestSuite, test_record_bag_mixed)
{
  std::string bag_name = "test_direct_mixed.bag";
  // Write a bag with many small ROS messages
  // Explicitly set the chunk size to 768kb
  rosbag_direct_write::DirectBag bag(bag_name, 768 * 1024);
  size_t number_of_iterations = 250;
  size_t number_of_imu_per_iteration = 10;
  __custom_image image;
  image.stamp.fromSec(ros::WallTime::now().toSec());
  image.data = rosbag_direct_write::VectorBuffer(640 * 480, 0x12);
  __custom_point_stamped imu;
  imu.stamp.fromSec(ros::WallTime::now().toSec());
  imu.x = 1.0;
  imu.y = 2.0;
  imu.z = 3.0;
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    for (size_t j = 0; j < number_of_imu_per_iteration; ++j)
    {
      imu.stamp.fromSec(ros::WallTime::now().toSec());

      bag.write("point_stamped", imu.stamp, imu);
    }
    image.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("camera", image.stamp, image);
  }
  bag.close();
  // Read bag with normal rosbag interface
  rosbag::Bag ros_bag;
  ros_bag.open(bag_name, rosbag::bagmode::Read);
  std::vector<std::string> topics;
  topics.push_back(std::string("point_stamped"));
  topics.push_back(std::string("camera"));
  rosbag::View view(ros_bag, rosbag::TopicQuery(topics));
  size_t number_of_imu_messages = 0;
  size_t number_of_image_messages = 0;
  for (auto &m : view)
  {
    auto image_msg = m.instantiate<sensor_msgs::Image>();
    auto imu_msg = m.instantiate<geometry_msgs::PointStamped>();
    ASSERT_TRUE(image_msg != nullptr || imu_msg != nullptr);
    if (image_msg != nullptr)
    {
      number_of_image_messages += 1;
    }
    if (imu_msg != nullptr)
    {
      ASSERT_EQ(imu_msg->point.x, 1);
      ASSERT_EQ(imu_msg->point.y, 2);
      ASSERT_EQ(imu_msg->point.z, 3);
      number_of_imu_messages += 1;
    }
  }
  ASSERT_EQ(number_of_iterations * number_of_imu_per_iteration,
            number_of_imu_messages);
  ASSERT_EQ(number_of_iterations, number_of_image_messages);
  ros_bag.close();
}

TEST_P(DirectBagTestSuite, test_record_complex_messages)
{
  std::string bag_name = "test_direct_complex_messages.bag";
  rosbag_direct_write::DirectBag bag(bag_name, GetParam());
  size_t number_of_iterations = 10;
  size_t number_of_imu_per_iteration = 10;
  test_direct_bag::AlignedPointCloud2 pc2;
  pc2.header.stamp.fromSec(ros::WallTime::now().toSec());
  pc2.height = 1;  // Unordered
  pc2.width = 2000;  // Bigger than 4096 bytes, but not a multiple on purpose.
  sensor_msgs::PointField pc2_field;
  pc2_field.name = "position";
  pc2_field.offset = 0;
  pc2_field.datatype = sensor_msgs::PointField::UINT32;
  pc2_field.count = 3;
  pc2.fields.push_back(pc2_field);
  pc2.is_bigendian = false;
  pc2.point_step = sizeof(uint32_t) * pc2_field.count;
  pc2.row_step = pc2.point_step * pc2.width;
  {
    rosbag_direct_write::VectorBuffer data(pc2.row_step * pc2.height, 0x20);
    pc2.data.swap(data);
  }
  // Shouldn't be a multiple of 4096 in order to test odd sized point clouds.
  EXPECT_NE(pc2.data.size() % 4096, 0u);
  __custom_point_stamped imu;
  imu.stamp.fromSec(ros::WallTime::now().toSec());
  imu.x = 1.0;
  imu.y = 2.0;
  imu.z = 3.0;
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    for (size_t j = 0; j < number_of_imu_per_iteration; ++j)
    {
      imu.stamp.fromSec(ros::WallTime::now().toSec());

      bag.write("point_stamped", imu.stamp, imu);
    }
    pc2.header.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("points", pc2.header.stamp, pc2);
  }
  bag.close();
  // Read bag with normal rosbag interface
  rosbag::Bag ros_bag;
  ros_bag.open(bag_name, rosbag::bagmode::Read);
  ASSERT_EQ(ros_bag.chunk_count_, number_of_iterations);
  std::vector<std::string> topics;
  topics.push_back(std::string("points"));
  rosbag::View view(ros_bag, rosbag::TopicQuery(topics));
  size_t number_of_imu_messages = 0;
  size_t number_of_pc2_messages = 0;
  for (auto &m : view)
  {
    auto pc2_msg = m.instantiate<sensor_msgs::PointCloud2>();
    auto imu_msg = m.instantiate<geometry_msgs::PointStamped>();
    ASSERT_TRUE(pc2_msg != nullptr || imu_msg != nullptr);
    if (pc2_msg != nullptr)
    {
      ASSERT_EQ(pc2.height, pc2_msg->height);
      ASSERT_EQ(pc2.width, pc2_msg->width);
      ASSERT_EQ(pc2.fields.size(), pc2_msg->fields.size());
      ASSERT_STREQ(pc2.fields[0].name.c_str(),
                   pc2_msg->fields[0].name.c_str());
      ASSERT_EQ(pc2.data.size(), pc2_msg->data.size());
      number_of_pc2_messages += 1;
    }
    if (imu_msg != nullptr)
    {
      ASSERT_EQ(imu_msg->point.x, 1);
      ASSERT_EQ(imu_msg->point.y, 2);
      ASSERT_EQ(imu_msg->point.z, 3);
      number_of_imu_messages += 1;
    }
  }
  ASSERT_EQ(number_of_iterations, number_of_pc2_messages);
  ros_bag.close();
}

TEST_P(DirectBagTestSuite, test_one_bag)
{
  std::string folder_name = "test_one_bag/a/b/c";
  rosbag_direct_write::DirectBagCollection bag;
  bag.open_directory(folder_name, GetParam(), "foo");
  size_t number_of_iterations = 1;
  size_t width = 320, height = 200;
  sensor_msgs::Image image;
  image.header.stamp.fromSec(ros::WallTime::now().toSec());
  image.header.frame_id = "/camera_frame";
  image.height = height;
  image.width = width;
  image.encoding = "Encoding 1";
  image.is_bigendian = true;
  image.step = 1;
  image.data = std::vector<uint8_t>(width * height, 0x12);
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    image.header.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("camera", image.header.stamp, image);
  }
  auto bag_files = bag.close();
  ASSERT_EQ(bag_files.size(), 1u);
  ASSERT_EQ(bag_files[0], folder_name + '/' + std::string("foo-0001.bag"));
  // Read bag with normal rosbag interface
  std::vector<rosbag_direct_write::shared_ptr<rosbag::Bag>> bags;
  std::vector<std::string> topics;
  topics.push_back(std::string("camera"));
  rosbag::View view;
  rosbag::TopicQuery topic_query(topics);
  for (auto &bag_file : bag_files)
  {
    bags.emplace_back(new rosbag::Bag(bag_file, rosbag::bagmode::Read));
    view.addQuery(*bags.back(), topic_query);
  }
  size_t number_of_images = 0;
  for (auto &m : view)
  {
    ASSERT_STREQ("camera", m.getTopic().c_str());
    auto msg = m.instantiate<sensor_msgs::Image>();
    ASSERT_NE(nullptr, msg);
    ASSERT_STREQ("/camera_frame", msg->header.frame_id.c_str());
    ASSERT_EQ(width, msg->width);
    ASSERT_EQ(height, msg->height);
    ASSERT_STREQ("Encoding 1", msg->encoding.c_str());
    ASSERT_TRUE(msg->is_bigendian);
    ASSERT_EQ(1u, msg->step);
    ASSERT_EQ(width * height, msg->data.size());
    for (auto &e : msg->data)
    {
      ASSERT_EQ(0x12, e);
    }
    number_of_images += 1;
  }
  ASSERT_EQ(number_of_iterations, number_of_images);
  for (auto &rosbag : bags)
  {
    rosbag->close();
  }
}

TEST_P(DirectBagTestSuite, test_open_directory)
{
  std::string folder_name = "test_open_directory/a/b/c";
  rosbag_direct_write::DirectBagCollection bag;
  bag.open_directory(
    folder_name,  // Folder to put bags in
    GetParam(),   // O_DIRECT enabled?
    "",           // Prefix for bag file names
    4096,         // 4096KB chunk threshold
    256 * 1024,   // 256KB bag file size threshold
    3             // Width of number suffix, e.g. 001.bag, 002.bag, ...
  );
  size_t number_of_iterations = 3;
  // A 1024 x 768 image should be more than enough to close the bag file
  size_t width = 1024, height = 768;
  sensor_msgs::Image image;
  image.header.stamp.fromSec(ros::WallTime::now().toSec());
  image.header.frame_id = "/camera_frame";
  image.height = height;
  image.width = width;
  image.encoding = "Encoding 1";
  image.is_bigendian = true;
  image.step = 1;
  image.data = std::vector<uint8_t>(width * height, 0x12);
  for (size_t i = 0; i < number_of_iterations; ++i)
  {
    image.header.stamp.fromSec(ros::WallTime::now().toSec());

    bag.write("camera", image.header.stamp, image);
  }
  auto bag_files = bag.close();
  ASSERT_EQ(bag_files.size(), 3u);  // One bag per image message
  ASSERT_EQ(bag_files[0], folder_name + '/' + std::string("001.bag"));
  // Read bag with normal rosbag interface
  std::vector<rosbag_direct_write::shared_ptr<rosbag::Bag>> bags;
  std::vector<std::string> topics;
  topics.push_back(std::string("camera"));
  rosbag::View view;
  rosbag::TopicQuery topic_query(topics);
  for (auto &bag_file : bag_files)
  {
    bags.emplace_back(new rosbag::Bag(bag_file, rosbag::bagmode::Read));
    view.addQuery(*bags.back(), topic_query);
  }
  size_t number_of_images = 0;
  for (auto &m : view)
  {
    ASSERT_STREQ("camera", m.getTopic().c_str());
    auto msg = m.instantiate<sensor_msgs::Image>();
    ASSERT_NE(nullptr, msg);
    ASSERT_STREQ("/camera_frame", msg->header.frame_id.c_str());
    ASSERT_EQ(width, msg->width);
    ASSERT_EQ(height, msg->height);
    ASSERT_STREQ("Encoding 1", msg->encoding.c_str());
    ASSERT_TRUE(msg->is_bigendian);
    ASSERT_EQ(1u, msg->step);
    ASSERT_EQ(width * height, msg->data.size());
    for (auto &e : msg->data)
    {
      ASSERT_EQ(0x12, e);
    }
    number_of_images += 1;
  }
  ASSERT_EQ(number_of_iterations, number_of_images);
  for (auto &rosbag : bags)
  {
    rosbag->close();
  }
}

TEST_P(DirectBagTestSuite, test_no_messages)
{
  std::string folder_name = "test_no_messages/a/b/c";
  rosbag_direct_write::DirectBagCollection bag;
  bag.open_directory(folder_name, GetParam());
  auto bag_files = bag.close();
  ASSERT_EQ(bag_files.size(), 1u);
  rosbag::Bag ros_bag(bag_files[0], rosbag::bagmode::Read);
  ros_bag.close();
}

INSTANTIATE_TEST_CASE_P(DirectBagWithODirect,
                        DirectBagTestSuite,
                        ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(DirectBagWithoutODirect,
                        DirectBagTestSuite,
                        ::testing::Values(false));

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
