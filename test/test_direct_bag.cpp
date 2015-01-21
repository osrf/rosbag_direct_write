#include <gtest/gtest.h>

#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <sensor_msgs/Image.h>

#include "rosbag_direct_write/direct_bag.h"

TEST(DirectBagTestSuite, test_record_bag)
{
  std::string bag_name = "test_direct.bag";
  // Write a bag with ROS messages
  rosbag_direct_write::DirectBag bag(bag_name);
  size_t number_of_iterations = 3;
  size_t width = 1024, height = 768;
  sensor_msgs::Image image;
  image.header.stamp.fromSec(ros::WallTime::now().toSec());
  image.header.frame_id = "/camera";
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
    ASSERT_STREQ("/camera", msg->header.frame_id.c_str());
    ASSERT_EQ(width, msg->width);
    ASSERT_EQ(height, msg->height);
    ASSERT_STREQ("Encoding 1", msg->encoding.c_str());
    ASSERT_TRUE(msg->is_bigendian);
    ASSERT_EQ(1, msg->step);
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

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
