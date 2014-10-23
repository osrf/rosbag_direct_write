cmake_minimum_required(VERSION 2.8.3)
project(rosbag_direct_write)

find_package(catkin REQUIRED COMPONENTS rosbag_storage)

catkin_package(CATKIN_DEPENDS rosbag_storage)

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
  FILES_MATCHING PATTERN "*.h"
)
