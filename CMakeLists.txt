cmake_minimum_required(VERSION 2.8.3)
project(rosbag_direct_write)

find_package(catkin REQUIRED COMPONENTS cpp_common rosbag_storage)

catkin_package(CATKIN_DEPENDS cpp_common rosbag_storage)

include_directories(include)
add_library(${PROJECT_NAME}
  src/direct_bag.cpp)

if(CATKIN_ENABLE_TESTING)
  find_package(sensor_msgs REQUIRED)
  list(APPEND catkin_LIBRARIES ${sensor_msgs_LIBRARIES})
  include_directories(include ${catkin_INCLUDE_DIRS} ${sensor_msgs_INCLUDE_DIRS})
  # Make sure include_directories and catkin_LIBRARIES are setup before adding
  add_subdirectory(test)
endif()

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
  FILES_MATCHING PATTERN "*.h"
)
