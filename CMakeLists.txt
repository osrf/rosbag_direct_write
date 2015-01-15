cmake_minimum_required(VERSION 2.8.3)
project(rosbag_direct_write)

find_package(catkin REQUIRED COMPONENTS cpp_common rosbag_storage)

catkin_package(CATKIN_DEPENDS cpp_common rosbag_storage)

if(CATKIN_ENABLE_TESTING)
  include_directories(include)
  # Make sure include_directories and catkin_LIBRARIES are setup before adding
  add_subdirectory(test)
endif()

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
  FILES_MATCHING PATTERN "*.h"
)
