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

/* The purpose of this header is to abstract the #include of some dependencies.
 *
 * This allows for including headers from dependencies in differnet namespaces
 * without maintain complex patches.
 */

#ifndef ROSBAG_BAG_DIRECT_DIRECT_BAG_IMPL_DEPENDENCIES_H
#define ROSBAG_BAG_DIRECT_DIRECT_BAG_IMPL_DEPENDENCIES_H

#include <rosbag/stream.h>  // For CompressionType Enum

#include <rosbag/bag.h>

#define ROSBAG_DIRECT_WRITE_HEADER_BUFFER_TYPE boost::shared_array

#endif  /* ROSBAG_BAG_DIRECT_DIRECT_BAG_IMPL_DEPENDENCIES_H */
