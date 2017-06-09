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

#ifndef ROSBAG_BAG_DIRECT_DIRECT_BAG_DEPENDENCIES_H
#define ROSBAG_BAG_DIRECT_DIRECT_BAG_DEPENDENCIES_H

#include <ros/header.h>
#include <ros/message_traits.h>
#include <ros/serialization.h>

#include <rosbag/bag.h>  // Included for definition of BagMode, nothing else

#include <rosbag/constants.h>
#include <rosbag/exceptions.h>
#include <rosbag/structures.h>

#define ROSBAG_DIRECT_WRITE_DECL ROSBAG_DECL

namespace rosbag_direct_write {using boost::shared_ptr;}

#endif  /* ROSBAG_BAG_DIRECT_DIRECT_BAG_DEPENDENCIES_H */
