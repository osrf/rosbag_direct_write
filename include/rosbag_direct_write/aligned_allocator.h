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

/* Note: Some of this was inspired by Boost::Align.
 * See: https://github.com/boostorg/align
 * Boost Align License: http://boost.org/LICENSE_1_0.txt
 */

#ifndef ROSBAG_DIRECT_WRITE_ALIGNED_ALLOCATOR_H
#define ROSBAG_DIRECT_WRITE_ALIGNED_ALLOCATOR_H

#include <stdlib.h>
#include <exception>
#include <memory>

template <class T, std::size_t N>
class aligned_allocator
{
public:
  typedef T value_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef void* void_pointer;
  typedef const void* const_void_pointer;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  typedef T& reference;
  typedef const T& const_reference;

public:
  template <class U>
  struct rebind
  {
    typedef aligned_allocator<U, N> other;
  };

  aligned_allocator() = default;

  template <class U>
  aligned_allocator(const aligned_allocator<U, N>&) {}

  pointer address(reference value) const
  {
    return std::addressof(value);
  }

  const_pointer address(const_reference value) const
  {
    return std::addressof(value);
  }

  pointer allocate(size_type size, const_void_pointer = 0)
  {
#if __ANDROID__
    void* p = memalign(N, sizeof(T) * size);
#else
    void* p;
    if (0 != posix_memalign(&p, N, sizeof(T) * size))
    {
      throw std::bad_alloc();
    }
#endif
    if (!p && size > 0)
    {
      throw std::bad_alloc();
    }
    return static_cast<T*>(p);
  }

  void deallocate(pointer ptr, size_type)
  {
    free(ptr);
  }

  size_type max_size() const
  {
    return ~static_cast<std::size_t>(0) / sizeof(T);
  }

  template <class U, class... Args>
  void construct(U* ptr, Args&&... args)
  {
    void* p = ptr;
    new (p) U(std::forward<Args>(args)...);
  }

  template <class U>
  void construct(U* ptr)
  {
    void* p = ptr;
    new (p) U();
  }

  template <class U>
  void destroy(U* ptr)
  {
    (void)ptr;
    ptr->~U();
  }
};

template <std::size_t N>
class aligned_allocator<void, N>
{
public:
  typedef void value_type;
  typedef void* pointer;
  typedef const void* const_pointer;

  template <class U>
  struct rebind
  {
    typedef aligned_allocator<U, N> other;
  };
};

template <class T1, class T2, std::size_t N>
inline bool operator==(const aligned_allocator<T1, N>&,
                       const aligned_allocator<T2, N>&)
{
  return true;
}

template <class T1, class T2, std::size_t N>
inline bool operator!=(const aligned_allocator<T1, N>&,
                       const aligned_allocator<T2, N>&)
{
  return false;
}

#endif  /* ROSBAG_DIRECT_WRITE_ALIGNED_ALLOCATOR_H */
