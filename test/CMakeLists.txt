catkin_add_gtest(aligned_allocator-test ../test/test_aligned_allocator.cpp)
set_target_properties(aligned_allocator-test PROPERTIES COMPILE_FLAGS "-std=c++11")

catkin_add_gtest(direct_bag_impl-test ../test/test_direct_bag_impl.cpp)
target_link_libraries(direct_bag_impl-test ${catkin_LIBRARIES})
set_target_properties(direct_bag_impl-test PROPERTIES COMPILE_FLAGS "-std=c++11")
