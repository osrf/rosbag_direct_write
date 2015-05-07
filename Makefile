all: build

UNAME := $(shell uname -s)

install_deps:
ifeq ($(UNAME),Darwin)
	brew tap ros/deps
	brew update
	brew outdated boost || brew upgrade boost || brew install boost
	sudo pip install rosinstall_generator wstool rosdep empy catkin_pkg
	sudo rosdep init
	rosdep update
	mkdir catkin_ws
	cd catkin_ws && rosinstall_generator cpp_common rosbag_storage sensor_msgs --rosdistro hydro --tar > deps.rosinstall
	cd catkin_ws && wstool init src deps.rosinstall
	cd catkin_ws && rosdep install --from-paths src --ignore-src -y
	cd catkin_ws && ./src/catkin/bin/catkin_make install
	echo "source catkin_ws/install/setup.bash" > setup.bash
else
	sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu precise main" > /etc/apt/sources.list.d/ros-latest.list'
	wget http://packages.ros.org/ros.key -O - | sudo apt-key add -
	sudo apt-get update
	sudo apt-get install ros-hydro-cpp-common ros-hydro-rosbag-storage ros-hydro-sensor-msgs
	echo "source /opt/ros/hydro/setup.bash" > setup.bash
endif

build/Makefile:
	@mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug ..

.PHONY: check_cmake
check_cmake:
	-@cd build && make cmake_check_build_system

build: check_cmake build/Makefile
	cd build && make

.PHONY: test
test: build
	cd build && make run_tests
	# ==> Checking test results
	@catkin_test_results build

clean:
	rm -rf build
