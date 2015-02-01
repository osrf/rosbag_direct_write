all: build

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
