cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(ThreadPool LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)


add_library(thr
	STATIC
	thread_pool.h
	thread_pool.cc
)
target_link_libraries(thr PRIVATE arrow_shared)
target_link_libraries(thr PRIVATE Threads::Threads)

enable_testing()

add_executable(
  thr_test
  thread_pool_test.cc
)

target_link_libraries(
  thr_test
  gmock
  gtest
  gtest_main
)

target_link_libraries(
  thr_test
  thr
)

target_link_libraries(
  thr_test
)

include(GoogleTest)
gtest_discover_tests(thr_test)
