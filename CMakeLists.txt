cmake_minimum_required(VERSION 3.9)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/.idxcppversion" IDX_VERSION)
string(REPLACE "\n" "" IDX_VERSION "${IDX_VERSION}")
string(REGEX MATCH "^([0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?)" VERSION ${IDX_VERSION})
if(NOT VERSION)
message(FATAL_ERROR "invalid .idxcppversion")
endif()

project(indexed_file VERSION ${VERSION})

#set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 11)

#if(DEBUGMODE)
#SET(CMAKE_BUILD_TYPE "Debug")
#SET(MIDDLEWARE_BUILD_TYPE \"debug\")
#MESSAGE("CMake Build Debug Version")
#else()
#SET(CMAKE_BUILD_TYPE "Release")
#MESSAGE("CMake Release Version")
#SET(MIDDLEWARE_BUILD_TYPE \"release\")
#endif(DEBUGMODE)

#设置debug与release编译选项
#set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
#set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

set(CMAKE_C_FLAGS_DEBUG "$ENV{CFLAGS} -mavx2 -O0 -Wall -g -ggdb")
set(CMAKE_C_FLAGS_RELEASE "$ENV{CFLAGS} -mavx2 -O3 -Wall")

#设置目标库路径(输出路径和链接路径)
set(MIDDLEWARE_DST_DIR ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${MIDDLEWARE_DST_DIR})
#SET(EXECUTABLE_OUTPUT_PATH ${MIDDLEWARE_DST_DIR})

#设置源文件路径
set(MIDDLEWARE_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

#设置头文件查找路径
set(MIDDLEWARE_INC_DIR ${PROJECT_SOURCE_DIR}/include)

#链接库查找路径
#LINK_DIRECTORIES(
#${MIDDLEWARE_DST_DIR}
#${PROJECT_SOURCE_DIR}/libs
#)

include_directories(
${MIDDLEWARE_SRC_DIR}
${MIDDLEWARE_SRC_DIR}/api
${MIDDLEWARE_SRC_DIR}/util
${MIDDLEWARE_SRC_DIR}/common
)

#增加子项目路径
#ADD_SUBDIRECTORY(${MIDDLEWARE_SRC_DIR}/FaceClassifySort)

add_executable(indexed_file
src/api/writer.c
src/api/reader.c

src/common/util/str_utils.c

src/common/io/file_meta_writer.c
src/common/io/file_meta_reader.c

src/common/io/file_meta_buffer.c
main.c)
