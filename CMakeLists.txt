cmake_minimum_required(VERSION 3.9)
project(indexed_file)

set(CMAKE_CXX_STANDARD 11)

IF(DEBUGMODE)
    SET(CMAKE_BUILD_TYPE "Debug")
    SET(MIDDLEWARE_BUILD_TYPE \"debug\")
    MESSAGE("CMake Build Debug Version")
ELSE()
    SET(CMAKE_BUILD_TYPE "Release")
    MESSAGE("CMake Release Version")
    SET(MIDDLEWARE_BUILD_TYPE \"release\")
ENDIF(DEBUGMODE)

#设置debug与release编译选项
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

#设置目标库路径(输出路径和链接路径)
#SET(MIDDLEWARE_DST_DIR ${PROJECT_SOURCE_DIR}/bin)
#SET(LIBRARY_OUTPUT_PATH ${MIDDLEWARE_DST_DIR})
#SET(EXECUTABLE_OUTPUT_PATH ${MIDDLEWARE_DST_DIR})

#设置源文件路径
SET(MIDDLEWARE_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

#设置头文件查找路径
SET(MIDDLEWARE_INC_DIR "${PROJECT_SOURCE_DIR}/include")

#链接库查找路径
LINK_DIRECTORIES(
        ${MIDDLEWARE_DST_DIR}
        ${PROJECT_SOURCE_DIR}/libs
)

#增加子项目路径
#ADD_SUBDIRECTORY(${MIDDLEWARE_SRC_DIR}/FaceClassifySort)

add_executable(indexed_file
        src/api/io.h
        src/api/reader.h
        src/api/schema.h
        src/api/writer.h
        src/common/util/visibility.h
        src/common/status.cc
        src/common/status.h
        src/exception.cc
        src/exception.h
        main.c)
