cmake_minimum_required(VERSION 3.12.4)

project(
    xiphos-parquet-loader
    VERSION 0.0.1
    DESCRIPTION "neuroblade daxl samples")

set(ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../../..)
set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
set(Boost_USE_DEBUG_LIBS        OFF)  # ignore debug libs and
set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.71.0 COMPONENTS date_time)


# language - defaults
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wno-attributes)

OPTION(BUILD_SHARED_LIBS "BUILD_SHARED_LIBS" OFF)

include(cmake/arrow.cmake)
include(cmake/daxl.cmake)


include_directories(csv)
include_directories(${ARROW_INCLUDE_DIR})

file(GLOB_RECURSE source_files *.cpp)

include_directories(${Boost_INCLUDE_DIRS})

add_executable(xiphos-parquet-loader xiphos-parquet-loader.cpp)
target_link_libraries(xiphos-parquet-loader PUBLIC Boost::date_time parquet arrow  ${DAXL_LIBRARIES} )
