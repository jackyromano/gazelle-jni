cmake_minimum_required(VERSION 3.16)
project(spark_columnar_plugin)

set(root_directory ${PROJECT_BINARY_DIR})
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_subdirectory(src)
