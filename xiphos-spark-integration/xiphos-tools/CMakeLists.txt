cmake_minimum_required(VERSION 3.12.4)

project(xiphos-tools)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(table_info)
add_subdirectory(data_ingestion/parquet)
add_subdirectory(nb-samples)

