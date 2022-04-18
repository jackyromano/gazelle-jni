cmake_minimum_required(VERSION 3.12.4)

project(
    neuroblade-daxl-samples
    VERSION 0.0.1
    DESCRIPTION "neuroblade daxl samples")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# language - defaults
set(CXX /usr/lib/clang++-12)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wno-attributes)

OPTION(BUILD_SHARED_LIBS "BUILD_SHARED_LIBS" OFF)

message(STATUS "root: ${CMAKE_ROOT}")

set (ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
set (releases_dir ${ROOT}/cpp/build/releases)
set(ARROW_LIBRARIES ${releases_dir}/libarrow.so.400 ${releases_dir}/libarrow_flight.so.400)
set(ARROW_INCLUDE_DIR ${releases_dir}/include)
set(NB_DAXL_DIR /usr/neuroblade/nb-daxl)
set(NB_LIBRARIES ${NB_DAXL_DIR}/lib/libneuroblade-daxl.so)

include_directories(csv)
include_directories(${NB_DAXL_DIR}/include)
include_directories(${ARROW_INCLUDE_DIR})


add_executable(etl_csv etlCsv.cc csv/CSVFileDataSource.cc csv/CSVRowBatch.cc csv/CSVtoArrowConverter.cc)
target_link_libraries(etl_csv ${NB_LIBRARIES} ${ARROW_LIBRARIES})

add_executable(strs_ddl strsDdl.cc)
target_link_libraries(strs_ddl ${NB_LIBRARIES} ${ARROW_LIBRARIES})

add_executable(plan_execute planAndExecute.cc)
target_link_libraries(plan_execute ${NB_LIBRARIES} ${ARROW_LIBRARIES})


