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

set (ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
set (releases_dir ${ROOT}/cpp/build/releases)
include_directories(${releases_dir}/include /usr/neuroblade/nb-daxl/include /usr/neuroblade/nb-arrow/include)
link_directories(${releases_dir} /usr/neuroblade/nb-daxl/lib)
set(ARROW_LIBRARIES ${releases_dir}/libarrow.so.400 ${releases_dir}/libarrow_flight.so.400)
set(NB_DAXL_DIR /usr/neuroblade/nb-daxl)
set(NB_LIBRARIES ${NB_DAXL_DIR}/lib/libneuroblade-daxl.so)

include_directories(/usr/neuroblade/nb-daxl/include)
#link_directories(/usr/neuroblade/nb-daxl/lib)

add_executable(table_info table_info.cc)
target_link_libraries(table_info ${NB_LIBRARIES} ${ARROW_LIBRARIES})
