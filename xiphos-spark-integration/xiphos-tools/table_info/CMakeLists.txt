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

set(ARROW_LIBRARIES arrow arrow_flight)

include_directories(/usr/neuroblade/nb-daxl/include 
    /usr/neuroblade/nb-arrow/include)
link_directories(/usr/neuroblade/nb-arrow/lib 
    /usr/neuroblade/nb-daxl/lib)

add_executable(table_info table_info.cpp)
target_link_libraries(table_info neuroblade-daxl ${ARROW_LIBRARIES})
