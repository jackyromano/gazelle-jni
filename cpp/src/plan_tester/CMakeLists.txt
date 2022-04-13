cmake_minimum_required(VERSION 3.16)
project(plan_tester)
include_directories(/usr/neuroblade/nb-daxl/include)

set(SRC
    main.cc
    )

set(NB_DIR /usr/neuroblade/nb-daxl/lib)
set(NB_LIBRARIES ${NB_DIR}/libneuroblade-daxl.so)
set(NB_STATIC_LIBRARIES /home/yromano/projects/jni3/test_libs/libneuroblade-daxl.so)
set(NB_TEST_LIBRARIES /home/yromano/projects/jni3/test_libs/libneuroblade-daxl.so)

add_executable(plan_tester ${SRC})
message("root_directory: ${root_directory}")
set(RELEASE_DIR ${root_directory}/releases)

target_link_libraries(plan_tester ${NB_TEST_LIBRARIES} spark_columnar_jni)
