cmake_minimum_required (VERSION 3.16)

set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

project (xiphosJNI)

add_compile_options(-Wno-attributes)

set (JNI_HEADER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jni)
set (JNI_HEADER ${JNI_HEADER_DIR}/com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp.h)
set (JNI_SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/../src/main/java/com/intel/dbio/sources/datasourcev2/xiphosv2/XiphosJniImp.java)
set (TARGET_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../target)

#find_package(Arrow CONFIG REQUIRED)
#message(STATUS "Using arrow ${ARROW_VERSION}")
set (ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../..)
set (releases_dir ${ROOT}/cpp/build/releases)
include_directories(${releases_dir}/include /usr/neuroblade/nb-daxl/include /usr/neuroblade/nb-arrow/include)
link_directories(${releases_dir} /usr/neuroblade/nb-daxl/lib)
message("releases_dir=${releases_dir}")
set(ARROW_LIBRARIES ${releases_dir}/libarrow.so.400 ${releases_dir}/libarrow_flight.so.400)

find_package(JNI)

if (JNI_FOUND) 
    message("using ${JNI_INCLUDE_DIRS}")
    message("using ${JNI_LIBRARIES}")
else()
    message(FATAL_ERROR "couldn't find JNI")
endif()

add_custom_command(
    OUTPUT ${JNI_HEADER}
    COMMAND mkdir -p /tmp/header_gen && javac -h ${JNI_HEADER_DIR} -d /tmp/header_gen ${JNI_SOURCE} && rm -rf /tmp/header_gen
    DEPENDS ${JNI_SOURCE}
)

set (SRC
    src/xiphosUtils.h
    src/xiphosJNI.cc
    ${JNI_HEADER}
    )

set(NB_LIBRARIES libneuroblade-daxl.so)

add_library(${PROJECT_NAME} SHARED ${SRC} )
target_include_directories(${PROJECT_NAME} PUBLIC 
    ${JNI_HEADER_DIR} 
    ${JNI_INCLUDE_DIRS})

#target_link_libraries(${PROJECT_NAME} ${JNI_LIBRARIES})
if (APPLE) 
    message("Using APPLE libraries")
    target_link_libraries(${PROJECT_NAME} ${NB_LIBRARIES} ${ARROW_LIBRARIES} /usr/local/Cellar/openjdk@8/1.8.0+312/libexec/openjdk.jdk/Contents/Home/jre/lib/server/libjvm.dylib)
else()
	message("Using arrow_libraries: ${ARROW_LIBRARIES}")
	target_link_libraries(${PROJECT_NAME} ${NB_LIBRARIES}  ${ARROW_LIBRARIES} ${JNI_LIBRARIES} pthread)
endif()

set_property(TARGET ${PROJECT_NAME}  PROPERTY CXX_STANDARD 17)

# for completness, copy the result library into the Java target dir
add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TARGET_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${PROJECT_NAME}> ${TARGET_DIR})

IF( EXISTS "${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json" )
    EXECUTE_PROCESS( COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
        )
ENDIF()

