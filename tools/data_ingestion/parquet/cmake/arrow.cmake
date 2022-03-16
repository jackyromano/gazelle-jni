set(ARROW_INSTALL_DIR /usr/neuroblade/nb-arrow)
set(ORIG_CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH})
set(CMAKE_PREFIX_PATH ${ARROW_INSTALL_DIR})

# There is a bug here, when installing arrow from source
# The package paths are not resolved and need to be set manually
set(ARROW_INSTALL_DIR /usr/neuroblade/nb-arrow)
find_package(Arrow CONFIG REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
message(STATUS "Using arrow ${ARROW_VERSION}")
set(ARROW_LIBRARIES arrow arrow_flight)
set(ARROW_LIBRARIES ${ARROW_INSTALL_DIR}/lib/libarrow.so ${ARROW_INSTALL_DIR}/lib/libarrow_flight.so)
set(ARROW_INCLUDE_DIR ${ARROW_INSTALL_DIR}/include)
