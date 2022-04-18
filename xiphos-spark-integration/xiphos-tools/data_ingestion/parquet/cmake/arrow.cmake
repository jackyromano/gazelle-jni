set(ARROW_INSTALL_DIR /usr/neuroblade/nb-arrow)
message("dir: ${CMAKE_CURRENT_SOURCE_DIR}")
set(ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../../..)

#set(ARROW_INSTALL_DIR /usr/neuroblade/nb-arrow)
set(ARROW_INSTALL_DIR ${ROOT}/tools/build/arrow_install)
set(ARROW_LIBRARIES ${ARROW_INSTALL_DIR}/lib/libarrow.so ${ARROW_INSTALL_DIR}/lib/libarrow_flight.so)
set(ARROW_INCLUDE_DIR ${ARROW_INSTALL_DIR}/include)
