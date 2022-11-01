cmake_minimum_required(VERSION 2.8.12)

set(TARGET_NAME arrow)
project(${TARGET_NAME})

include_directories(include)
include_directories(../..)

set(ARROW_SOURCES arrow_extension.cpp arrow_stream_buffer.cpp)

add_library(arrow_extension STATIC ${ARROW_SOURCES})

set(PARAMETERS "-warnings")

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ARROW_SOURCES})

# For static link TODO WIP include_directories(/usr/local/include/)
set_property(TARGET arrow_loadable_extension PROPERTY CXX_STANDARD 17)
set_property(TARGET arrow_extension PROPERTY CXX_STANDARD 17)
target_link_libraries(arrow_loadable_extension /usr/local/lib/libarrow.a)
target_link_libraries(arrow_extension /usr/local/lib/libarrow.a)

install(
  TARGETS arrow_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")