cmake_minimum_required(VERSION 2.8.12)

# Building Arrow
include(ExternalProject)
ExternalProject_Add(
        arrow_ep
        GIT_REPOSITORY "https://github.com/apache/arrow"
        GIT_TAG 4755e0f24411a02ad5cd839a64ca2d1a43f6df6c
        PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
        INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
        BUILD_BYPRODUCTS
        <INSTALL_DIR>/lib/libarrow.a
        CONFIGURE_COMMAND
            ${CMAKE_COMMAND} -G${CMAKE_GENERATOR}
            -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/arrow/install
            -DARROW_BUILD_STATIC=ON
            -DARROW_BUILD_SHARED=OFF
            -DARROW_NO_DEPRECATED_API=ON
            -DARROW_POSITION_INDEPENDENT_CODE=ON
            -DARROW_SIMD_LEVEL=NONE
            -DARROW_ENABLE_TIMING_TESTS=OFF
            -DARROW_IPC=ON
            -DARROW_JEMALLOC=OFF
            -DARROW_DEPENDENCY_SOURCE=BUNDLED
            -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF
            -DARROW_DEPENDENCY_USE_SHARED=OFF
            -DARROW_BOOST_USE_SHARED=OFF
            -DARROW_BROTLI_USE_SHARED=OFF
            -DARROW_BZ2_USE_SHARED=OFF
            -DARROW_GFLAGS_USE_SHARED=OFF
            -DARROW_GRPC_USE_SHARED=OFF
            -DARROW_JEMALLOC_USE_SHARED=OFF
            -DARROW_LZ4_USE_SHARED=OFF
            -DARROW_OPENSSL_USE_SHARED=OFF
            -DARROW_PROTOBUF_USE_SHARED=OFF
            -DARROW_SNAPPY_USE_SHARED=OFF
            -DARROW_THRIFT_USE_SHARED=OFF
            -DARROW_UTF8PROC_USE_SHARED=OFF
            -DARROW_ZSTD_USE_SHARED=OFF
            -DARROW_USE_GLOG=OFF
            -DARROW_WITH_BACKTRACE=OFF
            -DARROW_WITH_OPENTELEMETRY=OFF
            -DARROW_WITH_BROTLI=OFF
            -DARROW_WITH_BZ2=OFF
            -DARROW_WITH_LZ4=OFF
            -DARROW_WITH_SNAPPY=OFF
            -DARROW_WITH_ZLIB=OFF
            -DARROW_WITH_ZSTD=OFF
            -DARROW_WITH_UCX=OFF
            -DARROW_WITH_UTF8PROC=OFF
            -DARROW_WITH_RE2=OFF
            <SOURCE_DIR>/cpp
        CMAKE_ARGS -Wno-dev
        UPDATE_COMMAND "")

ExternalProject_Get_Property(arrow_ep install_dir)
set(install_arrow ${install_dir})

# Build extension
set(TARGET_NAME arrow)
project(${TARGET_NAME})

include_directories(include)
include_directories(../..)

set(ARROW_SOURCES arrow_extension.cpp arrow_stream_buffer.cpp arrow_scan_ipc.cpp)

add_library(arrow_extension STATIC ${ARROW_SOURCES})

set(PARAMETERS "-warnings")

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ARROW_SOURCES})

include_directories(${install_arrow}/include)

set_property(TARGET arrow_loadable_extension PROPERTY CXX_STANDARD 17)
set_property(TARGET arrow_extension PROPERTY CXX_STANDARD 17)
target_link_libraries(arrow_loadable_extension ${install_arrow}/lib/libarrow.a)
target_link_libraries(arrow_extension ${install_arrow}/lib/libarrow.a)
add_dependencies(arrow_loadable_extension arrow_ep)
add_dependencies(arrow_extension arrow_ep)

install(
  TARGETS arrow_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")