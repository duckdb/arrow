cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME arrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/arrow_extension.cpp
    src/arrow_stream_buffer.cpp
    src/arrow_readers.cpp
    src/arrow_scan_ipc.cpp
    src/arrow_to_ipc.cpp)

if(NOT "${OSX_BUILD_ARCH}" STREQUAL "")
  set(OSX_ARCH_FLAG -DCMAKE_OSX_ARCHITECTURES=${OSX_BUILD_ARCH})
else()
  set(OSX_ARCH_FLAG "")
endif()

# Building Arrow (e03105e is for tag apache-arrow-15.0.2)
set(CMAKE_CXX_STANDARD 17)
include(ExternalProject)
ExternalProject_Add(
        ARROW_EP
        GIT_REPOSITORY "https://github.com/apache/arrow"
        GIT_TAG 6a2e19a852b367c72d7b12da4d104456491ed8b7
        PREFIX "${CMAKE_BINARY_DIR}/third_party/arrow"
        INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow/install"
        BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libarrow.a
        CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -G${CMAKE_GENERATOR}
                           ${OSX_ARCH_FLAG}
                         -DCMAKE_BUILD_TYPE=Release
                         -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/third_party/arrow/install
                         -DCMAKE_INSTALL_LIBDIR=lib
                         -DARROW_BUILD_STATIC=ON
                         -DARROW_BUILD_SHARED=OFF
                         -DARROW_DEPENDENCY_USE_SHARED=OFF
                         -DARROW_DEPENDENCY_SOURCE=BUNDLED
                         -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF
                         -DARROW_ENABLE_TIMING_TESTS=OFF
                         -DARROW_NO_DEPRECATED_API=ON
                         -DARROW_POSITION_INDEPENDENT_CODE=ON
                         -DARROW_SIMD_LEVEL=NONE
                         -DARROW_JEMALLOC=OFF
                         -DARROW_IPC=ON
                         -DARROW_JSON=OFF
                         -DARROW_ORC=OFF
                         -DARROW_CSV=OFF
                         -DARROW_PARQUET=OFF
                         -DARROW_SUBSTRAIT=OFF
                         -DARROW_ACERO=OFF
                         -DARROW_COMPUTE=OFF
                         -DARROW_DATASET=OFF
                         -DARROW_FILESYSTEM=OFF
                         -DARROW_FLIGHT=OFF
                         -DARROW_FLIGHT_SQL=OFF
                         -DARROW_GANDIVA=OFF
                         -DARROW_USE_GLOG=OFF
                         -DARROW_WITH_BACKTRACE=OFF
                         -DARROW_WITH_OPENTELEMETRY=OFF
                         -DARROW_WITH_UCX=OFF
                         -DARROW_WITH_RE2=OFF
                         -DARROW_WITH_BROTLI=OFF
                         -DARROW_WITH_BZ2=OFF
                         -DARROW_WITH_LZ4=OFF
                         -DARROW_WITH_SNAPPY=OFF
                         -DARROW_WITH_ZLIB=OFF
                         -DARROW_WITH_ZSTD=OFF
                         -DARROW_WITH_UTF8PROC=OFF
                         -DARROW_BOOST_USE_SHARED=OFF
                         -DARROW_GFLAGS_USE_SHARED=OFF
                         -DARROW_GRPC_USE_SHARED=OFF
                         -DARROW_JEMALLOC_USE_SHARED=OFF
                         -DARROW_OPENSSL_USE_SHARED=OFF
                         -DARROW_PROTOBUF_USE_SHARED=OFF
                         -DARROW_THRIFT_USE_SHARED=OFF
                         -DARROW_BROTLI_USE_SHARED=OFF
                         -DARROW_BZ2_USE_SHARED=OFF
                         -DARROW_LZ4_USE_SHARED=OFF
                         -DARROW_SNAPPY_USE_SHARED=OFF
                         -DARROW_ZSTD_USE_SHARED=OFF
                         -DARROW_UTF8PROC_USE_SHARED=OFF
                         <SOURCE_DIR>/cpp
        CMAKE_ARGS -Wno-dev
        UPDATE_COMMAND "")

ExternalProject_Get_Property(ARROW_EP install_dir)
add_library(arrow STATIC IMPORTED GLOBAL)
if(WIN32)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION
                                         ${install_dir}/lib/arrow_static.lib)
else()
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION
                                         ${install_dir}/lib/libarrow.a)
endif()

# create static library
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
add_dependencies(${EXTENSION_NAME} ARROW_EP)
target_link_libraries(${EXTENSION_NAME} PUBLIC arrow)
target_include_directories(${EXTENSION_NAME} PRIVATE ${install_dir}/include)

# create loadable extension
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
add_dependencies(${TARGET_NAME}_loadable_extension ARROW_EP)
target_link_libraries(${TARGET_NAME}_loadable_extension arrow)
if(WIN32)
  target_compile_definitions(${TARGET_NAME}_loadable_extension
                             PUBLIC ARROW_STATIC)
endif()
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE ${install_dir}/include)

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

