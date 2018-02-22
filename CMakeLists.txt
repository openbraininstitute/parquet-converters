cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)

include(CheckCXXCompilerFlag)

CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_OPT_ARCH_NATIVE_SUPPORTED)
add_definitions(-std=c++11)
add_definitions(-Wall -Wextra -Wno-unused-parameter)

if (COMPILER_OPT_ARCH_NATIVE_SUPPORTED)
    add_definitions(-march=native)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

option(PARQUET_STATICALLY_LINKED "Whether parquet was statically linked and we dont have to find its dependencies" OFF)


# Dependencies
find_package(OpenMP)
find_package(Boost COMPONENTS regex)
find_path(PARQUET_INCLUDE_DIR "parquet")
find_library(PARQUET_LIB NAMES libparquet.a parquet)

# Should be optional
find_package(MPI)

# Some systems set MPI_LIB env dir
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} $ENV{MPI_LIB})

set(HDF5_PREFER_PARALLEL True)
find_package(HDF5)


# Transitive dependencies. Give preference to static for the critical ones
if(NOT PARQUET_STATICALLY_LINKED)
    find_path(ARROW_INCLUDE_DIR "arrow")
    find_library(ARROW_LIB NAMES libarrow.a arrow)
    find_library(THRIFT_LIB NAMES libthrift.a thrift thriftd)
    find_library(SNAPPY_LIB NAMES libsnappy.a snappy)
    find_library(Z_LIB NAMES libz.a z)
endif()


include_directories(${MPI_INCLUDE_PATH})


# Lib
add_subdirectory(lib)
include_directories(lib ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

# MAIN EXECUTABLE
add_executable(touch2parquet touch2parquet.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(touch2parquet TouchParquet)

if (OPENMP_FOUND)
    set_target_properties( touch2parquet PROPERTIES
        COMPILE_FLAGS ${OpenMP_CXX_FLAGS}
        LINK_FLAGS ${OpenMP_CXX_FLAGS} )
endif()

add_custom_target( touch2parquet_endian ALL
                   COMMAND ln -sf touch2parquet touch2parquet_endian
                   DEPENDS touch2parquet
                   COMMENT "Generating touch2parquet_endian")

# TESTS
enable_testing()
add_subdirectory(tests)


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(FILES ${CMAKE_BINARY_DIR}/touch2parquet_endian DESTINATION bin)
