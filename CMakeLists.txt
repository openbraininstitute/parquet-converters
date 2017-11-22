cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
ADD_DEFINITIONS( -std=c++11 )
ADD_DEFINITIONS( -Wall -Wextra -Wno-unused-parameter )

# Dependencies
find_package(OpenMP)
find_package(Boost COMPONENTS regex)
find_path(PARQUET_INCLUDE_DIR "parquet")
find_path(ARROW_INCLUDE_DIR "arrow")
# Libraries. Give preference to static for the critical ones
find_library(PARQUET_LIB NAMES libparquet.a parquet)
find_library(ARROW_LIB NAMES libarrow.a arrow)
find_library(THRIFT_LIB NAMES thrift thriftd)
find_library(SNAPPY_LIB NAMES libsnappy.a snappy)
find_library(Z_LIB NAMES libz.a z)

# Lib
add_subdirectory(lib)
include_directories(lib ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

# MAIN EXECUTABLE
add_executable(touch2parquet touch2parquet.cpp)
target_link_libraries(touch2parquet TouchParquet)

if (OPENMP_FOUND)
    set_target_properties( touch2parquet PROPERTIES
        COMPILE_FLAGS ${OpenMP_CXX_FLAGS}
        LINK_FLAGS ${OpenMP_CXX_FLAGS} )
endif()

ADD_CUSTOM_TARGET( touch2parquet_endian ALL
                   COMMAND ln -sf touch2parquet touch2parquet_endian
                   DEPENDS touch2parquet
                   COMMENT "Generating touch2parquet_endian")

# TESTS
add_subdirectory(tests)


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(FILES ${CMAKE_BINARY_DIR}/touch2parquet_endian DESTINATION bin)
