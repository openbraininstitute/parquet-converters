cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
add_definitions(-std=c++11)
add_definitions(-Wall -Wextra -Wno-unused-parameter)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

option(PARQUET_STATICALLY_LINKED "Whether parquet was statically linked and we dont have to find its dependencies" ON)


# Dependencies
find_package(OpenMP)
find_package(Boost COMPONENTS regex)
find_path(PARQUET_INCLUDE_DIR "parquet")
find_library(PARQUET_LIB NAMES libparquet.a parquet)

# Transitive dependencies. Give preference to static for the critical ones
if(NOT PARQUET_STATICALLY_LINKED)
    find_path(ARROW_INCLUDE_DIR "arrow")
    find_library(ARROW_LIB NAMES libarrow.a arrow)
    find_library(THRIFT_LIB NAMES thrift thriftd)
    find_library(SNAPPY_LIB NAMES libsnappy.a snappy)
    find_library(Z_LIB NAMES libz.a z)
endif()


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

add_custom_target( touch2parquet_endian ALL
                   COMMAND ln -sf touch2parquet touch2parquet_endian
                   DEPENDS touch2parquet
                   COMMENT "Generating touch2parquet_endian")

# TESTS
add_subdirectory(tests)


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(FILES ${CMAKE_BINARY_DIR}/touch2parquet_endian DESTINATION bin)
