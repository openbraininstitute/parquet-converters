cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
set(CMAKE_BUILD_TYPE Debug)
ADD_DEFINITIONS( -std=c++11 )
ADD_DEFINITIONS( -Wall -Wextra -Wno-unused-parameter )

find_package(OpenMP)

add_library(TouchParquet STATIC loader.cpp parquetwriter.cpp)
target_link_libraries(TouchParquet parquet arrow arrow_io)

add_executable(test_convert test_convert.cpp )
target_link_libraries(test_convert TouchParquet)

add_executable(touch2parquet touch2parquet.cpp)
target_link_libraries(touch2parquet TouchParquet)
if (OPENMP_FOUND)
    set_target_properties( touch2parquet PROPERTIES
        COMPILE_FLAGS ${OpenMP_CXX_FLAGS}
        LINK_FLAGS ${OpenMP_CXX_FLAGS} )
endif()
