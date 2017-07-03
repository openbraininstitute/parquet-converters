cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
set(CMAKE_BUILD_TYPE Debug)
ADD_DEFINITIONS( -std=c++11 )
ADD_DEFINITIONS( -Wall -Wextra -Wno-unused-parameter )

find_package(OpenMP)

find_library(PARQUET_LIB parquet)
find_library(ARROW_LIB arrow)
find_library(ARROW_IO_LIB arrow_io)


link_libraries(${PARQUET_LIB} ${ARROW_LIB} ${ARROW_IO_LIB})

add_library(TouchParquet STATIC loader.cpp parquetwriter.cpp)

add_executable(test_convert test_convert.cpp )
target_link_libraries(test_convert TouchParquet)

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
