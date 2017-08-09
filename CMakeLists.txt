cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
set(CMAKE_BUILD_TYPE Debug)
ADD_DEFINITIONS( -std=c++11 )
ADD_DEFINITIONS( -Wall -Wextra -Wno-unused-parameter )

find_package(OpenMP)
find_library(PARQUET_LIB NAMES libparquet.a parquet)
find_path(PARQUET_INCLUDE_DIR "parquet")
find_library(ARROW_LIB NAMES libarrow.a arrow)
find_path(ARROW_INCLUDE_DIR "arrow")
find_library(BROTLI_LIBS NAMES libbrotli.a)
message(${BROTLI_LIBS})

find_library(THRIFT_LIB NAMES thriftd)
find_package(Boost COMPONENTS regex)

include_directories(${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

find_library(PARQUET_LIB parquet)
find_library(ARROW_LIB arrow)
find_library(ARROW_IO_LIB arrow_io)


link_libraries(${PARQUET_LIB} ${ARROW_LIB} ${ARROW_IO_LIB})

add_library(TouchParquet STATIC loader.cpp parquetwriter.cpp)
target_link_libraries(TouchParquet ${PARQUET_LIB} ${ARROW_LIB} ${THRIFT_LIB} ${Boost_LIBRARIES} ${BROTLI_LIBS} z)

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


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(FILES ${CMAKE_BINARY_DIR}/touch2parquet_endian DESTINATION bin)
