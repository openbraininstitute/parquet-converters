cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)

option(NEURONPARQUET_USE_STATIC "Use static libraries for dependencies" ON)
option(NEURONPARQUET_USE_MPI "Enable MPI" OFF)

include(CheckCXXCompilerFlag)

CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_OPT_ARCH_NATIVE_SUPPORTED)
add_definitions(-std=c++11 -Wall -Wextra -Wcast-align -Wdisabled-optimization -Wformat=2 -Winit-self -Wlogical-op -Wnoexcept -Woverloaded-virtual -Wredundant-decls -Wsign-promo -Wstrict-null-sentinel -Wstrict-overflow=5 -Wno-unused -Wno-variadic-macros -Wno-parentheses -fdiagnostics-show-option)

if (COMPILER_OPT_ARCH_NATIVE_SUPPORTED)
    add_definitions(-march=native)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)


##
## DEPENDENCIES
##
find_package(OpenMP)
find_package(Boost COMPONENTS filesystem regex REQUIRED)

find_library(SYN2_LIB "syn2")
if(NEURONPARQUET_USE_MPI)
    find_package(MPI REQUIRED)
    add_definitions("-DNEURONPARQUET_USE_MPI")
    include_directories(${MPI_INCLUDE_PATH})

    # Some distributions have phdf5 within mpi
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH};$ENV{MPI_LIB})

endif()

# Can change according to options
unset(HDF5_C_LIBRARY_hdf5 CACHE)
unset(PARQUET_LIB CACHE)
unset(ARROW_LIB CACHE)

#set(HDF5_USE_STATIC_LIBRARIES ON)
find_package(HDF5)

if(NEURONPARQUET_USE_STATIC)
    find_library(PARQUET_LIB NAMES libparquet.a)
    find_library(ARROW_LIB NAMES libarrow.a arrow)
    find_library(THRIFT_LIB NAMES libthrift.a thrift thriftd)
    find_library(SNAPPY_LIB NAMES libsnappy.a snappy)
    find_library(Z_LIB NAMES libz.a z)
    find_library(ZSTD_LIB NAMES libzstd.a zstd)
    # attept to build without ZSTD if not found
    if(NOT ZSTD_LIB)
        set(ZSTD_LIB "")
    endif()
    set(PARQUET_LIB_DEPS ${PARQUET_LIB} ${ARROW_LIB} ${THRIFT_LIB} ${SNAPPY_LIB} ${Boost_LIBRARIES} ${SNAPPY_LIB} ${Z_LIB} ${ZSTD_LIB} ${Boost_LIBRARIES})
else()
    find_library(PARQUET_LIB NAMES parquet)
    find_library(ARROW_LIB NAMES arrow)
    set(PARQUET_LIB_DEPS ${PARQUET_LIB} ${ARROW_LIB})
endif()


## includes
find_path(PARQUET_INCLUDE_DIR "parquet")
find_path(ARROW_INCLUDE_DIR "arrow")
include_directories(${Boost_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

# Build core lib
add_subdirectory(lib)

include_directories("lib")


# MAIN EXECUTABLES
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


if(NEURONPARQUET_USE_MPI)
  add_executable(touch2parquetp touch2parquetp.cpp $<TARGET_OBJECTS:ConversionProgress>)
  target_link_libraries(touch2parquetp ${MPI_LIBRARIES} TouchParquet)

  add_custom_target( touch2parquetp_endian ALL
                     COMMAND ln -sf touch2parquetp touch2parquetp_endian
                     DEPENDS touch2parquetp
                     COMMENT "Generating touch2parquetp_endian")
endif()


add_executable(parquet2syn2 parquet2syn2.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(parquet2syn2 CircuitParquet)

if(NEURONPARQUET_USE_MPI)
    add_executable(parquet2syn2p parquet2syn2p.cpp $<TARGET_OBJECTS:ConversionProgress>)
    target_link_libraries(parquet2syn2p ${MPI_LIBRARIES} CircuitParquet)
endif()


# TESTS
enable_testing()
add_subdirectory(tests)


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(TARGETS parquet2syn2 DESTINATION bin)
install(FILES ${CMAKE_BINARY_DIR}/touch2parquet_endian DESTINATION bin)

if(NEURONPARQUET_USE_MPI)
    install(TARGETS touch2parquetp DESTINATION bin)
    install(TARGETS parquet2syn2p DESTINATION bin)
    install(FILES ${CMAKE_BINARY_DIR}/touch2parquetp_endian DESTINATION bin)
endif()
