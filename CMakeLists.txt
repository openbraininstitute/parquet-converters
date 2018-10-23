cmake_minimum_required(VERSION 3.1)
project(neuron_parquet)

option(NEURONPARQUET_USE_STATIC "Use static libraries for dependencies" ON)
option(NEURONPARQUET_USE_MPI "Enable MPI" OFF)
option(NEURONPARQUET_TESTS "Build tests" ON)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMake)
include(ReleaseDebugAutoFlags)

add_definitions(${CMAKE_CXX_WARNING_EXTREME})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Look for libraries in 'lib64' as well as in 'lib'
set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS ON)

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

find_package(HDF5)

if(NEURONPARQUET_USE_STATIC)
    find_library(PARQUET_LIB NAMES libparquet.a parquet)
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

include_directories("include")


# MAIN EXECUTABLES
add_executable(touch2parquet touch2parquet.cpp)
target_link_libraries(touch2parquet TouchParquet)

if (OPENMP_FOUND)
    set_target_properties( touch2parquet PROPERTIES
        COMPILE_FLAGS ${OpenMP_CXX_FLAGS}
        LINK_FLAGS ${OpenMP_CXX_FLAGS} )
endif()


if(NEURONPARQUET_USE_MPI)
    add_executable(touch2parquetp touch2parquetp.cpp)
    target_link_libraries(touch2parquetp ${MPI_LIBRARIES} TouchParquet)
endif()


add_executable(parquet2syn2 parquet2syn2.cpp)
target_link_libraries(parquet2syn2 CircuitParquet)

if(NEURONPARQUET_USE_MPI)
    add_executable(parquet2syn2p parquet2syn2p.cpp)
    target_link_libraries(parquet2syn2p ${MPI_LIBRARIES} CircuitParquet)
endif()


# TESTS
if (NEURONPARQUET_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()


# INSTALL
install(TARGETS touch2parquet DESTINATION bin)
install(TARGETS parquet2syn2 DESTINATION bin)

if(NEURONPARQUET_USE_MPI)
    install(TARGETS touch2parquetp DESTINATION bin)
    install(TARGETS parquet2syn2p DESTINATION bin)
endif()


# lib install
# In case the devel lib is installed to the system these header files must be copied over,
# including those under the lib sym-link
install(DIRECTORY lib DESTINATION include/neuron_parquet
        FILES_MATCHING PATTERN "*.h")
file(GLOB header_files "include/neuron_parquet/*.h")
install(FILES ${header_files}
        DESTINATION include/neuron_parquet)
