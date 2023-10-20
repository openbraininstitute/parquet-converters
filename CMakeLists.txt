cmake_minimum_required(VERSION 3.1)
project(neuron_parquet VERSION 0.8.1)

option(NEURONPARQUET_USE_STATIC "Use static libraries for dependencies" ON)
option(NEURONPARQUET_USE_MPI "Enable MPI" OFF)
option(NEURONPARQUET_TESTS "Build tests" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Look for libraries in 'lib64' as well as in 'lib'
set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS ON)

find_package(Git)
execute_process(
    COMMAND ${GIT_EXECUTABLE} describe --tags
    OUTPUT_VARIABLE NEURONPARQUET_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE ECODE
    ERROR_QUIET
)

if(ECODE)
    message("Failed to determine version from git, using ${CMAKE_PROJECT_VERSION}")
    set(NEURONPARQUET_VERSION ${CMAKE_PROJECT_VERSION})
endif()

if(NEURONPARQUET_USE_MPI)
    find_package(MPI REQUIRED)
    add_definitions("-DNEURONPARQUET_USE_MPI")

    # Some distributions have phdf5 within mpi
    set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH};$ENV{MPI_LIB})
endif()

find_package(Arrow REQUIRED)
get_filename_component(MY_SEARCH_DIR ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${MY_SEARCH_DIR})
find_package(HDF5 REQUIRED)
find_package(HighFive REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(Range-v3 REQUIRED)
find_package(SynapseTool REQUIRED)

add_subdirectory(${CMAKE_SOURCE_DIR}/deps/cli11)
add_subdirectory(src)

if (NEURONPARQUET_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
