cmake_minimum_required(VERSION 3.1)
project(neuron_parquet VERSION 0.8.1)

set(CMAKE_CXX_STANDARD 17)

# Look for libraries in 'lib64' as well as in 'lib'
set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS ON)

find_package(Git)
execute_process(
  COMMAND ${GIT_EXECUTABLE} describe --tags
  OUTPUT_VARIABLE NEURONPARQUET_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE ECODE
  ERROR_QUIET)

if(ECODE)
  message(
    "Failed to determine version from git, using ${CMAKE_PROJECT_VERSION}")
  set(NEURONPARQUET_VERSION ${CMAKE_PROJECT_VERSION})
endif()

find_package(MPI REQUIRED)
find_package(Arrow REQUIRED)
get_filename_component(MY_SEARCH_DIR ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${MY_SEARCH_DIR})
find_package(HDF5 REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(Range-v3 REQUIRED)

find_package(HighFive)
if(NOT HighFive_FOUND)
  add_subdirectory(${CMAKE_SOURCE_DIR}/deps/highfive)
endif()

find_package(CLI11)
if(NOT CLI11_FOUND)
  add_subdirectory(${CMAKE_SOURCE_DIR}/deps/cli11)
endif()

add_subdirectory(src)

find_package(Catch2)
if(NOT ${Catch2_FOUND})
  add_subdirectory(deps/catch2 EXCLUDE_FROM_ALL)
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/deps/catch2/extras)
endif()

enable_testing()
add_subdirectory(tests)
cmake_minimum_required(VERSION 3.15)
project(index_writer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find MPI package
find_package(MPI REQUIRED)

# Find HDF5 package
find_package(HDF5 REQUIRED)

# Find HighFive package (assuming it's installed in a standard location)
find_package(HighFive REQUIRED)

if (CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

find_package(Python 3.8 COMPONENTS Interpreter ${DEV_MODULE} REQUIRED)

execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

# Add the index writer library
add_library(index_writer STATIC
    src/index/index.cpp
)

set_target_properties(index_writer PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Add the Python module
nanobind_add_module(index_writer_py src/index/index_bindings.cpp)

target_link_libraries(index_writer_py PRIVATE index_writer)

# Install the Python module
install(TARGETS index_writer_py DESTINATION .)
