cmake_minimum_required(VERSION 2.8)

project(neuron_parquet)
set(CMAKE_BUILD_TYPE Debug)
ADD_DEFINITIONS( -std=c++11 )
ADD_DEFINITIONS( -Wall -Wextra -Wno-unused-parameter )

file(GLOB PROJ_SRCS "*.cpp")

add_executable(neuron_parquet ${PROJ_SRCS})
target_link_libraries( neuron_parquet parquet arrow arrow_io )
