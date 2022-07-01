configure_file(version.h.in version.h @ONLY)

set(TOUCH_SRCS
    "touches/touch_reader.cpp"
    "touches/parquet_writer.cpp")
set(CIRCUIT_SRCS
    "circuit/parquet_reader.cpp"
    "circuit/sonata_writer.cpp"
    "circuit/sonata_file.cpp")

add_library(TouchParquet STATIC ${TOUCH_SRCS})
add_library(CircuitParquet STATIC ${CIRCUIT_SRCS})

target_include_directories(TouchParquet PUBLIC "${PROJECT_BINARY_DIR}")
target_link_libraries(TouchParquet
                      arrow_shared
                      parquet_shared
                      range-v3)
target_include_directories(CircuitParquet PUBLIC "${PROJECT_BINARY_DIR}")
target_link_libraries(CircuitParquet
                      ${HDF5_LIBRARIES}
                      arrow_shared
                      parquet_shared
                      SynapseTool::syn2
                      nlohmann_json::nlohmann_json
                      HighFive
                      range-v3)

if(NEURONPARQUET_INSTALL_DEVEL)
    install(TARGETS TouchParquet CircuitParquet
            DESTINATION lib64)
endif()
