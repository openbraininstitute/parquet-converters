set(TOUCH_SRCS
    "touches/touch_reader.cpp"
    "touches/parquet_writer.cpp")
set(CIRCUIT_SRCS
    "circuit/parquet_reader.cpp"
    "circuit/syn2_writer.cpp"
    "circuit/syn2hdf5.cpp")

add_library(TouchParquet STATIC ${TOUCH_SRCS})
add_library(CircuitParquet STATIC ${CIRCUIT_SRCS})

target_link_libraries(TouchParquet
                      ${PARQUET_LIB_DEPS}
                      ${ZSTD_LIB}
                      range-v3)
target_link_libraries(CircuitParquet
                      ${HDF5_LIBRARIES}
                      ${PARQUET_LIB_DEPS}
                      ${ZSTD_LIB}
                      SynapseTool::syn2
                      HighFive
                      range-v3)

if(NEURONPARQUET_INSTALL_DEVEL)
    install(TARGETS TouchParquet CircuitParquet
            DESTINATION lib64)
endif()
