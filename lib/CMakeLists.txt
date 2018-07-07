set(TOUCH_SRCS
    "touches/touch_reader.cpp"
    "touches/parquet_writer.cpp")
set(CIRCUIT_SRCS
    "circuit/parquet_reader.cpp"
    "circuit/syn2_writer.cpp"
    "circuit/syn2hdf5.cpp")

add_library(ConversionProgress OBJECT progress.cpp)
add_library(TouchParquet STATIC ${TOUCH_SRCS} $<TARGET_OBJECTS:ConversionProgress>)
add_library(CircuitParquet STATIC ${CIRCUIT_SRCS} $<TARGET_OBJECTS:ConversionProgress>)

target_link_libraries(TouchParquet ${PARQUET_LIB_DEPS} ${ZSTD_LIB})
target_link_libraries(CircuitParquet ${HDF5_LIBRARIES} ${PARQUET_LIB_DEPS} ${SYN2_LIB} ${ZSTD_LIB})

if(NEURONPARQUET_INSTALL_DEVEL)
    install(TARGETS TouchParquet CircuitParquet
            DESTINATION lib64)
endif()
