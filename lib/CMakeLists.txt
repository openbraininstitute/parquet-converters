set(TOUCH_SRCS touch_reader.cpp touch_writer_parquet.cpp)
set(CIRCUIT_SRCS circuit_reader_parquet.cpp circuit_writer_syn2.cpp syn2_circuit.cpp)

include_directories(${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

add_library(TouchParquet STATIC ${TOUCH_SRCS})
add_library(CircuitParquet STATIC ${CIRCUIT_SRCS})
add_library(ConversionProgress OBJECT progress.cpp)

target_link_libraries(TouchParquet ${PARQUET_LIB_DEPS} ${ZSTD_LIB})
target_link_libraries(CircuitParquet ${HDF5_LIBRARIES} ${PARQUET_LIB_DEPS} ${SYN2_LIB} ${ZSTD_LIB})
