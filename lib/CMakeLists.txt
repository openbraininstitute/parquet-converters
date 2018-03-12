set(TOUCH_SRCS touch_reader.cpp touch_writer_parquet.cpp)
set(CIRCUIT_SRCS circuit_reader_parquet.cpp circuit_writer_syn2.cpp syn2_circuit.cpp)

include_directories(${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

add_library(TouchParquet STATIC ${TOUCH_SRCS})
add_library(CircuitParquet STATIC ${CIRCUIT_SRCS})
add_library(ConversionProgress OBJECT progress.cpp)

if(PARQUET_STATICALLY_LINKED)
    target_link_libraries(TouchParquet ${PARQUET_LIB} ${Boost_LIBRARIES})
    target_link_libraries(CircuitParquet ${PARQUET_LIB} ${Boost_LIBRARIES} ${HDF5_LIBRARIES} ${SYN2_LIB})
else()
    target_link_libraries(TouchParquet ${PARQUET_LIB} ${ARROW_LIB} ${THRIFT_LIB} ${SNAPPY_LIB} ${Boost_LIBRARIES} ${SNAPPY_LIB} ${Z_LIB})
    target_link_libraries(CircuitParquet ${PARQUET_LIB} ${ARROW_LIB} ${THRIFT_LIB} ${SNAPPY_LIB} ${Boost_LIBRARIES} ${SNAPPY_LIB} ${Z_LIB} ${HDF5_LIBRARIES} ${SYN2_LIB})
endif()

