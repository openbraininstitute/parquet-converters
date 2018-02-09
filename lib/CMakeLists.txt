
set(TOUCH_SRCS touch_reader.cpp touch_writer_parquet.cpp)

include_directories(${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})

add_library(TouchParquet STATIC ${TOUCH_SRCS})
add_library(ConversionProgress OBJECT progress.cpp)

if(PARQUET_STATICALLY_LINKED)
    target_link_libraries(TouchParquet ${PARQUET_LIB} ${Boost_LIBRARIES})
else()
    target_link_libraries(TouchParquet ${PARQUET_LIB} ${ARROW_LIB} ${THRIFT_LIB} ${SNAPPY_LIB} ${Boost_LIBRARIES} ${SNAPPY_LIB} ${Z_LIB})
endif()

