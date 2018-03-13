add_executable(test_touch_convert test_touch_convert.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_touch_convert TouchParquet)

add_executable(test_circuit_convert test_parquet2syn.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_circuit_convert CircuitParquet)

add_executable(test_multi_parquet2syn test_multi_parquet2syn.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_multi_parquet2syn CircuitParquet)

add_executable(test_circuit_convert_parallel test_parallel_parquet2syn.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_circuit_convert_parallel CircuitParquet ${MPI_LIBRARIES})

add_test(
    NAME touches_conversion
    COMMAND test_touch_convert ${CMAKE_CURRENT_SOURCE_DIR}/touchesData.0
)

add_test(
    NAME circuit_conversion
    COMMAND test_circuit_convert_parallel ${CMAKE_CURRENT_SOURCE_DIR}/testCircuit.parquet
)


add_test(
    NAME multifile_parquet2syn
    COMMAND test_multi_parquet2syn
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part0.parquet
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part1.parquet
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part2.parquet
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part3.parquet
)
