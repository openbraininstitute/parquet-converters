add_executable(test_touch_convert test_touch_convert.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_touch_convert TouchParquet)

add_test(
    NAME touches_conversion
    COMMAND test_touch_convert ${CMAKE_CURRENT_SOURCE_DIR}/touchesData.0
)

