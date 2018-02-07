
add_executable(test_convert test_convert.cpp $<TARGET_OBJECTS:ConversionProgress>)
target_link_libraries(test_convert TouchParquet)

ADD_CUSTOM_TARGET( test_convert_endian ALL
                   COMMAND ln -sf test_convert test_convert_endian
                   DEPENDS test_convert
                   COMMENT "Generating test_convert_endian")
