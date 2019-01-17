add_executable(test_touch_convert test_touch_convert.cpp)
target_link_libraries(test_touch_convert TouchParquet)

add_executable(test_circuit_convert test_parquet2syn.cpp)
target_link_libraries(test_circuit_convert CircuitParquet)

add_executable(test_multi_parquet2syn test_multi_parquet2syn.cpp)
target_link_libraries(test_multi_parquet2syn CircuitParquet)

if(NEURONPARQUET_USE_MPI)
    find_program(mpi_launcher
        NAMES srun mpirun ${MPIEXEC_EXECUTABLE}
        HINTS ${MPI_HOME} $ENV{MPI_HOME}
        PATHS ${_MPI_PREFIX_PATH}
        PATH_SUFFIXES bin
        DOC "Executable for running MPI programs.")

    add_executable(test_circuit_convert_parallel test_parallel_parquet2syn.cpp)
    target_link_libraries(test_circuit_convert_parallel CircuitParquet ${MPI_LIBRARIES})

    add_test(
        NAME circuit_conversion
        COMMAND ${mpi_launcher} -n 1 test_circuit_convert_parallel
            ${CMAKE_CURRENT_SOURCE_DIR}/testCircuit.parquet
    )

    add_test(
        NAME parquet2syn_mpi
        COMMAND ${mpi_launcher} -n 2 ../parquet2syn2p
            ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part0.parquet
            ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part1.parquet
            ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part2.parquet
    )
endif()

add_test(
    NAME touches_conversion
    COMMAND test_touch_convert
        ${CMAKE_CURRENT_SOURCE_DIR}/touchesData.0
)

add_test(
    NAME multifile_parquet2syn
    COMMAND ${maybe_mpi} test_multi_parquet2syn
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part0.parquet
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part1.parquet
        ${CMAKE_CURRENT_SOURCE_DIR}/parquets/part2.parquet
)

