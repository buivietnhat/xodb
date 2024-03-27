cmake_minimum_required(VERSION 3.10)

include(GoogleTest)

file(GLOB_RECURSE IOSERVICE_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make check-tests"
# #########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

# #########################################
# "make XYZ_test"
# #########################################
foreach (ioservice_test_source ${IOSERVICE_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(ioservice_test_filename ${ioservice_test_source} NAME)
    string(REPLACE ".cpp" "" ioservice_test_name ${ioservice_test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${ioservice_test_name} EXCLUDE_FROM_ALL ${ioservice_test_source} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
    add_dependencies(build-tests ${ioservice_test_name})
    add_dependencies(check-tests ${ioservice_test_name})

    gtest_discover_tests(${ioservice_test_name}
            EXTRA_ARGS
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${ioservice_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
    )

    target_link_libraries(${ioservice_test_name} ioservice gtest gmock_main Backward::Backward)

    # Set test target properties and dependencies.
    set_target_properties(${ioservice_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${ioservice_test_name}
    )

    # Add a Valgrind target
    add_custom_target(${ioservice_test_name}_valgrind
            COMMAND valgrind
            --leak-check=full
            --error-exitcode=1
            --tool=memcheck
            --leak-check=full
            --errors-for-leak-kinds=definite
            --show-leak-kinds=definite ./${ioservice_test_name}
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            DEPENDS ${ioservice_test_name}
    )
endforeach ()


