cmake_minimum_required(VERSION 3.10)

include(GoogleTest)

file(GLOB_RECURSE EXECUTIONENGINE_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

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
foreach (executionengine_test_source ${EXECUTIONENGINE_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(executionengine_test_filename ${executionengine_test_source} NAME)
    string(REPLACE ".cpp" "" executionengine_test_name ${executionengine_test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${executionengine_test_name} EXCLUDE_FROM_ALL ${executionengine_test_source} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
    add_dependencies(build-tests ${executionengine_test_name})
    add_dependencies(check-tests ${executionengine_test_name})

    gtest_discover_tests(${executionengine_test_name}
            EXTRA_ARGS
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${executionengine_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
    )

    target_link_libraries(${executionengine_test_name} executionengine gtest gmock_main Backward::Backward)

    # Set test target properties and dependencies.
    set_target_properties(${executionengine_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${executionengine_test_name}
    )
endforeach ()


