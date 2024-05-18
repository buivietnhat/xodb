include(ExternalProject)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE) # don't override our compiler/linker options when building gtest
add_subdirectory(googletest)
add_subdirectory(fmt)
add_subdirectory(backward-cpp)
add_subdirectory(concurrentqueue)

set(XXHASH_PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/xxhash)
ExternalProject_Add(
        xxHash
        SOURCE_DIR ${XXHASH_PROJECT_DIR}
        CONFIGURE_COMMAND ""  # Skip the configure step if there's no configure script
        BUILD_COMMAND make    # Command to build the external project
        INSTALL_COMMAND ""    # Skip the install step if there's no install script
        BUILD_IN_SOURCE 1     # Build in the source directory
)
