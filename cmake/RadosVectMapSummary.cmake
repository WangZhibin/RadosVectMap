#-------------------------------------------------------------------------------
# Print the configuration summary
#-------------------------------------------------------------------------------
message(STATUS "----------------------------------------")
message(STATUS "Installation path: " ${CMAKE_INSTALL_PREFIX})
message(STATUS "C Compiler:        " ${CMAKE_C_COMPILER})
message(STATUS "C++ Compiler:      " ${CMAKE_CXX_COMPILER})
message(STATUS "Build type:        " ${CMAKE_BUILD_TYPE})
message(STATUS "")
message(STATUS "LibRados support:  " ${LIBRADOS_FOUND})
message(STATUS "GTest support:     " ${GTEST_FOUND})
message(STATUS "----------------------------------------")
