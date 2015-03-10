#-------------------------------------------------------------------------------
# Detect in source builds
#-------------------------------------------------------------------------------
function(CheckBuildDirectory)

  # Get Real Paths of the source and binary directories
  get_filename_component(srcdir "${CMAKE_SOURCE_DIR}" REALPATH)
  get_filename_component(bindir "${CMAKE_BINARY_DIR}" REALPATH)

  # Check for in-source builds
  if(${srcdir} STREQUAL ${bindir})
    message(FATAL_ERROR "XRootD cannot be built in-source! "
                        "Please run cmake <src-dir> outside the "
                        "source directory and be sure to remove "
                        "CMakeCache.txt or CMakeFiles if they "
                        "exist in the source directory.")
  endif()

endfunction()
