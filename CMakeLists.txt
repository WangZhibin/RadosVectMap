#------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@cern.ch>
#------------------------------------------------------------------------------

#*******************************************************************************
#* RadosVectMap                                                                 *
#* Copyright (C) 2015 CERN/Switzerland                                         *
#*                                                                             *
#* This program is free software: you can redistribute it and/or modify        *
#* it under the terms of the GNU General Public License as published by        *
#* the Free Software Foundation, either version 3 of the License, or           *
#* (at your option) any later version.                                         *
#*                                                                             *
#* This program is distributed in the hope that it will be useful,             *
#* but WITHOUT ANY WARRANTY; without even the implied warranty of              *
#* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                *
#* GNU General Public License for more details.                                *
#*                                                                             *
#* You should have received a copy of the GNU General Public License           *
#* along with this program. If not, see <http://www.gnu.org/licenses/>.        *
#******************************************************************************/

#-------------------------------------------------------------------------------
# Project description
#-------------------------------------------------------------------------------
cmake_minimum_required( VERSION 2.8.11 )
project(RadosVectMap)

set(VERSION_MAJOR "0")
set(VERSION_MINOR "1")
set(VERSION_PATCH "0")

if (NOT RELEASE)
  set(RELEASE "head")
endif()

set(VERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")

set(CMAKE_MODULE_PATH
   ${PROJECT_SOURCE_DIR}/src
   ${PROJECT_SOURCE_DIR}/cmake)

include(RadosVectMapUtils)
CheckBuildDirectory()
include(RadosVectMapOSDefs)

if("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE RelWithDebug)
endif()

#-------------------------------------------------------------------------------
# Add dependencies
#-------------------------------------------------------------------------------
find_package(LibRados REQUIRED)

#-------------------------------------------------------------------------------
# Build in subdirectories
#-------------------------------------------------------------------------------
add_subdirectory(src)

if(BUILD_TESTS)
  find_package(GTest REQUIRED)
  enable_testing()
  add_subdirectory(tests)
endif()

include(RadosVectMapSummary)

#-------------------------------------------------------------------------------
# Create uninstall target
#-------------------------------------------------------------------------------
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(uninstall
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

#-------------------------------------------------------------------------------
# TODO: Add CPack for building RPMS
#-------------------------------------------------------------------------------
