#------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@cern.ch>
#------------------------------------------------------------------------------

#*******************************************************************************
#* CephVectMap                                                                 *
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

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${GTEST_INCLUDE_DIRS})

add_executable(
   run_tests 
   tests.cc
   RadosMapTest.cc 
   RadosMapTest.hh
   test.conf)

target_link_libraries(
  run_tests
  ${GTEST_LIBRARIES}
  ${LIBRADOS_LIBRARIES})

add_test(
  NAME AllTestsRadosMap 
  COMMAND run_tests --conf=${CMAKE_SOURCE_DIR}/tests/test.conf)
