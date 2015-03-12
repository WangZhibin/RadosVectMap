#------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@cern.ch>
#------------------------------------------------------------------------------

#*******************************************************************************
#* RadosVectMap                                                                *
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

# Try to find librados
# Once done, this will define
#
# LIBRADOS_FOUND        - system has librados
# LIBRADOS_INCLUDE_DIRS - librados include directories
# LIBRADOS_LIBRARIES    - libraries need to use librados

if (LIBRADOS_INCLUDE_DIRS AND LIBRADOS_LIBRARIES)
  set(LIBRADOS_FOUND_QUIETLY TRUE)
else()
  find_path(
    LIBRADOS_INCLUDE_DIR
    NAMES librados.hpp
    HINTS ${LIBRADOS_ROOT_DIR}
    PATH_SUFFIXES include/rados
  )

  find_library(
    LIBRADOS_LIBRARY
    NAMES rados
    HINTS ${LIBRADOS_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX}
  )

  set(LIBRADOS_INCLUDE_DIRS ${LIBRADOS_INCLUDE_DIR})
  set(LIBRADOS_LIBRARIES ${LIBRADOS_LIBRARY})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    LibRados
    DEFAULT_MSG
    LIBRADOS_LIBRARY
    LIBRADOS_INCLUDE_DIR
  )

   mark_as_advanced(LIBRADOS_LIBRARY LIBRADOS_INCLUDE_DIR)
endif()
