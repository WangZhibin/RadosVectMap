//------------------------------------------------------------------------------
// File: RadosExceptioin.hh
// Author: Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/*******************************************************************************
 * CephVectMap                                                                 *
 * Copyright (C) 2015 CERN/Switzerland                                         *
 *                                                                             *
 * This program is free software: you can redistribute it and/or modify        *
 * it under the terms of the GNU General Public License as published by        *
 * the Free Software Foundation, either version 3 of the License, or           *
 * (at your option) any later version.                                         *
 *                                                                             *
 * This program is distributed in the hope that it will be useful,             *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of              *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                *
 * GNU General Public License for more details.                                *
 *                                                                             *
 * You should have received a copy of the GNU General Public License           *
 * along with this program. If not, see <http://www.gnu.org/licenses/>.        *
 ******************************************************************************/

#ifndef __RADOSEXCEPTION_HH__
#define __RADOSEXCEPTION_HH__

#include <exception>
#include <utility>

namespace rados {

  //----------------------------------------------------------------------------
  //! Exception thrown in object constructor
  //----------------------------------------------------------------------------
  struct RadosContainerException: public std::exception
  {
    //--------------------------------------------------------------------------
    //! Default constructor
    //--------------------------------------------------------------------------
    RadosContainerException():
      std::exception()
    {
    }

    //--------------------------------------------------------------------------
    //! Constructor with parameter
    //!
    //! @param reason Reason for the exception
    //--------------------------------------------------------------------------

    RadosContainerException(std::string&& reason):
      std::exception(),
      mReason(std::move(reason))
    {
    }

    const char* what() const noexcept
    {
      std::string why = "Exception reason: ";
      why += mReason;
      return why.c_str();
    }

    std::string mReason;
  };
}
#endif // __RADOSEXCEPTION_HH__
