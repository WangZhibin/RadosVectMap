//------------------------------------------------------------------------------
// File: RadosMapTest.hh
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

#ifndef __RADOSMAPTEST_HH__
#define __RADOSMAPTEST_HH__

#include <gtest/gtest.h>
#include <rados/librados.hpp>
#include "src/RadosMap.hh"

#define ENV_CONF_FILE "TEST_CONF_FILE"


class RadosMapTest: public testing::Test
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RadosMapTest() noexcept(false);
  
  //----------------------------------------------------------------------------
  //! Desstructor
  //----------------------------------------------------------------------------
  virtual ~RadosMapTest();

protected:
  
  void SetUp();

  void TearDown();

  librados::Rados mCluster; ///< rados cluster object
  std::map<std::string, std::string> mConfig; ///< map with the config values
  rados::map<std::string, std::string>* mMapSS; ///< map used for testing

private:

  //----------------------------------------------------------------------------
  //! Read in test configuration specified in the environment variable 
  //! ENV_CONF_FILE
  //----------------------------------------------------------------------------
  void ReadConfiguration();
};

#endif // __RADOSMAPTEST_HH__
