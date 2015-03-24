//------------------------------------------------------------------------------
// File: RadosMapTest.hh
// Author: Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/*******************************************************************************
 * RadosVectMap                                                                *
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

#ifndef __RADOS_MAP_TEST_HH__
#define __RADOS_MAP_TEST_HH__

#include <gtest/gtest.h>
#include <rados/librados.hpp>
#include "src/RadosMap.hh"

#define ENV_CONF_FILE "TEST_CONF_FILE"

//------------------------------------------------------------------------------
//! Function that times the execution of another arbitrary function
//!
//! @param takes any function which returns void and takes no arguments
//!
//! @returns the execution time of the function in nanoseconds
//------------------------------------------------------------------------------
auto timethis(std::function<void()> exec_func)
  -> decltype((std::chrono::steady_clock::now() -
               std::chrono::steady_clock::now()).count()) ;

//------------------------------------------------------------------------------
//! Compute statistics based on the information in the container
//!
//! @param container holding the data
//!
//! @return pair of values representing the mean and standard deviation of the
//!         data points
//------------------------------------------------------------------------------
template <typename Container>
std::pair<double, double> compute_statistics(const Container& c);

//------------------------------------------------------------------------------
//! Class RadosMapTest
//------------------------------------------------------------------------------
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


//------------------------------------------------------------------------------
// Compute statistics based on the information in the container
//------------------------------------------------------------------------------
template <typename Container>
std::pair<double, double> compute_statistics(const Container& c)
{
  auto sum = std::accumulate(std::begin(c), std::end(c), double());
  auto size = std::distance(std::begin(c), std::end(c));
  auto mean = sum / c.size();
  double accum = 0.0;

  for(auto const &elem: c)
    accum += (elem - mean) * (elem - mean);

  return std::make_pair(mean, std::sqrt(accum / ( size - 1)));
}


#endif // __RADOS_MAP_TEST_HH__
