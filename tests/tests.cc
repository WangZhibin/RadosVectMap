//------------------------------------------------------------------------------
// File: tests.cc
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

#include <tuple>
#include <gtest/gtest.h>
#include "RadosMapTest.hh"
#include "src/RadosMap.hh"

//------------------------------------------------------------------------------
// Create map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, DISABLED_ListMap)
{
  for (auto&& iter: *mMapSS)
    fprintf(stdout, "key -> %s ..... value -> %s\n", iter.first.c_str(),
            iter.second.c_str());
}

//------------------------------------------------------------------------------
// Insert element in map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, InsertInMap)
{
  ASSERT_EQ(0, mMapSS->size());
  mMapSS->insert("layout", "replica");
  mMapSS->insert("stripes", "3");
  ASSERT_EQ(2, mMapSS->size());
}

//------------------------------------------------------------------------------
// Erase elements from map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, EraseFromMap)
{
  mMapSS->erase("layout");
  mMapSS->erase("stripes");
  ASSERT_EQ(0, mMapSS->size());
}

//------------------------------------------------------------------------------
// Test conversion from different objects to string
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, StringConversions)
{
  std::string str;
  std::tuple<double, float, uint64_t> tuple (0xffffffffffffffff,
                                             0x00000000ffffffff,
                                             0xffffffffffffffff);

  str = mMapSS->ToString(std::get<0>(tuple));
  ASSERT_EQ("18446744073709551616.000000", str);
  str = mMapSS->ToString(std::get<1>(tuple));
  ASSERT_EQ("4294967296.000000", str);
  str = mMapSS->ToString(std::get<2>(tuple));
  ASSERT_EQ("18446744073709551615", str);

  double val_double = mMapSS->FromString<double>("18446744073709551616.000000");
  ASSERT_EQ((double)(0xffffffffffffffff), val_double);
  float val_float = mMapSS->FromString<float>("4294967296.000000");
  ASSERT_TRUE((float)(0x00000000ffffffff) == val_float);
  uint64_t val_uint64 = mMapSS->FromString<uint64_t>("18446744073709551615");
  ASSERT_TRUE((uint64_t)(0xffffffffffffffff) == val_uint64);
}

//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
GTEST_API_ int
main(int argc, char** argv)
{
  std::string arg;

  if (argc != 2 )
    goto run_tests_usage;

  arg = argv[1];

  if (arg.find("--conf") == 0)
    setenv(ENV_CONF_FILE, arg.substr(arg.find('=') + 1).c_str(), 1);
  else
    goto run_tests_usage;

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

 run_tests_usage:
  std::cout << "run_tests --conf=<test_config_file>" << std::endl;
  return 1;
}
