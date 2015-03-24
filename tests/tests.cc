//------------------------------------------------------------------------------
// File: tests.cc
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

#include <tuple>
#include <chrono>
#include <algorithm>
#include <type_traits>
#include <functional>
#include <gtest/gtest.h>
#include "RadosMapTest.hh"


//------------------------------------------------------------------------------
// Create map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, DISABLED_ListMap)
{
  for (auto const &iter: *mMapSS)
    fprintf(stdout, "key -> %s ..... value -> %s\n", iter.first.c_str(),
            iter.second.c_str());
}

//------------------------------------------------------------------------------
// Check for supported template parameter types
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, DISABLED_CheckTemplateParam)
{
  try
  {
    rados::map<std::string, int> unsupp_map(mCluster, mConfig["pool"],
                                            mConfig["obj_name"], "cookie1", false);
  }
  catch (rados::RadosContainerException& e)
  {
    ASSERT_STREQ("Exception reason: unsupported template parameter type", e.what());
  }

  try
  {
    rados::map<int, std::string> unsupp_map(mCluster, mConfig["pool"],
                                            mConfig["obj_name"], "cookie1", false);
  }
  catch (rados::RadosContainerException& e)
  {
    ASSERT_STREQ("Exception reason: unsupported template parameter type", e.what());
  }

  try
  {
    rados::map<bool, std::string> unsupp_map(mCluster, mConfig["pool"],
                                             mConfig["obj_name"], "cookie1", false);
  }
  catch (rados::RadosContainerException& e)
  {
    ASSERT_STREQ("Exception reason: unsupported template parameter type", e.what());
  }
}

//------------------------------------------------------------------------------
// Insert element in map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, InsertInMap)
{
  int start {330020};
  int num_entries {1};
  int end = start + num_entries;
  std::string key, val;
  decltype(mMapSS->insert(key, val)) ret_pair;
  std::vector<double> tm_insert; // in microseconds

  for (int i = start; i < end; ++i)
  {
    key = "key_"; key += std::to_string(i);
    val = "value_"; val += std::to_string(i);
    auto duration = timethis([&] { ret_pair =  mMapSS->insert(key, val); });
    ASSERT_TRUE(ret_pair.second);
    tm_insert.push_back((double)duration / 1000.0);
  }

  //ASSERT_EQ(num_entries, mMapSS->size());

  // Analyse the insert performance
  auto max_val = std::max_element(std::begin(tm_insert), std::end(tm_insert));
  auto min_val = std::min_element(std::begin(tm_insert), std::end(tm_insert));
  auto info_stat = compute_statistics(tm_insert);
  fprintf(stdout, "Insert num_entries=%i, max=%f, min=%f, mean=%f, "
          "std=%f (microsec)\n", num_entries, *max_val, *min_val,
            info_stat.first, info_stat.second);
}

//------------------------------------------------------------------------------
// Erase elements from map
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, DISABLED_EraseFromMap)
{
  int num_entries {1000};
  std::vector<double> tm_erase;
  auto it = mMapSS->begin();

  while ((it != mMapSS->end()) && (num_entries >= 0))
  {
    num_entries--;
    auto duration = timethis([&] { mMapSS->erase(it++); });
    tm_erase.push_back((double) duration / 1000.0);
  }

  //ASSERT_EQ(0, mMapSS->size());
  auto max_val = std::max_element(std::begin(tm_erase), std::end(tm_erase));
  auto min_val = std::min_element(std::begin(tm_erase), std::end(tm_erase));
  auto info_stat = compute_statistics(tm_erase);
  fprintf(stdout, "Erase num_entries=%i, max=%f, min=%f, mean=%f, "
          "std=%f (microsec)\n", num_entries, *max_val, *min_val,
            info_stat.first, info_stat.second);
}

//------------------------------------------------------------------------------
// Test conversion from different objects to string
//------------------------------------------------------------------------------
TEST_F(RadosMapTest, DISABLED_StringConversions)
{
  std::string str;
  std::tuple<double, float, uint64_t> tuple (0xffffffffffffffff,
                                             0x00000000ffffffff,
                                             0xffffffffffffffff);

  str = mMapSS->ToString(std::get<0>(tuple));
  ASSERT_TRUE("18446744073709551616.000000" == str);
  str = mMapSS->ToString(std::get<1>(tuple));
  ASSERT_TRUE("4294967296.000000" == str);
  str = mMapSS->ToString(std::get<2>(tuple));
  ASSERT_TRUE("18446744073709551615" == str);

  double val_double = mMapSS->FromString<double>("18446744073709551616.000000");
  ASSERT_DOUBLE_EQ((double)(0xffffffffffffffff), val_double);
  float val_float = mMapSS->FromString<float>("4294967296.000000");
  ASSERT_FLOAT_EQ((float)(0x00000000ffffffff), val_float);
  uint64_t val_uint64 = mMapSS->FromString<uint64_t>("18446744073709551615");
  ASSERT_EQ((uint64_t)(0xffffffffffffffff),  val_uint64);
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
