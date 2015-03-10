//------------------------------------------------------------------------------
// File: RadosMapTest.cc
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

#include <fstream>
#include <iostream>
#include <numeric>
#include <cmath>
#include "RadosMapTest.hh"

//------------------------------------------------------------------------------
// Function that times the execution of another arbitrary function
//------------------------------------------------------------------------------
auto timethis(std::function<void()> exec_func)
  -> decltype((std::chrono::steady_clock::now() -
               std::chrono::steady_clock::now()).count())
{
  auto now = std::chrono::steady_clock::now;
  auto start = now();
  exec_func();
  auto end = now();
  return (end - start).count();
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RadosMapTest::RadosMapTest()
{
  ReadConfiguration();
  mCluster.init(mConfig["user"].c_str());

  if (mCluster.conf_read_file(mConfig["ceph_config"].c_str()))
    throw std::invalid_argument("Error while reading configuration file");

  if (mCluster.connect())
    throw std::runtime_error("Cannot connect to cluster");

  auto init_duration = timethis([&] {
      try
      {
        mMapSS = new rados::map<std::string, std::string>(mCluster,
                                                          mConfig["pool"],
                                                          mConfig["obj_name"],
                                                          mConfig["cookie"]);
      }
      catch (std::exception& e)
      {
        std::cerr << e.what() << std::endl;
        exit(-1);
      }
    });

    fprintf(stderr, "Initialization map size=%lu, time=%f microsec\n",
          mMapSS->size(), init_duration / 1000.0);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RadosMapTest::~RadosMapTest()
{
  delete mMapSS;
  mCluster.shutdown();
}

//------------------------------------------------------------------------------
// SetUp
//------------------------------------------------------------------------------
void
RadosMapTest::SetUp() {}

//------------------------------------------------------------------------------
// SetUp
//------------------------------------------------------------------------------
void
RadosMapTest::TearDown() {}

//------------------------------------------------------------------------------
// Read in the configuration
//------------------------------------------------------------------------------
void
RadosMapTest::ReadConfiguration()
{
  std::string config_fn {getenv(ENV_CONF_FILE) ? getenv(ENV_CONF_FILE) : ""};

  if (config_fn.empty())
    throw std::invalid_argument("configuration file not set");

  std::string key, value;
  std::ifstream infile {config_fn};

  while (infile >> key >> value)
  {
    // Lines starting with # are comments
    if (key.find('#') == 0 || key.empty() || value.empty())
    {
      // read the rest of the line
      std::getline(infile, key);
      continue;
    }

    mConfig[key] = value;
    // fprintf(stdout, "%s --> %s\n", key.c_str(), value.c_str());
  }
}
