//------------------------------------------------------------------------------
// File: RadosMap.hh
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

#ifndef __RADOSMAP_HH__
#define __RADOSMAP_HH__

#include <rados/librados.hpp>
#include <map>
#include <sstream>
#include <cstdio>
#include "RadosException.hh"

namespace rados {

  template<class K, class V>
  class map
  {
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param rados_cluster Rados cluster obj
    //! @param pool_name name of the pool the map obj will be in
    //! @param name name of the map
    //! @param app_id application identifier
    //!
    //--------------------------------------------------------------------------
    map(librados::Rados& rados_cluster,
        const std::string& pool_name,
        const std::string& name,
        const std::string& app_id) noexcept(false)
    {
      int ret;
      std::ostringstream oss;
      oss << "/map/" << name << "/" << app_id;
      mObjId = oss.str();
      ret = rados_cluster.ioctx_create(pool_name.c_str(), mIoCtx);

      if (ret)
      {
        fprintf(stderr, "error: unable to create pool=%s", pool_name.c_str());
        throw ConstructorException("unable to create pool");
      }

      if (!GetOmap())
      {
        fprintf(stderr, "error: unable to get omap");
        throw ConstructorException("unable to get omap");
      }
    }

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~map();

  private:
    std::map<K, V> mMap; ///< local representation of the map
    std::string mObjId;  ///< object id that holds the map information
    librados::IoCtx mIoCtx; ///< io context

    //--------------------------------------------------------------------------
    //! Get omap
    //!
    //! @return true if successful, otherwise false
    //--------------------------------------------------------------------------
    template<K, V>
    bool GetOmap()
    {
      librados::ObjectReadOperation omap_read_op;
      std::map<std::string, librados::bufferlist> omap;
      int ret = mIoCtx.omap_get_vals(mObjId, "", "", UINT_MAX, &omap);

      if (ret)
      {
        fprintf(stderr, "error: failed to get omap for obj=%s", mObjId.c_str());
        return false;
      }

      fprintf(stdout, "omap contents:\n");

      for (auto iter: omap)
       {
         fprintf(stdout, "key=%s value=%s\n", iter.first, iter.second);
         // TODO: to the necessary conversion
         mMap[iter.first] = std::string(iter.second.c_str(), iter.second.length());
       }
    }
  };
}
#endif //__RADOSMAP_HH__
