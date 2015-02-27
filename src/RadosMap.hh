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

#include <map>
#include <climits>
#include <string>
#include <sstream>
#include <cstdio>
#include <utility>
#include <cstdint>
#include <cstddef>
#include <typeinfo>
#include <chrono>
#include <thread>
#include <rados/librados.hpp>
#include "RadosException.hh"

#define OBJ_LOCK_NAME "object-lock-name"
#define OBJ_LOCK_COOKIE "object-lock-cookie"
#define OBJ_LOCK_TAG "object-lock-tag"
#define OBJ_LOCK_DESC "object-lock-description"
#define OBJ_LOCK_DURATION 60
#define OBJ_LOCK_FLAG_NORMAL 0x0
#define OBJ_LOCK_FLAG_RENEW  0x1

namespace rados {

  template<typename K, typename V>
  class map
  {
    typedef typename std::map<K, V>::iterator iterator_t;

    public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param rados_cluster Rados cluster obj
    //! @param pool_name name of the pool the map obj will be in
    //! @param name name of the map
    //! @param cookie application identifier
    //! @param pbo persist backend object (delete or not obj. holding the map)
    //! @param ch_log mark every map operation in the object itself as a changelog
    //--------------------------------------------------------------------------
    map(librados::Rados& rados_cluster,
        const std::string& pool_name,
        const std::string& name,
        const std::string& cookie,
        bool pbo = true,
        bool ch_log = false) noexcept(false);


    //--------------------------------------------------------------------------
    //! Copy constructor - disabled
    //--------------------------------------------------------------------------
    map(const map& other) = delete;

    //--------------------------------------------------------------------------
    //! Copy constructor - disabled
    //--------------------------------------------------------------------------
    map& operator=(const map& other) = delete;

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~map();

    //--------------------------------------------------------------------------
    //! Insert new value
    //!
    //! @param key key
    //! @param value value
    //!
    //! @return pair with the iterator pointing to the newly added element and
    //!         status of the insert - true if element inserted, otherwise
    //!         false
    //--------------------------------------------------------------------------
    std::pair<iterator_t, bool>
    insert(K key, V value);

    //--------------------------------------------------------------------------
    //! Erase key from map
    //!
    //! @param key key to be erased from the map
    //--------------------------------------------------------------------------
    void erase(K key);

    //--------------------------------------------------------------------------
    //! Number of entries in map
    //!
    //! @return number of entries in map
    //--------------------------------------------------------------------------
    uint64_t size() const;

    //--------------------------------------------------------------------------
    //! Get iterator to beginning of local map
    //--------------------------------------------------------------------------
    iterator_t begin()
    {
       return mMap.begin();
    }

    //--------------------------------------------------------------------------
    //! Get iterator to end of local map
    //--------------------------------------------------------------------------
    iterator_t end()
    {
       return mMap.end();
    }

    //--------------------------------------------------------------------------
    //! Get string representation of the object
    //!
    //! @param value obj to be converted
    //!
    //! @return string representation of the object
    //--------------------------------------------------------------------------
    template <typename W>
    std::string ToString(W& value) const;

    //--------------------------------------------------------------------------
    //! Convert string to type object
    //!
    //! @param value string object to be converted
    //!
    //! @return object obtained from converting the string
    //--------------------------------------------------------------------------
    template <typename W>
    W FromString(const std::string& sval) const;


  private:

    std::map<K, V> mMap; ///< local representation of the map
    std::string mObjId;  ///< object id that holds the map information
    librados::IoCtx mIoCtx; ///< io context
    bool mUseChangeLog; ///< use a changelog for map updates
    bool mPbo; /// < persist object backend

    //--------------------------------------------------------------------------
    //! Helper function to convert string to non-string object.
    //!
    //! @param value string object to be converted
    //!
    //! @return object obtained from converting the string to type W
    //--------------------------------------------------------------------------
    template <typename W>
    bool HelperFromString(const std::string& sval, W& ret) const;

    //--------------------------------------------------------------------------
    //! Helper function to convert string to a string.
    //! Migth sound confugins but it's done like this so that the code compiles
    //! when the return value is either a string or a numeric type.
    //!
    //! @param value string object to be converted
    //!
    //! @return object obtained from converting the string to type W
    //--------------------------------------------------------------------------
    bool HelperFromString(const std::string& sval, std::string& ret) const;

    //--------------------------------------------------------------------------
    //! Helper function to get string representation of an object which is not
    //! a string.
    //!
    //! @param value obj to be converted
    //!
    //! @return string representation of the object
    //--------------------------------------------------------------------------
    template <typename W>
    bool HelperToString(W& value, std::string& ret) const;

    //--------------------------------------------------------------------------
    //! Helper function to get string representation of a string. :)
    //! Might sound confusing but it's done like  this so that the code compiles
    //! when the return value is either a string or a numeric type.
    //!
    //! @param value obj to be converted
    //!
    //! @return string representation of the object
    //--------------------------------------------------------------------------
    bool HelperToString(std::string& value, std::string& ret) const;

    //--------------------------------------------------------------------------
    //! Get omap
    //!
    //! @return true if successful, otherwise false
    //--------------------------------------------------------------------------
    bool GetOmap();

    //--------------------------------------------------------------------------
    //! Exclusive lock on the CEPH object
    //--------------------------------------------------------------------------
    void ExclusiveLock();

    //--------------------------------------------------------------------------
    //! Shared lock on the CEPH object
    //--------------------------------------------------------------------------
    void SharedLock();

    //--------------------------------------------------------------------------
    //! Unlock CEPH object
    //--------------------------------------------------------------------------
    void Unlock();
  };

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  map<K, V>::map(librados::Rados& rados_cluster,
                 const std::string& pool_name,
                 const std::string& name,
                 const std::string& cookie,
                 bool pbo,
                 bool ch_log) noexcept(false):
    mUseChangeLog(ch_log),
    mPbo(pbo)
  {
    int ret;
    std::ostringstream oss;
    oss << "/map/" << name << "/" << cookie;
    mObjId = oss.str();
    ret = rados_cluster.ioctx_create(pool_name.c_str(), mIoCtx);

    if (ret)
      throw RadosContainerException("unable to create ioctx for pool");

    // Check if object exists, if not create it
    uint64_t psize;
    time_t pmtime;

    if (mIoCtx.stat(mObjId, &psize, &pmtime))
    {
      if (mIoCtx.create(mObjId, true))
        throw RadosContainerException("unable to create obj.");
    }
    else
    {
      if (!GetOmap())
        throw RadosContainerException("unable to get omap");
    }
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  map<K, V>::~map()
  {
    if (!mPbo && mIoCtx.remove(mObjId))
      throw RadosContainerException("unable to remove obj.");
  }

  //----------------------------------------------------------------------------
  // Get omap
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  bool map<K, V>::GetOmap()
  {
    std::map<std::string, librados::bufferlist> omap;
    ExclusiveLock();
    int ret = mIoCtx.omap_get_vals(mObjId, "", "", UINT_MAX, &omap);
    Unlock();

    if (ret)
    {
      fprintf(stderr, "error: failed to get omap for obj=%s\n", mObjId.c_str());
      return false;
    }

    K key;
    V val;

    // Add the entries to the local map
    for (auto&& it: omap)
    {
      key = FromString<decltype(key)>(it.first);
      val = FromString<decltype(val)>(std::string(it.second.c_str(),
                                                  it.second.length()));
      mMap.insert(std::make_pair(key, val));
    }

    return true;
  }

  //----------------------------------------------------------------------------
  // Exclusive lock on the CEPH object
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  void map<K, V>::ExclusiveLock()
  {
    timeval* duration = nullptr;

    while (mIoCtx.lock_exclusive(mObjId, OBJ_LOCK_NAME, OBJ_LOCK_COOKIE,
                                 OBJ_LOCK_DESC, duration, OBJ_LOCK_FLAG_NORMAL))
    {
      // TODO: maybe wait before retrying
    }
  }

  //----------------------------------------------------------------------------
  // Exclusive lock on the CEPH object
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  void map<K, V>::SharedLock()
  {
    timeval* duration = static_cast<struct timeval*>(0);

    while (mIoCtx.lock_shared(mObjId, OBJ_LOCK_NAME, OBJ_LOCK_COOKIE,
                              OBJ_LOCK_TAG, OBJ_LOCK_DESC, duration,
                              OBJ_LOCK_FLAG_NORMAL))
    {
      // TODO: maybe wait before retrying
    }
  }

  //----------------------------------------------------------------------------
  // Exclusive lock on the CEPH object
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  void map<K, V>::Unlock()
  {
    mIoCtx.unlock(mObjId, OBJ_LOCK_NAME, OBJ_LOCK_COOKIE);
  }

  //----------------------------------------------------------------------------
  // Insert new value
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  std::pair<typename std::map<K, V>::iterator, bool>
  map<K, V>::insert(K key, V value)
  {
    std::pair<K, V> elem(key, value);
    std::pair<map::iterator_t, bool> result = mMap.insert(elem);

    if (mUseChangeLog)
    {
      // TODO: add logic for the changelog
    }
    else
    {
      // Insert directly in the OMAP
      librados::ObjectWriteOperation omap_write_op;
      std::map<std::string, librados::bufferlist> add_map;
      librados::bufferlist buff_val;
      std::string skey = ToString(key);
      std::string sval = ToString(value);
      buff_val.append(sval);
      add_map.insert(std::make_pair(skey, buff_val));
      ExclusiveLock();
      mIoCtx.omap_set(mObjId, add_map);
      Unlock();
    }

    return result;
  }

  //----------------------------------------------------------------------------
  // Erase key from map
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  void
  map<K, V>::erase(K key)
  {
    // Local remove
    mMap.erase(key);

    // Remote remove
    std::set<std::string> to_rm;
    to_rm.insert(key);
    ExclusiveLock();
    mIoCtx.omap_rm_keys(mObjId, to_rm);
    Unlock();
  }

  //----------------------------------------------------------------------------
  // Count the number of entries
  //----------------------------------------------------------------------------
  template <typename K, typename V>
  uint64_t map<K, V>::size() const
  {
    if (mUseChangeLog)
    {

    }

    return mMap.size();
  }

  //--------------------------------------------------------------------------
  // Get string representation of the object
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  template <typename W>
  std::string map<K, V>::ToString(W& value) const
  {
    std::string ret;

    if (!HelperToString(value, ret))
      throw RadosContainerException("unable to convert to string");

    return ret;
  }

  //--------------------------------------------------------------------------
  // Helper function to get string representation of an object which is not
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  template <typename W>
  bool map<K, V>::HelperToString(W& value, std::string& ret) const
  {
    ret = std::to_string(value);
    return true;
  }

  //--------------------------------------------------------------------------
  // Helper function to get string representation of a string. :)
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  bool map<K, V>::HelperToString(std::string& value, std::string& ret) const
  {
    ret = value;
    return true;
  }

  //--------------------------------------------------------------------------
  // Convert string to required representation
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  template <typename W>
  W map<K, V>::FromString(const std::string& sval) const
  {
    W ret;

    if (!HelperFromString(sval, ret))
      throw RadosContainerException("unable to convert from string");

    return ret;
  }

  //--------------------------------------------------------------------------
  // Helper function to convert string to number representation
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  template <typename W>
  bool map<K, V>::HelperFromString(const std::string& sval, W& ret) const
  {
    if (std::is_same<W, double>::value)
    {
      ret = std::stod(sval, nullptr);
      return true;
    }
    else if (std::is_same<W, float>::value)
    {
      ret = std::stof(sval, nullptr);
      return true;
    }
    else if (std::is_same<W, unsigned long long>::value ||
             std::is_same<W, uint64_t>::value)
    {
      ret = std::stoull(sval, nullptr);
      return true;
    }

    return false;
  }

  //--------------------------------------------------------------------------
  // Helper function to convert string to string representation
  //--------------------------------------------------------------------------
  template <typename K, typename V>
  bool
  map<K, V>::HelperFromString(const std::string& sval, std::string& ret) const
  {
    ret = sval;
    return true;
  }

}

#endif //__RADOSMAP_HH__
