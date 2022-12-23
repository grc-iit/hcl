/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef INCLUDE_HCL_UNORDERED_MAP_CONCURRENT_H_
#define INCLUDE_HCL_UNORDERED_MAP_CONCURRENT_H_

/**
 * Include Headers
 */

#include <hcl/common/debug.h>
#include <hcl/common/singleton.h>
#include <hcl/communication/rpc_factory.h>
#include <hcl/communication/rpc_lib.h>
/** MPI Headers**/
#include <mpi.h>
/** RPC Lib Headers**/
#ifdef HCL_ENABLE_RPCLIB

#include <rpc/client.h>
#include <rpc/rpc_error.h>
#include <rpc/server.h>

#endif
/** Thallium Headers **/
#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
#include <thallium.hpp>
#endif

/** Boost Headers **/
#include <boost/algorithm/string.hpp>
/** Standard C++ Headers**/
#include <hcl/common/container.h>

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <float.h>
#include "../../base/containers/concurrent_unordered_map/block_map.h"

namespace hcl {

template <class KeyT, 
	  class ValueT,
	  class HashFcn=std::hash<KeyT>,
	  class EqualFcn=std::equal_to<KeyT>>
class unordered_map_concurrent : public container 
{
  public :
      typedef BlockMap<KeyT,ValueT,HashFcn,EqualFcn> map_type;
      typedef memory_pool<KeyT,ValueT,HashFcn,EqualFcn> pool_type;

 private:
	uint64_t totalSize;
        uint64_t maxSize;
	uint64_t min_range;
	uint64_t max_range;
        uint32_t nservers;
        uint32_t serverid;
        KeyT emptyKey;
        pool_type *pl;
        map_type *my_table;



 public:
   bool isLocal(KeyT &k)
   {
       uint64_t hashval = HashFcn()(k);
       uint64_t pos = hashval % totalSize;
       if(is_server && pos >= min_range && pos < max_range) return true;
       else return false;
   }

   uint64_t serverLocation(KeyT &k)
   {
      uint64_t localSize = totalSize/num_servers;
      uint64_t rem = totalSize%num_servers;
      uint64_t hashval = HashFcn()(k);
      uint64_t v = hashval % totalSize;
      uint64_t offset = rem*(localSize+1);
      uint64_t id = -1;
      if(v >= 0 && v < totalSize)
      {
         if(v < offset)
           id = v/(localSize+1);
         else id = rem+((v-offset)/localSize);
      }

      return id;
   }

    void initialize_tables(uint64_t n,uint32_t np,uint32_t rank,KeyT maxKey)
    {
        totalSize = n;
        nservers = np;
        serverid = rank;
        emptyKey = maxKey;
        my_table = nullptr;
        pl = nullptr;
        assert (totalSize > 0 && totalSize < UINT64_MAX);
        uint64_t localSize = totalSize/nservers;
        uint64_t rem = totalSize%nservers;
        if(serverid < rem) maxSize = localSize+1;
        else maxSize = localSize;
        assert (maxSize > 0 && maxSize < UINT64_MAX);
        min_range = 0;

        if(serverid < rem)
           min_range = serverid*(localSize+1);
        else
           min_range = rem*(localSize+1)+(serverid-rem)*localSize;

        max_range = min_range + maxSize;

        if(is_server)
        {
          pl = new pool_type(100);
          my_table = new map_type(maxSize,pl,emptyKey);
        }

   }

  ~unordered_map_concurrent() 
  {
    if(my_table != nullptr) delete my_table;
    if(pl != nullptr) delete pl;  
  }

  void construct_shared_memory() override 
  {

  }
  void open_shared_memory() override 
  {

  }
  void bind_functions() override 
  {
    switch (HCL_CONF->RPC_IMPLEMENTATION) {
#ifdef HCL_ENABLE_RPCLIB
      case RPCLIB: {
        std::function<bool(KeyT &, ValueT &)> insertFunc(
            std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::LocalInsert, this,
                      std::placeholders::_1, std::placeholders::_2));
        std::function<bool(KeyT &)> findFunc(
            std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::LocalFind, this,
                      std::placeholders::_1));
        std::function<bool(KeyT &)> eraseFunc(
            std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::LocalErase, this,
                      std::placeholders::_1));
	std::function<ValueT(KeyT&)> getFunc(
	   std::bind(&unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalGetValue, this,
		   std::placeholders::_1));
	std::function<bool(KeyT&,ValueT&)>updateFunc(
	   std::bind(&unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalUpdate, this,
		 std::placeholders::_1,std::placeholders::_2));

        rpc->bind(func_prefix + "_Insert", insertFunc);
        rpc->bind(func_prefix + "_Find", findFunc);
        rpc->bind(func_prefix + "_Erase", eraseFunc);
	rpc->bind(func_prefix + "_Get", getFunc);
	rpc->bind(func_prefix + "_Update", updateFunc);
        break;
      }
#endif
#ifdef HCL_ENABLE_THALLIUM_TCP
      case THALLIUM_TCP:
#endif
#ifdef HCL_ENABLE_THALLIUM_ROCE
      case THALLIUM_ROCE:
#endif
#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
      {

        std::function<void(const tl::request &, KeyT &, ValueT &)> insertFunc(
	   std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::ThalliumLocalInsert,
           this, std::placeholders::_1, std::placeholders::_2,std::placeholders::_3));
        std::function<void(const tl::request &, KeyT &)> findFunc(
            std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::ThalliumLocalFind,
                      this, std::placeholders::_1, std::placeholders::_2));
        std::function<void(const tl::request &, KeyT &)> eraseFunc(
            std::bind(&unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::ThalliumLocalErase,
                      this, std::placeholders::_1, std::placeholders::_2));
	std::function<void(const tl::request &, KeyT &)> getFunc(
	    std::bind(&unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalGetValue,
		      this, std::placeholders::_1, std::placeholders::_2));
	std::function<void(const tl::request &, KeyT &, ValueT &)> updateFunc(
	   std::bind(&unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalUpdate,
		     this,std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

        rpc->bind(func_prefix + "_Insert", insertFunc);
        rpc->bind(func_prefix + "_Find", findFunc);
        rpc->bind(func_prefix + "_Erase", eraseFunc);
	rpc->bind(func_prefix + "_Get", getFunc);
	rpc->bind(func_prefix + "_Update", updateFunc);
        break;
      }
#endif
    }
  }

  explicit unordered_map_concurrent(CharStruct name_ = "TEST_UNORDERED_MAP_CONCURRENT",uint16_t port = HCL_CONF->RPC_PORT)
      : container(name_, port)
  {
    my_table = nullptr;
    pl = nullptr;
    AutoTrace trace = AutoTrace("hcl::map");
    if (is_server) {
      bind_functions();
    } else if (!is_server && server_on_node) {
    }
  }

  map_type *data() 
  {
     return my_table;
  }

  bool LocalInsert(KeyT &k,ValueT &v)
  {
   uint32_t r = my_table->insert(k,v);
   if(r != NOT_IN_TABLE) return true;
   else return false;
  }
  bool LocalFind(KeyT &k)
  {
    if(my_table->find(k) != NOT_IN_TABLE) return true;
    else return false;
  }
  bool LocalErase(KeyT &k)
  {
     return my_table->erase(k);
  }
  bool LocalUpdate(KeyT &k,ValueT &v)
  {
       return my_table->update(k,v);
  }
  bool LocalGet(KeyT &k,ValueT* v)
  {
       return my_table->get(k,v);
  }
  ValueT LocalGetValue(KeyT &k)
  {
	ValueT v;
	new (&v) ValueT();
	bool b = LocalGet(k,&v);
	return v;
  }

  template<typename... Args>
  bool LocalUpdateField(KeyT &k,void(*f)(ValueT*,Args&&... args),Args&&...args_)
  {
     return my_table->update_field(k,f,std::forward<Args>(args_)...);
  }

  uint64_t allocated()
  {
     return my_table->allocated_nodes();
  }

  uint64_t removed()
  {
     return my_table->removed_nodes();
  }

#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
  THALLIUM_DEFINE(LocalInsert, (k,v), KeyT& k, ValueT& v)
  THALLIUM_DEFINE(LocalFind, (k), KeyT& k)
  THALLIUM_DEFINE(LocalErase, (k), KeyT& k)
  THALLIUM_DEFINE(LocalGetValue, (k), KeyT & k)
  THALLIUM_DEFINE(LocalUpdate, (k,v), KeyT& k, ValueT& v)
#endif

   bool Insert(uint64_t &s, KeyT& k,ValueT& v);
   bool Find(uint64_t &s,KeyT& k);
   bool Erase(uint64_t &s, KeyT& k);
   ValueT Get(uint64_t &s, KeyT& k);
   bool Update(uint64_t &s, KeyT& k,ValueT& v);


};

#include "unordered_map.cpp"

}  // namespace hcl

#endif  
