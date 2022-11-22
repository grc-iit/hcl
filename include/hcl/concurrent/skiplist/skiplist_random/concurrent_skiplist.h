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

#ifndef INCLUDE_HCL_CONCURRENT_SKIPLIST_H_
#define INCLUDE_HCL_CONCURRENT_SKIPLIST_H_

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
#include "Skiplist.h"

namespace hcl {

template <class T,
	 class HashFcn = std::hash<T>,
	 class Comp = std::less<T>,
	 class NodeAlloc = std::allocator<char>,
	 int MAX_HEIGHT = 24>
class concurrent_skiplist : public container
{

  typedef ConcurrentSkipList<T,Comp,NodeAlloc,MAX_HEIGHT> SkipListType;
  typedef typename ConcurrentSkipList<T,Comp,NodeAlloc,MAX_HEIGHT>::Accessor SkipListAccessor;

  private:
        uint64_t totalSize;
	uint32_t nservers;
	uint32_t serverid;
	uint64_t nbits;
	SkipListType *s;
	SkipListAccessor *a;

	uint64_t power_of_two(int n)
	{
	    int c = 1;
	    while(c < n)
	    {
		c = 2*c;
	    }
	    return c;
	}
  public:

	uint64_t serverLocation(T &k)
	{
	    uint64_t id = -1;
	    uint64_t hashval = HashFcn()(k);
	    uint64_t mask = UINT64_MAX;
	    mask = mask << (64-nbits);  
	    id = hashval & mask;
	    id = id >> (64-nbits);
	    if(id >= nservers) id = nservers-1;
	    return id;
	}

	bool isLocal(T &k)
	{
	   if(serverLocation(k)==serverid) return true;
	   else return false;
	}

	void initialize_sets(uint32_t np,uint32_t rank)
	{
	    nservers = np;
	    serverid = rank;
	    uint64_t nservers_2 = power_of_two(nservers);
	    nbits = log2(nservers_2);

	    s = nullptr; a = nullptr;
	    if(is_server)
	    {
		s = new SkipListType(2);
		a = new SkipListAccessor(s);
	    }
	}
	~concurrent_skiplist()
	{
	   if(s != nullptr) delete s;
	   if(a != nullptr) delete a;
	   this->container::~container();
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
            std::function<bool(T &)> insertFunc(
            std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::LocalInsert, this,std::placeholders::_1));
            std::function<bool(T &)> findFunc(
            std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::LocalFind, this,std::placeholders::_1));
            std::function<bool(T &)> eraseFunc(
            std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::LocalErase, this,std::placeholders::_1));
            
	    rpc->bind(func_prefix + "_Insert", insertFunc);
            rpc->bind(func_prefix + "_Find", findFunc);
            rpc->bind(func_prefix + "_Erase", eraseFunc);
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

           std::function<void(const tl::request &, T &)> insertFunc(
           std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::ThalliumLocalInsert,
           this, std::placeholders::_1, std::placeholders::_2));
            std::function<void(const tl::request &, T &)> findFunc(
            std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::ThalliumLocalFind,
                      this, std::placeholders::_1, std::placeholders::_2));
            std::function<void(const tl::request &,T &)> eraseFunc(
            std::bind(&concurrent_skiplist<T,HashFcn,Comp,NodeAlloc,MAX_HEIGHT>::ThalliumLocalErase,
                      this, std::placeholders::_1, std::placeholders::_2));
            
	    rpc->bind(func_prefix + "_Insert", insertFunc);
            rpc->bind(func_prefix + "_Find", findFunc);
            rpc->bind(func_prefix + "_Erase", eraseFunc);
            break;
        }
#endif
        }
       }
	
	explicit concurrent_skiplist(CharStruct name_ = "TEST_CONCURRENT_SKIPLIST",uint16_t port = HCL_CONF->RPC_PORT)
      : container(name_, port)
      {
    	a = nullptr;
    	s = nullptr;
    	AutoTrace trace = AutoTrace("hcl::concurrent_skiplist");
    	if(is_server)
    	{
      	  construct_shared_memory();
      	  bind_functions();
        }
    	else if (!is_server && server_on_node)
        {
      	  open_shared_memory();
        }
     }

     bool LocalInsert(T &k)
     {
	  return a->add(k);
     }
     bool LocalFind(T &k)
     {
	return a->contains(k);
     }
     bool LocalErase(T &k)
     {
	return a->remove(k);
     }


#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
    THALLIUM_DEFINE(LocalInsert, (k), T& k)
    THALLIUM_DEFINE(LocalFind, (k), T& k)
    THALLIUM_DEFINE(LocalErase, (k), T& k)
#endif

   bool Insert(uint64_t &s, T& k);
   bool Find(uint64_t &s,T& k);
   bool Erase(uint64_t &s, T& k);

};
#include "concurrent_skiplist.cpp"
}

#endif

