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

#ifndef INCLUDE_HCL_QUEUE_CONCURRENT_H_
#define INCLUDE_HCL_QUEUE_CONCURRENT_H_

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
#include <boost/lockfree/queue.hpp>
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

namespace hcl {

template <class ValueT>
class queue_concurrent : public container 
{

public:
	typedef boost::lockfree::queue<ValueT> 	queue_type;
private:
	queue_type *queue;


 public:

  ~queue_concurrent() 
  {
    if(queue != nullptr) delete queue;
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
        std::function<bool(ValueT &)> pushFunc(
            std::bind(&queue_concurrent<ValueT>::LocalPush, this,
                      std::placeholders::_1));
        std::function<std::pair<bool,ValueT>(void)> popFunc(
            std::bind(&queue_concurrent<ValueT>::LocalPop, this));

        rpc->bind(func_prefix + "_Push", pushFunc);
        rpc->bind(func_prefix + "_Pop", popFunc);
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

        std::function<void(const tl::request &, ValueT &)> pushFunc(
	   std::bind(&queue_concurrent<ValueT>::ThalliumLocalPush,
           this, std::placeholders::_1,std::placeholders::_2));
        std::function<void(const tl::request &)> popFunc(
            std::bind(&queue_concurrent<ValueT>::ThalliumLocalPop,
                      this, std::placeholders::_1));

        rpc->bind(func_prefix + "_Push", pushFunc);
        rpc->bind(func_prefix + "_Pop", popFunc);
        break;
      }
#endif
    }
  }

  explicit queue_concurrent(CharStruct name_ = "TEST_QUEUE_CONCURRENT",uint16_t port = HCL_CONF->RPC_PORT)
      : container(name_, port)
  {
    queue = nullptr;
    AutoTrace trace = AutoTrace("hcl::queue_concurrent");
    if (is_server) 
    {
      queue = new queue_type (1024);
      bind_functions();
    } 
    else if (!is_server && server_on_node) 
    {
    }
  }

  queue_type *data() 
  {
     return queue;
  }

  bool LocalPush(ValueT &v)
  {
     return queue->push(v);
  }
  std::pair<bool,ValueT> LocalPop()
  {
      ValueT v;
      bool b = queue->pop(v);
      if(b) return std::pair<bool,ValueT> (true,v);
      else return std::pair<bool,ValueT> (false,ValueT());
  }

#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
  THALLIUM_DEFINE(LocalPush, (v), ValueT& v)
  THALLIUM_DEFINE1(LocalPop)
#endif

   bool Push(uint64_t &s, ValueT& v);
   std::pair<bool,ValueT> Pop(uint64_t &s);

};

#include "queue.cpp"

}  // namespace hcl

#endif  
