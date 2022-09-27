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

#ifndef INCLUDE_HCL_QUEUE_CONCURRENT_CPP_
#define INCLUDE_HCL_QUEUE_CONCURRENT_CPP_

template <typename ValueT>
bool queue_concurrent<ValueT>::Push(uint64_t& s, ValueT &data) 
{
  uint16_t key_int = static_cast<uint16_t>(s);
  AutoTrace trace = AutoTrace("hcl::queue_concurrent::Push(remote)", data);
  return RPC_CALL_WRAPPER("_Push", key_int,bool,data);
}

template <typename ValueT>
std::pair<bool,ValueT> queue_concurrent<ValueT>::Pop(uint64_t &s) 
{
  uint16_t key_int = static_cast<uint16_t>(s);
  AutoTrace trace = AutoTrace("hcl::queue_concurrent::Pop(remote)");
  typedef std::pair<bool,ValueT> ret_type;
  return RPC_CALL_WRAPPER1("_Pop", key_int,ret_type);
}

#endif  
