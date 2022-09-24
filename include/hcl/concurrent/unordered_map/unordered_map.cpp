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

#ifndef INCLUDE_HCL_UNORDERED_MAP_CONCURRENT_CPP_
#define INCLUDE_HCL_UNORDERED_MAP_CONCURRENT_CPP_

template <typename KeyT, typename ValueT,typename HashFcn,typename EqualFcn>
bool unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::Insert(uint64_t& s,KeyT &key, ValueT &data) 
{
  uint16_t key_int = static_cast<uint16_t>(s);
  AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Insert(remote)", key, data);
  return RPC_CALL_WRAPPER("_Insert", key_int,bool, key, data);
}

template <typename KeyT, typename ValueT,typename HashFcn,typename EqualFcn>
bool unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::Find(uint64_t &s,KeyT &key) 
{
  uint16_t key_int = static_cast<uint16_t>(s);
  AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Find(remote)", key);
  return RPC_CALL_WRAPPER("_Find", key_int,bool, key);
}

template <typename KeyT, typename ValueT,typename HashFcn,typename EqualFcn>
bool unordered_map_concurrent<KeyT, ValueT,HashFcn,EqualFcn>::Erase(uint64_t &s,KeyT &key) 
{
  uint16_t key_int = static_cast<uint16_t>(s);
  AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Erase(remote)", key);
  return RPC_CALL_WRAPPER("_Erase", key_int,bool, key);
}

template <typename KeyT, typename ValueT, typename HashFcn, typename EqualFcn>
ValueT unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Get(uint64_t &s, KeyT &key)
{
   uint16_t key_int = static_cast<uint16_t>(s);
   AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Get(remote)",key);
   return RPC_CALL_WRAPPER("_Get",key_int,ValueT,key);
}

template<typename KeyT, typename ValueT, typename HashFcn, typename EqualFcn>
bool unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Update(uint64_t &s,KeyT &key,ValueT &data)
{
   uint16_t key_int = static_cast<uint16_t>(s);
   AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Update(remote)",key,data);
   return RPC_CALL_WRAPPER("_Update",key_int,bool,key,data);
}

#endif  
