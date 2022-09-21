#ifndef INCLUDE_HCL_UNORDERED_MAP_H_
#define INCLUDE_HCL_UNORDERED_MAP_H_

#include <hcl/communication/rpc_lib.h>
#include <hcl/communication/rpc_factory.h>
#include <hcl/common/singleton.h>
#include <hcl/common/debug.h>

#include <mpi.h>

#ifdef HCL_ENABLE_RPCLIB
#include <rpc/server.h>
#include <rpc/client.h>
#include <rpc/rpc_error.h>
#endif

#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
#include <thallium.hpp>
#endif

#include <boost/lockfree/queue.hpp>

#include <atomic>
#include <memory>
#include <algorithm>
#include <climits>
#include <float.h>
#include <mutex>
#include <cmath>
#include <iostream>
#include <type_traits>
#include <hcl/common/container.h>
#include "Block_chain_memory.h"

namespace hcl
{

   template<class KeyT,
	    class ValueT,
	    class HashFcn=std::hash<KeyT>,
	    class EqualFcn=std::equal_to<KeyT>>
   class unordered_map_concurrent :public container
{

     public :
	     typedef BlockMap<KeyT,ValueT,HashFcn,EqualFcn> map_type;
	     typedef memory_pool<KeyT,ValueT,HashFcn,EqualFcn> pool_type;

     private:
	     uint64_t totalSize;
	     uint64_t maxSize;
	     uint64_t min_range;
	     uint64_t max_range;
	     uint32_t num_servers;
	     uint32_t server_id;
	     KeyT emptyKey;
	     pool_type *pl;
	     map_type *my_table;

     public:
	     bool isLocal(KeyT &k)
             {
            	uint64_t hashval = HashFcn()(k);
            	uint64_t pos = hashval % totalSize;
            	if(pos >= min_range && pos < max_range) return true;
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
		num_servers = np;
		server_id = rank;
		emptyKey = maxKey;
		my_table = nullptr;
		pl = nullptr;
		assert (totalSize > 0 && totalSize < UINT64_MAX);
           	uint64_t localSize = totalSize/num_servers;
           	uint64_t rem = totalSize%num_servers;
           	if(server_id < rem) maxSize = localSize+1;
           	else maxSize = localSize;
		assert (maxSize > 0 && maxSize < UINT64_MAX);
           	min_range = 0;

		if(server_id < rem) 
		   min_range = server_id*(localSize+1);
		else
		   min_range = rem*(localSize+1)+(server_id-rem)*localSize;

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

	     explicit unordered_map_concurrent(CharStruct name_ = "TEST_UNORDERED_MAP_CONCURRENT", uint16_t port=HCL_CONF->RPC_PORT);
    	     
	     map_type * data()
	     {
    		return my_table;	
	     }

	     void bind_functions() override;

	     void open_shared_memory() override;

	     void construct_shared_memory() override;

		
	     uint32_t LocalInsert(KeyT &k,ValueT &v)
	     {
		     uint32_t ret = my_table->insert(k,v);
		     return ret;
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

	     template<typename... Args>
	     bool LocalUpdateField(KeyT &k,void(*f)(ValueT*,Args&&... args),Args&&...args_)
	     {
		return my_table->update_field(k,f,std::forward<Args>(args_)...);
	     }

	     uint32_t Insert(uint32_t &s, KeyT& k,ValueT& v);
	     bool Find(uint32_t &s,KeyT& k);
	     bool Erase(uint32_t &s, KeyT& k);
	     //ValueT Get(uint32_t &s, KeyT& k);
	     bool Update(uint32_t &s, KeyT& k,ValueT& v);
	     
	     #if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
    		THALLIUM_DEFINE(LocalInsert, (k,v), KeyT& k, ValueT& v)
    		THALLIUM_DEFINE(LocalFind, (k), KeyT& k)
    		THALLIUM_DEFINE(LocalErase, (k), KeyT& k)
		THALLIUM_DEFINE(LocalUpdate, (k,v), KeyT& k, ValueT& v)
	     #endif


};

#include "unordered_map.cpp"

}

#endif
