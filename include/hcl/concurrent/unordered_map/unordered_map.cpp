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


template<class KeyT,
       	 class ValueT,
	 class HashFcn, 
	 class EqualFcn>
unordered_map_concurrent<KeyT,ValueT, HashFcn,EqualFcn>::unordered_map_concurrent(CharStruct name_, uint16_t port)
	: container(name,port)
{
    // init my_server, num_servers, server_on_node, processor_name from RPC
    AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent");
    if (is_server) 
    {
        bind_functions();

    }else if (!is_server && server_on_node) 
    {
	
    }
}



template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
uint32_t unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Insert(uint32_t &server,KeyT &k,ValueT &v) 
{
    AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Insert(remote)", k,v);
    return RPC_CALL_WRAPPER("_Insert",server,uint32_t,k,v);
}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
bool unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Find(uint32_t &server,KeyT &k)
{
    AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Find(remote)",k);
    return RPC_CALL_WRAPPER("_Find",server,bool,k);
}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
bool unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Erase(uint32_t &server,KeyT &k)
{
   AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Erase(remote)",k);
   return RPC_CALL_WRAPPER("_Erase",server,bool,k);
}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
ValueT unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Get(uint32_t &server,KeyT &k)
{
   AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Get(remote)",k);
   return RPC_CALL_WRAPPER("_Get",server,ValueT,k);
}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
bool unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::Update(uint32_t &server,KeyT& k,ValueT &v)
{
    AutoTrace trace = AutoTrace("hcl::unordered_map_concurrent::Update(remote)",k);
    return RPC_CALL_WRAPPER("_Update",server,bool,k,v);
}	

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
void unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::open_shared_memory()
{

}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
void unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::construct_shared_memory()
{

}

template<class KeyT,
	 class ValueT,
	 class HashFcn,
	 class EqualFcn>
void unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::bind_functions() 
{

switch (HCL_CONF->RPC_IMPLEMENTATION) 
{
#ifdef HCL_ENABLE_RPCLIB
        case RPCLIB: 
	{
            std::function<uint32_t(KeyT&,ValueT&)> insertFunc(
                          std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalInsert, this,
                              std::placeholders::_1));
            std::function<bool(KeyT&)> findFunc(
			  std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalFind, this));
            std::function<bool(KeyT&)> eraseFunc(
		     std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalErase, this));
	    std::function<bool(KeyT&,ValueT&)>updateFunc(
		     std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalUpdate, this));
	    std::function<ValueT(KeyT&)>getFunc(
		     std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::LocalGetValue,this));

            rpc->bind(func_prefix+"_Insert", insertFunc);
            rpc->bind(func_prefix+"_Find", findFunc);
            rpc->bind(func_prefix+"_Erase", eraseFunc);
	    rpc->bind(func_prefix+"_Update",updateFunc);
	    rpc->bind(func_prefix+"_Get",getFunc);
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
                    std::function<void(const tl::request &,KeyT&,ValueT&)> insertFunc(
                        std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalInsert, this,
                                  std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
                    std::function<void(const tl::request &,KeyT&)> findFunc(
		        std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalFind, this, std::placeholders::_1,std::placeholders::_2));
                    std::function<void(const tl::request &,KeyT&)> eraseFunc(
		        std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalErase, this, std::placeholders::_1,std::placeholders::_2));
		    std::function<void(const tl::request &,KeyT&,ValueT&)> updateFunc(
			std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalUpdate, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

		    std::function<void(const tl::request &,KeyT&)> getFunc(std::bind(&hcl::unordered_map_concurrent<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalGetValue,this,std::placeholders::_1,std::placeholders::_2));

                    rpc->bind(func_prefix+"_Insert",insertFunc);
                    rpc->bind(func_prefix+"_Find", findFunc);
                    rpc->bind(func_prefix+"_Erase", eraseFunc);
		    rpc->bind(func_prefix+"_Update",updateFunc);
		    rpc->bind(func_prefix+"_Get",getFunc);
                    break;
                }
#endif
    }
}
