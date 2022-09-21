#ifndef __BLOCK_CHAIN_
#define __BLOCK_CHAIN_

#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <mutex>
#include <cassert>
#include <atomic>
#include <memory>
#include <type_traits>
#include <string>
#include "memory_allocation.h"

#define NOT_IN_TABLE UINT64_MAX
#define EXISTS 1
#define INSERTED 0

namespace hcl
{

template <
	class KeyT,
	class ValueT,
	class HashFcn=std::hash<KeyT>,
	class EqualFcn=std::equal_to<KeyT>>
struct f_node
{
    uint64_t num_nodes;
    std::mutex mutex_t;
    struct node<KeyT,ValueT,HashFcn,EqualFcn> *head;
};

template <
    class KeyT,
    class ValueT, 
    class HashFcn = std::hash<KeyT>,
    class EqualFcn = std::equal_to<KeyT>>
class BlockMap
{

   public :
	typedef struct node<KeyT,ValueT,HashFcn,EqualFcn> node_type;
	typedef struct f_node<KeyT,ValueT,HashFcn,EqualFcn> fnode_type;
   private :
	fnode_type *table;
	uint64_t maxSize;
	std::atomic<uint64_t> allocated;
	std::atomic<uint64_t> removed;
	memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *pl;
	KeyT emptyKey;

	uint64_t KeyToIndex(KeyT &k)
	{
	    uint64_t hashval = HashFcn()(k);
	    return hashval % maxSize;
	}
  public:

	BlockMap(uint64_t n,memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *m,KeyT maxKey) : maxSize(n), pl(m), emptyKey(maxKey)
	{
  	   assert (maxSize > 0);
	   table = (fnode_type *)std::malloc(maxSize*sizeof(fnode_type));
	   assert (table != nullptr);
	   for(size_t i=0;i<maxSize;i++)
	   {
	      table[i].num_nodes = 0;
	      table[i].head = pl->memory_pool_pop();
	      new (&(table[i].head->key)) KeyT(emptyKey);
	      table[i].head->next = nullptr; 
	   }
	   allocated.store(0);
	   removed.store(0);
	   assert(maxSize < UINT64_MAX);
	}

  	~BlockMap()
	{
	    std::free(table);
	}

	uint32_t insert(KeyT &k,ValueT &v)
	{
	    uint64_t pos = KeyToIndex(k);

	    table[pos].mutex_t.lock();

	    node_type *p = table[pos].head;
	    node_type *n = table[pos].head->next;

	    bool found = false;
	    while(n != nullptr)
	    {
		if(EqualFcn()(n->key,k)) found = true;
		if(HashFcn()(n->key)>HashFcn()(k)) 
		{
		   break;
		}
		p = n;
		n = n->next;
	    }

	    uint32_t ret = (found) ? EXISTS : 0;
	    if(!found)
	    {
		allocated.fetch_add(1);
		node_type *new_node=pl->memory_pool_pop();
		new (&(new_node->key)) KeyT(k);
		new (&(new_node->value)) ValueT(v);
		new_node->next = n;
		p->next = new_node;
		table[pos].num_nodes++;
		found = true;
		ret = INSERTED;
	    }

	   table[pos].mutex_t.unlock();
	   return ret;
	}

	uint64_t find(KeyT &k)
	{
	    uint64_t pos = KeyToIndex(k);

	    table[pos].mutex_t.lock();

	    node_type *n = table[pos].head->next;
	    bool found = false;
	    while(n != nullptr)
	    {
		if(EqualFcn()(n->key,k))
		{
		   found = true;
		}
		if(HashFcn()(n->key) > HashFcn()(k)) break;
		n = n->next;
	    }

	    table[pos].mutex_t.unlock();

	    return (found ? pos : NOT_IN_TABLE);
	}

	bool update(KeyT &k,ValueT &v)
	{
	   uint64_t pos = KeyToIndex(k);

	   table[pos].mutex_t.lock();

	   node_type *n = table[pos].head->next;

	   bool found = false;
	   while(n != nullptr)
	   {
		if(EqualFcn()(n->key,k))
		{
		   found = true;
		   n->value = v;
		}
		if(HashFcn()(n->key) > HashFcn()(k)) break;
		n = n->next;
	   }

	   table[pos].mutex_t.unlock();
	   return found;
	}

	bool get(KeyT &k,ValueT *v)
	{
	    bool found = false;

	    uint64_t pos = KeyToIndex(k);

	    table[pos].mutex_t.lock();

	    node_type *n = table[pos].head;

	    while(n != nullptr)
	    {
		if(EqualFcn()(n->key,k))
		{
		   found = true;
		   *v = n->value;
		}
		if(HashFcn()(n->key) > HashFcn()(k)) break;
		n = n->next;
	    }

	   table[pos].mutex_t.unlock();

	   return found;
	}
	
	template<typename... Args>
	bool update_field(KeyT &k,void(*fn)(ValueT *,Args&&... args),Args&&... args_)
	{
	    bool found = false;
	    uint64_t pos = KeyToIndex(k);

	    table[pos].mutex_t.lock();

	    node_type *n = table[pos].head->next;

	    while(n != nullptr)
	    {
		if(EqualFcn()(n->key,k))
		{
		    found = true;
		    fn(&(n->value),std::forward<Args>(args_)...);
		}
		if(HashFcn()(n->key) > HashFcn()(k)) break;
		n = n->next; 
	    }

	    table[pos].mutex_t.unlock();

	    return found;
	}

	bool erase(KeyT &k)
	{
	   uint64_t pos = KeyToIndex(k);

	   table[pos].mutex_t.lock();

	   node_type *p = table[pos].head;
	   node_type *n = table[pos].head->next;

	   bool found = false;

	   while(n != nullptr)
	   {
		if(EqualFcn()(n->key,k)) break;

		if(HashFcn()(n->key) > HashFcn()(k)) break;
		p = n;
		n = n->next;
	  }
         
	  if(n != nullptr)
	  if(EqualFcn()(n->key,k))
	  {
		found = true;
		p->next = n->next;
		pl->memory_pool_push(n);
		table[pos].num_nodes--;
		removed.fetch_add(1);
	  }
	 
	   table[pos].mutex_t.unlock();
	   return found;
	}

	uint64_t allocated_nodes()
	{
		return allocated.load();
	}

	uint64_t removed_nodes()
	{
		return removed.load();
	}

	uint64_t count_block_entries()
	{
	   uint64_t num_entries = 0;
	   for(size_t i=0;i<maxSize;i++)
	   {
		num_entries += table[i].num_nodes;
	   }
	   return num_entries;
	}
	
};

}

#endif
