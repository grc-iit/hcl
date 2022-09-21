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
#include "node.h"
#include "memory_allocation.h"

#define MAX_COLLISIONS 10
#define NOT_IN_TABLE UINT64_MAX
#define MAX_TABLE_SIZE 0.8*maxSize*MAX_COLLISIONS
#define MIN_TABLE_SIZE 0.15*maxSize*MAX_COLLISIONS
#define FULL 2
#define EXISTS 1
#define INSERTED 0

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
	uint64_t totalSize;
	uint64_t maxSize;
	uint64_t num_procs;
	uint64_t myrank;
	uint64_t min_range;
	uint64_t max_range;
	std::atomic<uint64_t> allocated;
	std::atomic<uint64_t> removed;
	memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *pl;
	boost::atomic<boost::int128_type> full_empty;
	KeyT emptyKey;

	uint64_t KeyToIndex(KeyT k)
	{
	    uint64_t hashval = HashFcn()(k);
	    return hashval % maxSize;
	}

public:
	bool isLocal(KeyT k)
	{
	    uint64_t hashval = HashFcn()(k);
	    uint64_t pos = hashval % totalSize;
	    if(pos >= min_range && pos < max_range) return true;
	    else return false;
	}
	uint64_t procLocation(KeyT k)
	{
	   uint64_t localSize = totalSize/num_procs;
	   uint64_t rem = totalSize%num_procs;
	   uint64_t hashval = HashFcn()(k);
	   uint64_t pos = hashval % totalSize;
	   uint64_t start = 0, end = 0;
	   uint64_t proc_id = -1;

	   for(int i=0;i<num_procs;i++)
	   {
		uint64_t lsize = 0;
		if(i < rem) lsize = localSize+1;
		else lsize = localSize;
		end = start + lsize;
		if(pos >= start && pos < end)
		{
		   proc_id = i; break;
	   	}
     		start += lsize;		
	   }

	   return proc_id;
	}

	BlockMap(uint64_t n,memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *m,KeyT maxKey,uint64_t np,uint64_t rank) : totalSize(n), pl(m), emptyKey(maxKey), num_procs(np),myrank(rank)
	{
  	   assert (totalSize > 0);
	   uint64_t localSize = totalSize/num_procs;
	   uint64_t rem = totalSize%num_procs;
	   if(myrank < rem) maxSize = localSize+1;
	   else maxSize = localSize;
	   min_range = 0;
	   for(int i=0;i<myrank;i++)
	   {
		if(i < rem) min_range += localSize+1;
     		else min_range += localSize;
	   }
	   max_range = min_range + maxSize;	   

	   table = (fnode_type *)std::malloc(maxSize*sizeof(fnode_type));
	   assert (table != nullptr);
	   for(size_t i=0;i<maxSize;i++)
	   {
	      table[i].num_nodes = 0;
	      table[i].head = pl->memory_pool_pop();
	      std::memcpy(&(table[i].head->key),&emptyKey,sizeof(KeyT));
	      table[i].head->next = nullptr; 
	   }
	   allocated.store(0);
	   removed.store(0);
	   full_empty.store(0);
	   assert(maxSize < UINT64_MAX && maxSize*MAX_COLLISIONS < UINT64_MAX);
	}

  	~BlockMap()
	{
	    std::free(table);
	}

	int insert(KeyT k,ValueT v)
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

	    int ret = (found) ? EXISTS : 0;
	    if(!found)
	    {
		boost::int128_type prev, next;
		uint64_t prev_a = allocated.load();
		uint64_t prev_r = removed.load();

		allocated.fetch_add(1);
		node_type *new_node=pl->memory_pool_pop();
		std::memcpy(&new_node->key,&k,sizeof(k));
		std::memcpy(&new_node->value,&v,sizeof(v));
		new_node->next = n;
		p->next = new_node;
		table[pos].num_nodes++;
		found = true;
		ret = INSERTED;
	    }

	   table[pos].mutex_t.unlock();
	   return ret;
	}

	uint64_t find(KeyT k)
	{
	    uint64_t pos = KeyToIndex(k);

	    table[pos].mutex_t.lock();

	    node_type *n = table[pos].head;
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

	bool update(KeyT k,ValueT v)
	{
	   uint64_t pos = KeyToIndex(k);

	   table[pos].mutex_t.lock();

	   node_type *n = table[pos].head;

	   bool found = false;
	   while(n != nullptr)
	   {
		if(EqualFcn()(n->key,k))
		{
		   found = true;
		   std::memcpy(&(n->value),&v,sizeof(ValueT));
		}
		if(HashFcn()(n->key) > HashFcn()(k)) break;
		n = n->next;
	   }

	   table[pos].mutex_t.unlock();
	   return found;
	}

	bool erase(KeyT k)
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
	
        bool block_full()
	{
		boost::int128_type full = 1; full = full << 64;
		return (full_empty.load()==full) ? false : true;
	}
	bool block_empty()
	{
	   boost::int128_type full = 1; full = full << 64;
	   if(allocated.load()-removed.load() < MIN_TABLE_SIZE)
	   {
		boost::int128_type prev = full_empty.load();
		boost::int128_type next = 0;
		if(prev == full)
		{
		    bool b = full_empty.compare_exchange_strong(prev,next);
		}
	   }

	   if(full_empty.load()==full) return false;
	   else return true;
	}	   
};

#endif
