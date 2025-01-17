#ifndef HCL_MEMORY_H
#define HCL_MEMORY_H

#include <boost/lockfree/queue.hpp>
#include <mutex>
#include <vector>
#include <iostream>
#include <algorithm>
#include <cstdarg>
#include <functional>

#define MIN_FREE_NODES 100

/* This module is used by the concurrent unordered map container for memory management. 
 * It allocates nodes and reuses them to keep the total memory allocation the program under check.
 * Since the container is concurrent, the allocated nodes are kept in two concurrent queues : active and free
 * The active queue contains nodes available for the unordered map and the free queue contains nodes that have been deleted by the map. Nodes are moved from the free queue to the active queue when the number of available nodes goes to zero*/

namespace hcl
{

template<
        class KeyT,
        class ValueT,
        class HashFcn=std::hash<KeyT>,
        class EqualFcn=std::equal_to<KeyT>
        >
struct node
{
   KeyT key;
   ValueT value;
   struct node*next;
};


template <class KeyT,
	 class ValueT,
	 class HashFcn = std::hash<KeyT>,
	 class EqualFcn = std::equal_to<KeyT>>
class memory_pool
{

  public :
	 typedef struct node<KeyT,ValueT,HashFcn,EqualFcn> node_type;
  private :
	  uint32_t chunk_size;
	  std::atomic<uint64_t> free_nodes;
	  std::atomic<uint64_t> num_chunks;
	  boost::lockfree::queue<node_type *> *memory_chunks;
	  boost::lockfree::queue<node_type *> *active_queue;
	  boost::lockfree::queue<node_type *> *free_queue;

  public :
	  memory_pool(uint32_t csize) : chunk_size(csize)
	 {
	     memory_chunks = new boost::lockfree::queue<node_type*> (1024);
	     active_queue = new boost::lockfree::queue<node_type*> (1024);
	     free_queue = new boost::lockfree::queue<node_type*> (1024);
	     assert (memory_chunks != nullptr && active_queue != nullptr && free_queue != nullptr);
	     node_type *chunk = (node_type *)std::malloc(chunk_size*sizeof(node_type));
	     assert (chunk != nullptr);
	     for(uint32_t i=0;i<chunk_size;i++)
	     {
		new (&(chunk[i].key)) KeyT();
		new (&(chunk[i].value)) ValueT();
		chunk[i].next = nullptr;
		active_queue->push(&(chunk[i]));
	     }
	     memory_chunks->push(chunk);
	     free_nodes.store(0);
	     num_chunks.store(1);
	 }

	 ~memory_pool()
	 {
		int n = 0;
		node_type *chunk = nullptr;
		while(memory_chunks->pop(chunk))
		{
		   n++;
		   std::free(chunk);
		}
		delete memory_chunks;
		delete active_queue;
		delete free_queue;
	 }

	 node_type* memory_pool_pop()
	 {
		node_type *n = nullptr;
		while(!active_queue->pop(n))
		{
		    node_type *t = nullptr;
		    while(free_queue->pop(t))
		    {
			t->next = nullptr;
			active_queue->push(t);
			t = nullptr;
		    }
		    free_nodes.store(0);
		  
		    if(active_queue->pop(n)) break;  

		    node_type *chunk = (node_type*)std::malloc(chunk_size*sizeof(node_type));
		    assert (chunk != nullptr);
		    num_chunks.fetch_add(1);
		    for(uint32_t i=0;i<chunk_size;i++)
		    {
			new (&(chunk[i].key)) KeyT();
			new (&(chunk[i].value)) ValueT();
			chunk[i].next = nullptr;
			active_queue->push(&(chunk[i]));
		    }
		    memory_chunks->push(chunk);
		    if(free_queue->empty()) free_nodes.store(0);
		}

		return n;
	 }
	 void memory_pool_push(node_type *n)
	 {
		free_queue->push(n);
		free_nodes.fetch_add(1);
	 }

};

}
#endif
