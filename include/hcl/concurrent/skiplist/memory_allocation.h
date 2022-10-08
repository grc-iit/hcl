#ifndef INCLUDE_HCL_MEMORY_ALLOC_H_
#define INCLUDE_HCL_MEMORY_ALLOC_H_ 

#include <vector>
#include <boost/lockfree/queue.hpp>
#include <memory>
#include <climits>
#include <algorithm>
#include <functional>
#include <cstdlib>
#include "node.h"
#include <iostream>

uint64_t next_power_two(uint64_t n)
{
   uint64_t c = 1;

   while(c < n)
   {
     c = c*2;
   }

   return c;
}

template<class K, class T>
class memory_allocator
{
   public :

   private:
	 boost::lockfree::queue<skipnode<K,T>*> *active_queue;
	 boost::lockfree::queue<skipnode<K,T>*> *free_queue;
	 boost::lockfree::queue<skipnode<K,T>*> *memory_blocks;
	 int csize_;

   public:
	 memory_allocator(int n) : csize_(n)
	 {
		active_queue = new boost::lockfree::queue<skipnode<K,T>*> (128);
		free_queue = new boost::lockfree::queue<skipnode<K,T>*> (128);
		memory_blocks = new boost::lockfree::queue<skipnode<K,T>*> (128);

		skipnode<K,T> *chunk = new skipnode<K,T> [csize_];
		for(int i=0;i<csize_;i++)
		{
		   active_queue->push(&(chunk[i]));
		}
		memory_blocks->push(chunk);

	 }
	 ~memory_allocator()
	 {
		delete active_queue;
		delete free_queue;

		skipnode<K,T> *s = nullptr;
		while(memory_blocks->pop(s))
		{
			delete [] s;
			s = nullptr;
		}

	 }

	 skipnode<K,T>* memory_pool_pop()
	 {
	    skipnode<K,T> *p = nullptr;

	    while(!active_queue->pop(p))
	    {
	      skipnode<K,T> *r = nullptr;
	      while(free_queue->pop(r))
	      {
		 active_queue->push(r);
		 r = nullptr;
	      }

	      if(active_queue->pop(p)) break;

	      skipnode<K,T> *chunk = new skipnode<K,T> [csize_];

	      for(int i=0;i<csize_;i++)
	      {
		 active_queue->push(&(chunk[i]));
	      }
	      memory_blocks->push(chunk);
            }
	
    	    assert (p != nullptr);	    
	    return p;
	 }

	 void memory_pool_push(skipnode<K,T> *p)
	 {
	    free_queue->push(p);
	 }
	    
};	    

#endif
