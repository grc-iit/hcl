#ifndef INCLUDE_HCL_SKIPLIST_ALLOC_H_
#define INCLUDE_HCL_SKIPLIST_ALLOC_H_ 

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
#include <boost/thread/lock_types.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/is_locked_by_this_thread.hpp>

/** Standard C++ Headers**/
#include <hcl/common/container.h>


#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <type_traits>

#include "memory_allocation.h"

template<class K, class T>
class Skiplist
{
    private:
	  memory_allocator<K,T> *pl;
	  std::atomic<skipnode<K,T>*> head;
	  std::atomic<skipnode<K,T>*> bottom;
	  std::atomic<skipnode<K,T>*> tail;
	  K maxValue;
	  K maxValue1;
    
    public: 
	  Skiplist(memory_allocator<K,T> *m) : pl(m)
	  {
		skipnode<K,T> *n = pl->memory_pool_pop();
		n->setIsHeadNode();
		maxValue = INT32_MAX-1;
		maxValue1 = maxValue+1;
		n->setKey(maxValue);
		head.store(n);
		skipnode<K,T> *n1 = pl->memory_pool_pop();
		n1->setKey(maxValue1);
		n1->setIsTailNode();
		skipnode<K,T> *n2 = pl->memory_pool_pop();
		n2->setIsBottomNode();
		head.load()->bottom.store(n2);
		head.load()->nlink.store(n1);
		n2->nlink.store(n2);
		n2->bottom.store(n2);
		n1->nlink.store(n1);
		n1->bottom.store(n1);
		tail.store(n1);
		bottom.store(n2);

	  }
	  ~Skiplist()
	  {

	  }

	  void add_node(skipnode<K, T> * n,K &k, T &data)
	  {
	     if(n->bottom.load()->isBottomNode() || n->key_ == 
                                   n->bottom.load()->nlink.load()->nlink.load()->nlink.load()->key_)
             {
                 skipnode<K,T> *t = pl->memory_pool_pop();
		 if(n->bottom.load()->isBottomNode())
		 {
		    n->bottom.load()->key_ = k;
		    n->bottom.load()->data_ = data;
		     
		    skipnode<K,T> *b = pl->memory_pool_pop();
		    b->bottom.store(b);
		    b->nlink.store(b);
		    b->setIsBottomNode();
		    t->bottom.store(b);
		 }
                 t->nlink.store(n->nlink.load());
		 if(!n->bottom.load()->isBottomNode())
		 {
                   t->bottom.store(n->bottom.load()->nlink.load()->nlink.load());
		 }
                 n->nlink.store(t);
		 t->setKey(n->key_);
                 t->setData(n->data_);
		 n->setKey(n->bottom.load()->nlink.load()->key_);
                 n->setData(n->bottom.load()->nlink.load()->data_);
             } 
	  }
	  void IncreaseDepth()
	  {
            if(!head.load()->nlink.load()->isTailNode())
            {
              skipnode<K,T> *t = pl->memory_pool_pop();
              t->bottom.store(head.load());
              t->nlink.store(tail.load());
              t->setKey(maxValue);
              head.store(t);
            }
	  }
	  bool Insert(boost::shared_lock<boost::upgrade_mutex> &lk1, skipnode<K,T> *n, K & k, T&data)
	  {
		 bool found = false;

		 if(n->isBottomNode() || (n->bottom.load()->isBottomNode() && k == n->key_)) 
		 {
			 return true;
		 }

		 if(k > n->key_)
		 {
		     skipnode<K,T> *n1 = n->nlink.load();
		     boost::shared_lock<boost::upgrade_mutex> lk2(n1->node_lock);
		     lk1.unlock();lk1.release();
		     found = Insert(lk2,n1,k,data);
		     return found;
		 }
		 else
		 {
		      while(true)
		      {	    

		        boost::upgrade_lock<boost::upgrade_mutex> lk2(std::move(lk1),boost::try_to_lock);
		        if(lk2.owns_lock())
		        {
		          skipnode<K,T> *b = n->bottom.load();

			  {
	                  boost::upgrade_lock<boost::upgrade_mutex> l1(b->node_lock);
		          if(b->isBottomNode()){ b->key_ = k; b->data_ = data;}
			  {
				skipnode<K,T> *n1 = b->nlink.load();
				if(!b->isBottomNode() && !n1->isTailNode())
				   boost::upgrade_lock<boost::upgrade_mutex> l2(n1->node_lock);
				{
				   skipnode<K,T> *n2 = n1->nlink.load();
				   if(!n2->isTailNode() && !n2->isBottomNode())
				      boost::upgrade_lock<boost::upgrade_mutex> l3(n2->node_lock);

				   {
				      skipnode<K,T> *n3 = n2->nlink.load();
				      if(!n3->isTailNode() && !n3->isBottomNode())
					  boost::upgrade_lock<boost::upgrade_mutex> l4(n3->node_lock);

				    	 add_node(n,k,data);
				   } 
				}
			 }
			 if(n == head.load()) IncreaseDepth();
			 }

			 boost::shared_lock<boost::upgrade_mutex> lk3(std::move(lk2));
			 assert(lk3.owns_lock()==true);
			 assert(lk2.owns_lock()==false);
			 b = n->bottom.load();
			 boost::shared_lock<boost::upgrade_mutex> lk4(b->node_lock);
			 lk3.unlock();lk3.release();
			 found = Insert(lk4,b,k,data);
			break;
		      }
		    }

		 }

		  return found;
	  }
	  bool InsertData(K& k,T &data)
	  {
	     bool b = false;

	     while(true)
	     {
		skipnode<K,T> *n = head.load();
		boost::shared_lock<boost::upgrade_mutex> lk0(n->node_lock);
		if(!n->nlink.load()->isTailNode())
		{
		   lk0.unlock();
		   lk0.release();
		}
		else
		{
		   b = Insert(lk0,n,k,data);
		   break;
		}
	     }	

	     return b;
	  }
	  void Erase(K&);
	  std::vector<K> & RangeSearch(K&,K&);

	  bool Find(boost::shared_lock<boost::upgrade_mutex> &lk0, skipnode<K,T> *n, K &k)
	  {
		bool found = false;

		if(n->isBottomNode()) found = false;

		if(k > n->key_)
		{
		    skipnode<K,T> *n1 = n->nlink.load();
		    boost::shared_lock<boost::upgrade_mutex> lk(n1->node_lock);
		    lk0.unlock();
		    lk0.release();
		    found = Find(lk,n1,k);
		}
		else if(n->bottom.load()->isBottomNode())
		{
		   if(k == n->key_) found = true;
		   else found = false;
		}
		else 
		{
		   skipnode<K,T> *b = n->bottom.load();
		   boost::shared_lock<boost::upgrade_mutex> lk(b->node_lock);
		   lk0.unlock();
		   lk0.release();
		   found = Find(lk,b,k);
		}

		return found;
	  }
	  bool FindData(K &k)
	  {
	      bool b = false;
		
	      while(true)
	      {
		skipnode<K,T> *n = head.load();
		boost::shared_lock<boost::upgrade_mutex> lk(n->node_lock);
		if(!n->nlink.load()->isTailNode()) 
		{
		   lk.unlock();
		   lk.release();
		}
		else 
		{
		   b = Find(lk,n,k);
		   break;
		}
	      }
	      return b;
	  }
	  /*void check_list()
	  {
		skipnode<T> *t = head.load();
		assert (t->nlink.load()->isTailNode());

		skipnode<T> *b = head.load();
		std::cout <<" level = "<<0<<" b data = "<<b->data_<<std::endl;
	
	        b = head.load()->bottom.load();
		std::cout <<" level = "<<1<<std::endl;

		while(!b->isTailNode())
		{
			std::cout <<" b data = "<<b->data_<<std::endl;
			b = b->nlink.load();
		}

		b = head.load()->bottom.load()->bottom.load();

		std::cout <<" level = "<<2<<std::endl;
		while(!b->isTailNode())
		{
			std::cout <<" b data = "<<b->data_<<std::endl;
			b = b->nlink.load();
		}	

	  }*/ 

};


#include "Skiplist.cpp"

#endif
