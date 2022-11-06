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

	  bool add_node(skipnode<K, T> * n,K &k, T &data)
	  {
	     skipnode<K,T> *b = n->bottom.load();
	     skipnode<K,T> *bn = n->nlink.load();
	     bool added = false;

	     bool empty = false;
	     K key1; T data1;

	     n->node_lock.lock_shared();
	     b = n->bottom.load();
	     bn = n->nlink.load();
             if(b != n) b->node_lock.lock_shared();
	     if(bn != b) bn->node_lock.lock_shared();
	     if(b->isBottomNode()) empty = true;
	     if(bn != b) bn->node_lock.unlock_shared();
	     if(b != n) b->node_lock.unlock_shared();
	     n->node_lock.unlock_shared();

	     if(empty)
             {
		   skipnode<K,T> *t = pl->memory_pool_pop();
		   skipnode<K,T> *t_b = pl->memory_pool_pop();
		   t_b->bottom.store(t_b); t_b->nlink.store(t_b);
		   t_b->setIsBottomNode();
		   t->bottom.store(t_b);
		   n->node_lock.lock();
		   b = n->bottom.load();
		   bn = n->nlink.load();
		   if(b != n) b->node_lock.lock_shared();
		   if(bn != b) bn->node_lock.lock_shared();
		   if(b->isBottomNode() && bn->isTailNode()) empty = true;
		   else empty = false;
		   if(bn != b) bn->node_lock.unlock_shared();
		   if(b != n) b->node_lock.unlock_shared();

		   if(empty)
		   {
		       t->nlink.store(bn);
		       t->setKey(n->key_); t->setData(n->data_);
                       n->nlink.store(t);
		       n->setKey(k);
                       n->setData(data);
		       added = true;
		   }
		   n->node_lock.unlock();
		   if(!added) 
		   {
		      pl->memory_pool_push(t); pl->memory_pool_push(t_b);
		   }
             }
	     else
	     {
		 std::vector<skipnode<K,T>*> nodes;
		 skipnode<K,T> *p_n = nullptr;
		 K key1; 
		 bool raised = false;

		 n->node_lock.lock_shared();
		 key1 = n->key_;
		 p_n = n->bottom.load();

		 for(;;)
		 {
		   p_n->node_lock.lock_shared();
		   nodes.push_back(p_n);
		   if(p_n->key_ == n->key_) break;
		   p_n = p_n->nlink.load();
		 }
		 n->node_lock.unlock_shared();

		 for(int i=0;i<nodes.size();i++) nodes[i]->node_lock.unlock_shared();

		 if(nodes.size() < 4) return false;
		 else
		 {
		    nodes.clear();
		    skipnode<K,T> *t = pl->memory_pool_pop();

		    n->node_lock.lock();
		    if(n->key_ == key1)
		    {
			p_n = n->bottom.load();

			for(;;)
			{
			   p_n->node_lock.lock_shared();
			   nodes.push_back(p_n);
			   if(p_n->key_ == n->key_) break;
			   p_n = p_n->nlink.load();
			}

			if(nodes.size() >= 4)
			{
			   n->key_ = nodes[1]->key_;
			   n->data_ = nodes[1]->data_;
			   t->key_ = nodes[nodes.size()-1]->key_;
			   t->data_ = nodes[nodes.size()-1]->data_;
			   t->bottom.store(nodes[2]);
			   t->nlink.store(n->nlink.load());
			   n->nlink.store(t);
			   raised = true;
			}

		    }
		    n->node_lock.unlock();

		    for(int i=0;i<nodes.size();i++) nodes[i]->node_lock.unlock_shared();
		    if(!raised) pl->memory_pool_push(t);
		    return raised;

		 }

	     }
	     return added; 
	  }
		
	  bool addleafnode(skipnode<K,T> *n,K &k, T& data)
	  {
		  std::vector<skipnode<K,T>*> nodes;

		  skipnode<K,T> *p_n = nullptr;
		  bool leaf_level = false;
		  skipnode<K,T> *b,*bn;
		  bool found = false;

		  n->node_lock.lock_shared();
		  b = n->bottom.load();
		  if(b != n) b->node_lock.lock_shared();
		  bn = b->bottom.load();
		  if(bn != b) bn->node_lock.lock_shared();
		  leaf_level = !b->isBottomNode() && bn->isBottomNode();
		  if(bn != b) bn->node_lock.unlock_shared();
		  if(b != n) b->node_lock.unlock_shared();
		  if(leaf_level)
		  {
			p_n = b;
			for(;;)
			{
			    p_n->node_lock.lock_shared();
			    nodes.push_back(p_n);
			    if(p_n->key_ == n->key_) break;
			    p_n = p_n->nlink.load();
			}

			for(int i=0;i<nodes.size();i++)
			{
				if(nodes[i]->key_ == k)
				{
				   found = true; 
				}
				nodes[i]->node_lock.unlock_shared();
			}
		  }

		  n->node_lock.unlock_shared();

		  if(found) return true;
		  else
		  {
		     skipnode<K,T> *t = pl->memory_pool_pop();
		     skipnode<K,T> *r = pl->memory_pool_pop();
		     skipnode<K,T> *nn = pl->memory_pool_pop();
		     r->bottom.store(r); r->nlink.store(r);
	     	     r->setIsBottomNode();	     
		     t->bottom.store(r);
		     bool raised = false, added = false;

		     //std::cout <<" k = "<<k<<" n = "<<n<<" head = "<<head.load()<<std::endl;
		     n->node_lock.lock();
		     leaf_level = false;
		     b = n->bottom.load();
		     if(n != b) b->node_lock.lock_shared();
		     bn = b->bottom.load();
		     if(bn != b) bn->node_lock.lock_shared();
		     leaf_level = !b->isBottomNode() && bn->isBottomNode();
		     if(bn != b) bn->node_lock.unlock_shared();
		     if(b != n) b->node_lock.unlock_shared();
		     if(leaf_level && k <= n->key_)
		     {
			nodes.clear();
			p_n = n->bottom.load();
			for(;;)
			{
			   nodes.push_back(p_n); 
			   if(p_n->key_ == n->key_) break;
			   p_n = p_n->nlink.load();
			}

			for(int i=0;i<nodes.size();i++)
			{
			   if(k <= nodes[i]->key_)
			   {
				if(k==nodes[i]->key_)
				{
					found = true; break;
				}
				else
				{
				   assert(nodes[i]!=n);
				   nodes[i]->node_lock.lock();
				   t->key_ = nodes[i]->key_;
				   t->data_ = nodes[i]->data_;
				   t->nlink.store(nodes[i]->nlink.load());
				   nodes[i]->key_ = k;
				   nodes[i]->data_ = data;
				   nodes[i]->nlink.store(t);
				   added = true;
				   found = true;
				   nodes[i]->node_lock.unlock();
				   break;
				}

			   }
			}
			nodes.clear();
		
			p_n = n->bottom.load();
			for(;;)
			{
			   p_n->node_lock.lock_shared();
			   nodes.push_back(p_n);
			   if(p_n->key_ == n->key_) break;
			   p_n = p_n->nlink.load();
			}
				
			if(nodes.size() >= 4)
			{
			     n->key_ = nodes[1]->key_;
		     	     n->data_ = nodes[1]->data_;
			     nn->key_ = nodes[nodes.size()-1]->key_;
			     nn->data_ = nodes[nodes.size()-1]->data_;
			     nn->nlink.store(n->nlink.load());
			     nn->bottom.store(nodes[2]);	     
			     n->nlink.store(nn);
			     raised = true;
			}

			for(int i=0;i<nodes.size();i++) nodes[i]->node_lock.unlock_shared();
		     }
		     n->node_lock.unlock();
		     if(!added) 
		     {
			   pl->memory_pool_push(t); pl->memory_pool_push(r);
		     }
		     if(!raised) pl->memory_pool_push(nn);
		  }

		  return found;
	  }

	  bool IncreaseDepth()
	  {
	    skipnode<K,T> *n = head.load();
	    bool invalid = false;
	    n->node_lock.lock_shared();
	    n->nlink.load()->node_lock.lock_shared();
	    invalid = n->nlink.load()->isTailNode();
	    n->nlink.load()->node_lock.unlock_shared();
	    n->node_lock.unlock_shared();
            if(!invalid)
            {
	      bool inc = false;
              skipnode<K,T> *t = pl->memory_pool_pop();
	      n->node_lock.lock();
	      skipnode<K,T> *nn = n->nlink.load();
	      assert (n != nn);
	      nn->node_lock.lock_shared();
	      if(!n->nlink.load()->isTailNode())
	      {
		t->setKey(n->key_);
		t->setData(n->data_);
		t->bottom.store(n->bottom.load());
		t->nlink.store(n->nlink.load());		
                n->bottom.store(t);
	        n->nlink.store(tail.load());
	        n->setKey(maxValue);
		inc = true;
	      }
	      nn->node_lock.unlock_shared();
	      n->node_lock.unlock();
	      if(!inc) pl->memory_pool_push(t);
	      return inc;
            }
	    return false;
	  }

	  bool ValidHead()
	  {
	     return head.load()->nlink.load()->isTailNode();
	  }

	  bool isLeafNode(skipnode<K,T> *n)
	  {
		return n->bottom.load()->isBottomNode();
	  }

	  bool Insert(skipnode<K,T> *n,K &k, T& data)
	  {
		bool found = false;
		bool invalid = false;
                std::vector<skipnode<K,T> *> nodes;
		skipnode<K,T> *p_n = nullptr;
		bool leaf_level = false;
		bool next_node = false;
		skipnode<K,T> *nn = nullptr;

		n->node_lock.lock_shared();
		invalid = n->isBottomNode() || n->isTailNode() ;
		skipnode<K,T> *pn = n->nlink.load();
		if(pn != n) pn->node_lock.lock_shared();
		if(n==head.load() && !ValidHead()) invalid = true;
		if(pn != n) pn->node_lock.unlock_shared();
		p_n = n->bottom.load();
		if(p_n != n) p_n->node_lock.lock_shared();
		if(p_n->bottom.load() != p_n) p_n->bottom.load()->node_lock.lock_shared();
		if(!p_n->isBottomNode() && p_n->bottom.load()->isBottomNode()) leaf_level = true;
		if(p_n->bottom.load() != p_n) p_n->bottom.load()->node_lock.unlock_shared();
		if(p_n != n) p_n->node_lock.unlock_shared();
		if(k > n->key_) 
		{
		   next_node = true; nn = n->nlink.load();
		}
		n->node_lock.unlock_shared();
		if(invalid) //(n==head.load() && n->key_ == n->bottom.load()->key_))
		{
		    return false; 
		}

		if(next_node)
		{
			found = Insert(nn,k,data);
			return found;
		}

		bool d = false;
		if(leaf_level)
		{
		   d = addleafnode(n,k,data);
		   /*if(head.load()->bottom.load()==n)
		   {
		      std::vector<skipnode<K,T>*> temp;
		      p_n = n->bottom.load();
		      for(;;)
		      { 
			p_n->node_lock.lock_shared();
			temp.push_back(p_n);
			//std::cout <<" n = "<<n->key_<<" nl = "<<p_n->key_<<std::endl;
			if(p_n->key_ == n->key_) break;
			p_n = p_n->nlink.load();
		      }

		      for(int i=0;i<nodes.size();i++) temp[i]->node_lock.unlock_shared();
		   }*/
		   if(d) return true;
		   else
		   {
		      nn = nullptr;
		      n->node_lock.lock_shared();
		      nn = n->nlink.load();	     
		      n->node_lock.unlock_shared();
			
		      if(n != head.load())
		      found = Insert(nn,k,data);
		      else
		      return found;
		   }
		}
		else
		{
		  bool f = add_node(n,k,data);
		}

		next_node = false;
		nn = nullptr;
		int pos = -1;

		n->node_lock.lock_shared();
	        
		if(k <= n->key_)
		{
		  p_n = n->bottom.load();	
		  for(;;)
		  {
		     p_n->node_lock.lock_shared();
		     nodes.push_back(p_n);
		     if(p_n->key_ == n->key_|| p_n->isBottomNode()) break;
		     p_n = p_n->nlink.load();
		  }

		  for(int i=0;i<nodes.size();i++)
		  {
		   if(k <= nodes[i]->key_)
		   {
		      pos = i; break;
		   }
		  }
		  for(int i=0;i<nodes.size();i++) nodes[i]->node_lock.unlock_shared();
		}
		else 
		{
		   next_node = true;
		   nn = n->nlink.load();
		}
		n->node_lock.unlock_shared();

		if(next_node)
		{
		   found = Insert(nn,k,data);  
		}
		else
		{
		   if(pos != -1)
		   {
		     found = Insert(nodes[pos],k,data);
		     return found;
		   }
		   
		   else return false;
		}

		return found;
	  }

	  bool InsertData(K& k,T &data)
	  {
	     bool b = false;

	     skipnode<K,T> *n = head.load();
	     skipnode<K,T> *nn = nullptr;
	     bool valid = false;
	     n->node_lock.lock_shared();
	     nn = n->nlink.load();
	     if(nn != n) nn->node_lock.lock_shared();
	     valid = ValidHead();
	     if(nn != n) nn->node_lock.unlock_shared();
	     n->node_lock.unlock_shared();
	     if(valid)
	     {
	        b = Insert(n,k,data);
	     }

	     n->node_lock.lock_shared();
	     nn = n->nlink.load();
	     if(n != nn) nn->node_lock.lock_shared(); 
	     valid = ValidHead();
	     if(n != nn) nn->node_lock.unlock_shared();
	     n->node_lock.unlock_shared();

	     if(!valid)
		 IncreaseDepth();

	     return b;
	  }

	  std::vector<K> & RangeSearch(K&,K&);

	  bool Find(skipnode<K,T> *n, K &k)
	  {
		bool found = false;

		bool invalid = false;
		bool leaflevel = false;
		bool next_node = false;
		skipnode<K,T> *p_n = nullptr;
		skipnode<K,T> *n_n = nullptr;
		K key;

		n->node_lock.lock_shared();
		invalid = n->isBottomNode() || n->isTailNode();
		invalid = n==head.load() && !ValidHead();
		next_node = (k > n->key_);
	        n_n = n->nlink.load();
		p_n = n->bottom.load();
		key = p_n->key_;
		n->node_lock.unlock_shared();

		if(invalid) 
		{
			found = false;
			return found;
		}

		if(next_node)
		{
		    found = Find(n_n,k);
		    return found;
		}

		std::vector<skipnode<K,T>*> nodes;
	        std::vector<K> keys;
	
		int pos = -1;

		if(p_n != n)
		{
		    for(;;)
		    {
			nodes.push_back(p_n);
			keys.push_back(key);
			bool end_node = false;
			skipnode<K,T> *np = nullptr;
			p_n->node_lock.lock_shared();
			end_node = p_n->key_ == n->key_ || p_n->isBottomNode();
			np = p_n->nlink.load();
			leaflevel = p_n->bottom.load()->isBottomNode();
			key = np->key_;
			p_n->node_lock.unlock_shared();
			if(end_node) break;
			p_n = np;
		    }
		}

		for(int i=0;i<nodes.size();i++)
		{
			bool fnode = false;
			fnode = (k <= keys[i]) ? true : false;

			if(fnode)
			{
			   pos = i; break;
			}
		}

		if(pos!=-1)
		{
		    
		    if(leaflevel) found = (keys[pos] == k) ? true : false;
		    if(leaflevel)
		    {
			return found;
		    }
		    else
		    {
			found = Find(nodes[pos],k);
		    }
		}
		else
		{
			return false;
		}

		return found;
	  }
	  bool FindData(K &k)
	  {
	      bool b = false;
              bool valid = false;	
	      skipnode<K,T> *n = head.load();
	      n->node_lock.lock_shared();
	      valid = ValidHead();
	      n->node_lock.unlock_shared();
	      if(valid)
		b = Find(n,k);
	      return b;
	  }
	  void check_list()
	  {
		skipnode<K,T> *t = head.load();
		//assert (t->nlink.load()->isTailNode());

		skipnode<K,T> *b = head.load();
		std::cout <<" level = "<<0<<" b data = "<<b->key_<<" l data = "<<b->bottom.load()->key_<<std::endl;
	
	        b = head.load()->bottom.load();
		std::cout <<" level = "<<1<<std::endl;

		while(!b->isTailNode() && !b->isBottomNode())
		{
			std::cout <<" b data = "<<b->key_<<" l data = "<<b->bottom.load()->key_<<std::endl;
			b = b->nlink.load();
		}

		b = head.load()->bottom.load()->bottom.load();

		std::cout <<" level = "<<2<<std::endl;
		while(!b->isTailNode() && !b->isBottomNode())
		{
			//if(b->key_ == b->bottom.load()->key_) assert(b->bottom.load()->isBottomNode());
			std::cout <<" b data = "<<b->key_<<" l data = "<<b->bottom.load()->key_<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;;
			b = b->nlink.load();
		
		}

		std::cout <<" level = "<<3<<std::endl;
		
		b = head.load()->bottom.load()->bottom.load()->bottom.load();

		while(!b->isTailNode() && !b->isBottomNode())
		{
		   //if(b->key_==b->bottom.load()->key_) assert(b->bottom.load()->isBottomNode());
		   std::cout <<" b data = "<<b->key_<<" l data = "<<b->bottom.load()->key_<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;
		   b = b->nlink.load();
		}

		std::cout <<" level = "<<4<<std::endl;

		b = head.load()->bottom.load()->bottom.load()->bottom.load()->bottom.load();

		while(!b->isTailNode() && !b->isBottomNode())
		{
		   std::cout <<" b data = "<<b->key_<<" l data = "<<b->bottom.load()->key_<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;
		   b = b->nlink.load();
		}


	  }

};


#include "Skiplist.cpp"

#endif
