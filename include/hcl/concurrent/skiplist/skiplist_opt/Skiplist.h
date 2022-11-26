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
#include <boost/atomic.hpp>
#include <boost/cstdint.hpp>
#include <boost/config.hpp>
#include <atomic>

/** Standard C++ Headers**/
#include <hcl/common/container.h>


#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <type_traits>

#include "memory_allocation.h"
#include <bitset>

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
		n->setFullyLinked();
		maxValue = INT32_MAX-1;
		maxValue1 = maxValue+1;
		head.store(n);
		skipnode<K,T> *n1 = pl->memory_pool_pop();
		n1->setIsTailNode();
		skipnode<K,T> *n2 = pl->memory_pool_pop();
		n2->setIsBottomNode();
		head.load()->bottom.store(n2);
		boost::int128_type kn = maxValue;
		kn = kn << 64;
		kn = kn | (boost::int64_t)n1;
		head.load()->key_nlink.store(kn);
		kn = head.load()->key_nlink.load();
		K key = (K)(kn >> 64);
		kn = maxValue1;
		kn = kn << 64;
		kn = kn | (boost::int64_t)n1;
		n1->bottom.store(n1);
		n1->key_nlink.store(kn);
		kn = 0;
		kn = kn << 64;
		kn = kn | (boost::int64_t)n2;
		n2->key_nlink.store(kn);
		n2->bottom.store(n2);
		n1->setFullyLinked();
		n2->setFullyLinked();
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

	     b = n->bottom.load();
	     while(!n->isFullyLinked() && !n->isMarked());
	     if(n->isMarked()) return false;
	       
	     n->node_lock.lock();
	     boost::int128_type kn = n->key_nlink.load();
	     K key = (K)(kn >> 64);
	     skipnode<K,T> *next = (skipnode<K,T>*) kn;

	     if(n==head.load () && n->bottom.load()->isBottomNode() && next->isTailNode())
             {
		   skipnode<K,T> *t = pl->memory_pool_pop();
		   skipnode<K,T> *t_b = pl->memory_pool_pop();
		   boost::int128_type t_n = 0; t_n = t_n << 64; t_n = t_n | (boost::int64_t)t_b;
		   t_b->bottom.store(t_b); t_b->key_nlink.store(t_n);
		   t_b->setIsBottomNode();
		   boost::int128_type k_n = 0; k_n = k_n << 64;
		   k_n = k_n | (boost::int64_t)t_b;
		   t_b->key_nlink.store(k_n);
		   t->bottom.store(t_b);
		   b = n->bottom.load();
		   
		   boost::int128_type k_nn = key; k_nn = k_nn << 64;
		   k_nn = k_nn | (boost::int64_t)next;
		   t->key_nlink.store(k_nn);
		   k_nn = t->key_nlink.load();
		   K key_n = (K)(k_nn >> 64);
		   boost::int128_type n_nn = k; 
		   n_nn = n_nn << 64;
		   n_nn = n_nn | (boost::int64_t)t;
		   bool bn = n->key_nlink.compare_exchange_strong(kn,n_nn);
		   n_nn = n->key_nlink.load();
		   key_n = (K)(n_nn >> 64);
		   t->setFullyLinked();
		   t_b->setFullyLinked();
		   if(bn) added = true;
		   
		   if(!added) 
		   {
		      pl->memory_pool_push(t); pl->memory_pool_push(t_b);
		   }
             }
	     else if(!n->isMarked()) 
	     {
		 std::vector<skipnode<K,T>*> nodes;
		 skipnode<K,T> *p_n = nullptr;
		 K key;
		 bool raised = false;
		 boost::int128_type kn;
		 boost::int128_type n_k;

		 n_k = n->key_nlink.load();
		 K n_key = (K)(n_k >> 64);
		 p_n = n->bottom.load();

		 for(;;)
		 {
		     while(!p_n->isFullyLinked() && !p_n->isMarked());
		     p_n->node_lock.lock();
		     kn = p_n->key_nlink.load();
		     key = (K)(kn >> 64);
		     nodes.push_back(p_n);
		     if(key == n_key ||p_n->isBottomNode()||p_n->isTailNode()) break;
		     p_n = (skipnode<K,T>*)kn;
		 }

 		 if(nodes.size() > 4)
		 {
		    bool raise = true;
		    for(int i=0;i<nodes.size();i++)
			    if(nodes[i]->isMarked()) raise = false;
		    if(raise)
		    {
		       skipnode<K,T> *t = pl->memory_pool_pop();
		       t->bottom.store(nodes[2]);
		       kn = nodes[1]->key_nlink.load(); kn = kn >> 64; kn = kn << 64;
		       kn = kn | (boost::int64_t)t;
		       bool bn = n->key_nlink.compare_exchange_strong(n_k,kn);
		       kn = n_key; kn = kn << 64;
		       kn = kn | (boost::int64_t)n_k;
		       t->key_nlink.store(kn);
		       t->setFullyLinked(); 

		    }
		 }		 

		 for(int i=0;i<nodes.size();i++)
		 {
			nodes[i]->node_lock.unlock();
		 }

	     }
	     n->node_lock.unlock();
	     return added; 
	  }
		
	  bool addleafnode(skipnode<K,T> *n,K &k, T& data)
	  {
		  std::vector<skipnode<K,T>*> nodes;
		  std::vector<K> keys;

		  skipnode<K,T> *p_n = nullptr;
		  bool leaf_level = false;
		  skipnode<K,T> *b,*bn;
		  bool found = false;
		  bool invalid = false;
		  boost::int128_type kn, k_n;
		  K key;

		  while(!n->isFullyLinked() && !n->isMarked());
		  n->node_lock.lock();
		  kn = n->key_nlink.load();
		  key = (K)(kn >> 64);
		  p_n = (skipnode<K,T>*)kn;
		  invalid = (n==head.load() && !p_n->isTailNode()) || n->isMarked();
		  if(invalid)
		  {
		     n->node_lock.unlock(); return false;
		  }
		    
		  b = n->bottom.load();
		  bn = b->bottom.load();
		  //if(n->key_ == b->key_ && !b->isBottomNode()) invalid = true;
		  leaf_level = !b->isBottomNode() && bn->isBottomNode();
		  if(leaf_level && k <= key)
		  {
			p_n = b;
			for(;;)
			{
			    while(!p_n->isFullyLinked() && !p_n->isMarked());
			    p_n->node_lock.lock();
			    nodes.push_back(p_n);
			    k_n = p_n->key_nlink.load();
			    K n_key = (K)(k_n >> 64);
			    keys.push_back(n_key);
			    if(n_key == key ||p_n->isBottomNode()) break;
			    p_n = (skipnode<K,T>*)k_n;
			}

			for(int i=0;i<nodes.size();i++)
			{
				if(keys[i] >= k)
				{
				   found = true;
				   if(keys[i] != k)
				   {
			             skipnode<K,T> *t = pl->memory_pool_pop();
			             skipnode<K,T> *r = pl->memory_pool_pop();
			             r->bottom.store(r); 
			             kn = 0; kn = kn << 64; kn = kn | (boost::int128_type)r;
			             r->key_nlink.store(kn); r->setIsBottomNode();
			             t->bottom.store(r);
			             boost::int128_type pr = nodes[i]->key_nlink.load();
			             kn = nodes[i]->key_nlink.load(); t->key_nlink.store(kn);
			             kn = k; kn = kn << 64; kn = kn | (boost::int64_t)t;
			             bool bn = nodes[i]->key_nlink.compare_exchange_strong(pr,kn);
			             r->setFullyLinked(); t->setFullyLinked();
			            }
				    break;
				}
			}

		  }
		  for(int i=0;i<nodes.size();i++)
			nodes[i]->node_lock.unlock();
		  n->node_lock.unlock();

		  return found;
	  }

	  bool IncreaseDepth()
	  {
	    skipnode<K,T> *n = head.load();
	    bool invalid = false;
	    boost::int128_type kn;
	    n->node_lock.lock();
	    kn = n->key_nlink.load();
	    K key = (K)(kn >> 64);
	    skipnode<K,T> *nn = (skipnode<K,T>*)kn;
	    while(!nn->isFullyLinked() && !nn->isMarked());
	    if(nn->isTailNode()) invalid = true;
	    bool inc = false;
            if(!invalid)
            {
              skipnode<K,T> *t = pl->memory_pool_pop();
	      kn = (boost::int64_t)key; kn = kn << 64;
	      kn = kn | (boost::int64_t) nn;
	      t->key_nlink.store(kn);
	      t->bottom.store(n->bottom.load());
              n->bottom.store(t);
	      kn = (boost::int64_t)maxValue; kn = kn << 64;
	      kn = kn | (boost::int64_t)tail.load();
	      n->key_nlink.store(kn);
	      t->setFullyLinked();
	      inc = true;
            }
	    n->node_lock.unlock();
	    return inc;
	  }

	  bool DecreaseDepth()
	  {
	      skipnode<K,T> *n = head.load();
	      n->node_lock.lock();
	      skipnode<K,T> *b = n->bottom.load();
	      b->node_lock.lock();
	      bool dec = false;
	      boost::int128_type kn = n->key_nlink.load();
	      K key1 = (K)(kn >> 64);
	      kn = b->key_nlink.load();
	      K key2 = (K)(kn >> 64);
	      if(key1 == key2 && !b->isBottomNode())
	      {
		//std::cout <<" decreasedepth"<<std::endl;
		skipnode<K,T> *bb = b->bottom.load();
		n->bottom.store(bb);
		b->setFlags(0); b->setMarkNode();
		dec = true;
	      }
	      b->node_lock.unlock();
	      n->node_lock.unlock();
	      return dec;
	  }
	  bool ValidHead()
	  {
	     return head.load()->nlink.load()->isTailNode();
	  }

	  bool isLeafNode(skipnode<K,T> *n)
	  {
		return n->bottom.load()->isBottomNode();
	  }

	  bool drop_key(skipnode<K,T> *n,K &k,std::vector<skipnode<K,T>*> &nodes)
	  {
	     bool found = false;
	     boost::int128_type kn,k_n;
	     skipnode<K,T> *p_n = nullptr;

	     int pos = -1;
	     for(int i=0;i<nodes.size();i++)
	     {
		    kn = nodes[i]->key_nlink.load();
		    K key_n = (K)(kn >> 64);
		    if(key_n == k)
		    {
			 pos = i; break;
		    }
	     }

	     if(pos==-1 || nodes.size() < 2)
	     {
		  return false;
	     }

             assert (nodes.size() >= 2);	     
	     p_n = n->bottom.load();
	     skipnode<K,T> *n1 = nodes[pos];
	     skipnode<K,T> *n2 = nodes[pos+1];
	     n1->unsetMarkNode();
	     K key1, key2;
	     kn = n1->key_nlink.load();
	     key1 = (K)(kn >> 64);
	     skipnode<K,T> *n1_n = (skipnode<K,T>*)kn;
	     kn = n2->key_nlink.load();
	     key2 = (K)(kn >> 64); 
	     skipnode<K,T> *n2_n = (skipnode<K,T>*)kn;

	     std::vector<skipnode<K,T>*> n1_nodes,n2_nodes;

	     p_n = n1->bottom.load();
	     for(;;)
	     {
		while(!p_n->isFullyLinked() && !p_n->isMarked());
		p_n->node_lock.lock();
		kn = p_n->key_nlink.load();
		K key_n = (K)(kn >> 64);
		if(key_n==k && !p_n->isMarked()) p_n->setMarkNode();
		n1_nodes.push_back(p_n);
		if(key_n == key1 || p_n->isBottomNode()) break;
		p_n = (skipnode<K,T>*)kn;
	     }
	    
	     p_n = n2->bottom.load();

	     for(;;)
	     {
		while(!p_n->isFullyLinked() && !p_n->isMarked());
		p_n->node_lock.lock();
		kn = p_n->key_nlink.load();
		K key_n = (K)(kn >> 64);
	        n2_nodes.push_back(p_n);
		if(key_n == key2 ||p_n->isBottomNode()) break;
		p_n = (skipnode<K,T>*)kn;
	     }	
	
	     int numnodes = n1_nodes.size();
	     if(numnodes > 2 && !n1_nodes[numnodes-2]->isMarked())
             {
		kn = n1_nodes[numnodes-2]->key_nlink.load();
		kn = kn >> 64; kn = kn << 64;
		kn = kn | (boost::int64_t)n1_n;
		n1->key_nlink.store(kn);
		n2->bottom.store(n1_nodes[numnodes-1]);
             }
             else
             {
		 kn = key2; kn = kn << 64;
		 kn = kn | (boost::int64_t)n2_n;
		 n2->setFlags(0); n2->setMarkNode();
		 n1->key_nlink.store(kn);
              }
		
	      for(int i=0;i<n1_nodes.size();i++) n1_nodes[i]->node_lock.unlock();
	      for(int i=0;i<n2_nodes.size();i++) n2_nodes[i]->node_lock.unlock();
	
	      return true;
	  }

	  bool merge_nodes(skipnode<K,T> *n1, skipnode<K,T> *n2,bool first_node,std::vector<skipnode<K,T>*> &n1_nodes, std::vector<skipnode<K,T>*> &n2_nodes)
	  {

	     bool found = false;
	     skipnode<K,T> *p_n = nullptr;
	     boost::int128_type kn, k_n;
	     K key1, key2;
		
	     kn = n1->key_nlink.load();
	     key1 = (K)(kn >> 64);
	     skipnode<K,T> *n1_n = (skipnode<K,T>*)kn;
	     k_n = n2->key_nlink.load();
	     skipnode<K,T>* n2_n = (skipnode<K,T>*)k_n;
	     key2 = (K)(k_n >> 64);
	     bool n2_d = false;
	     if(first_node)
	     {
		    assert (n1_nodes.size() > 0 && n2_nodes.size() > 0);

		     if(n1_nodes.size() < 3)
                    {
                        if(n2_nodes.size()<3)
                        {
                           kn = (boost::int128_type)key2;
			   kn = kn << 64;kn = kn | (boost::int64_t)n2_n;
			   n2->setFlags(0); n2->setMarkNode();
			   n1->key_nlink.store(kn);
			   n2_d = true;
                        }
                        else
                        {
                          if(n1_nodes.size()==1)
                          {
                             if(n2_nodes.size() >= 4 && !n2_nodes[1]->isMarked())
                             {
				kn = n2_nodes[1]->key_nlink.load();
				kn = kn >> 64; kn = kn << 64;
				kn = kn | (boost::int64_t)n1_n;
                                n1->key_nlink.store(kn);
                                n2->bottom.store(n2_nodes[2]);
                             }
                             else
                             {
				kn = (boost::int128_type)key2;
				kn = kn << 64; kn = kn | (boost::int64_t)n2_n;
				n2->setFlags(0); n2->setMarkNode();
                                n1->key_nlink.store(kn);
				n2_d = true;
                             }
                          }
                          else 
                          {
			      if(!n2_nodes[0]->isMarked())
			      {
			        kn = n2_nodes[0]->key_nlink.load();
			        kn = kn >> 64; kn = kn << 64;
			        kn = kn | (boost::int64_t)n1_n;
			        n1->key_nlink.store(kn);
                                n2->bottom.store(n2_nodes[1]);
			      }
			      else
			      {
				kn = (boost::int128_type)key2;
				kn = kn << 64; kn = kn | (boost::int64_t)n2_n;
				n2->setFlags(0); n2->setMarkNode();
				n1->key_nlink.store(kn);
				n2_d = true;
			      }
                          }

			}
		    }

	     }
	     else
	     {

		assert (n1_nodes.size() > 0 && n2_nodes.size() > 0);

		 bool n2_d = false;
		 if(n2_nodes.size() < 3)
                 {
                     if(n1_nodes.size() < 3)
                     {
                            kn = (boost::int128_type)key2;
			    kn = kn << 64; kn = kn | (boost::int64_t)n2_n;
			    n2->setFlags(0); n2->setMarkNode();
			    n1->key_nlink.store(kn);
			    n2_d = true;
                     }
                     else
                     {
                           if(n2_nodes.size()==1)
                           {
                                if(n1_nodes.size()>=4 && !n1_nodes[n1_nodes.size()-3]->isMarked())
                                {
				   kn = n1_nodes[n1_nodes.size()-3]->key_nlink.load();
				   kn = kn >> 64; kn = kn << 64;
				   kn = kn | (boost::int64_t)n1_n;
				   n1->key_nlink.store(kn);
                                   n2->bottom.store(n1_nodes[n1_nodes.size()-2]);
                                }
                                else
                                {
				   kn = (boost::int128_type)key2;
				   kn = kn << 64; kn = kn | (boost::int64_t)n2_n;
				   n2->setFlags(0); n2->setMarkNode();
				   n1->key_nlink.store(kn);
				   n2_d = true;
                                }
                           }
                           else
                           {
			     if(!n1_nodes[n1_nodes.size()-2]->isMarked())
			     {
			       kn = n1_nodes[n1_nodes.size()-2]->key_nlink.load();
			       kn = kn >> 64; kn = kn << 64;
			       kn = kn | (boost::int64_t)n1_n;
			       n1->key_nlink.store(kn);
                               n2->bottom.store(n1_nodes[n1_nodes.size()-1]);
			     }
			     else
			     {
				kn = (boost::int128_type)key2;
				kn = kn << 64; kn = kn | (boost::int64_t)n2_n;
				n2->setFlags(0); n2->setMarkNode();
				n1->key_nlink.store(kn);
				n2_d = true;
			     }
                           }
                        }

                    }
	     }

	     return found;


	  }

	  bool Erase(skipnode<K,T> *n1, skipnode<K,T> *n2, K &k)
	  {
	     bool found = false;

	     bool invalid = false;
	     bool leaflevel = false;
	     skipnode<K,T> *nn = nullptr;
	     skipnode<K,T> *n_t = nullptr;
	     skipnode<K,T> *p_n = nullptr;
	     std::vector<skipnode<K,T>*> nodes;
	     std::vector<K> keys;
	     boost::int128_type kn,k_n;
	     std::vector<skipnode<K,T>*> n1_nodes, n2_nodes;
	     K key1, key2;
	     skipnode<K,T> *n1_next = nullptr, *n2_next=nullptr;

	     while(!n1->isFullyLinked() && !n1->isMarked());
	     if(n1 != head.load()) while(!n2->isFullyLinked() && !n2->isMarked());
	     n1->node_lock.lock();
	     if(n1 != head.load()) n2->node_lock.lock();
	     invalid = !n1->isFullyLinked();
	     if(n1 != head.load() && !invalid) 
	     {
		 invalid = !n2->isFullyLinked();
		 if(!invalid)
		 {
		   kn = n1->key_nlink.load();
		   K key_n = (K)(kn >> 64);
		   invalid = (key_n != k && n1->isMarked());
		   k_n = n2->key_nlink.load();
		   key_n = (K)(k_n >> 64);
		   invalid = (key_n != k && n2->isMarked());
		   nn = (skipnode<K,T>*)kn;
		   if(n2!=nn) invalid = true;
		 }
	     }
	     if(n1->isBottomNode() || n1->isTailNode() || n2->isBottomNode() || n2->isTailNode()) invalid = true;

	     if(!invalid)
	     {
		if(n1==head.load())
		{
		   p_n = n1->bottom.load();
		   kn = n1->key_nlink.load();
		   key1 = (K)(kn >> 64);
		   K key_n;

		   for(;;)
		   {
		     while(!p_n->isFullyLinked() && !p_n->isMarked());
		     p_n->node_lock.lock();
		     k_n = p_n->key_nlink.load();
		     key_n = (K)(k_n >> 64);
		     n1_nodes.push_back(p_n);
		     if(key_n == key1 || p_n->isBottomNode()) break;
		     p_n = (skipnode<K,T>*)k_n;
		   }

		   if(n1_nodes.size() > 1 && !n1_nodes[0]->isBottomNode())
		   {
			bool d = drop_key(n1,k,n1_nodes);

			leaflevel = n1_nodes[0]->bottom.load()->isBottomNode();
			if(!leaflevel)
			{
			   int pos = -1;
			   for(int i=0;i<n1_nodes.size();i++)
			   {
				boost::int128_type k_nn = n1_nodes[i]->key_nlink.load();
				K keynn = (K)(k_nn >> 64);
				if(k <= keynn)
				{
			 	   pos = i; break;
				}
			   }
			   if(pos!=-1) 
			   {
				if(pos==0)
				{
				   n1_next = n1_nodes[0]; n2_next = n1_nodes[1];
				}
				else
				{
				    n1_next = n1_nodes[pos-1]; n2_next = n1_nodes[pos];
				}
			    }

			}
		   }

		}	
		else
		{
		   assert (n1 != n2);

		   p_n = n1->bottom.load();
		   kn = n1->key_nlink.load();
		   key1 = (K)(kn >> 64);
		   K key_n;

		   //std::cout <<" n1 = "<<n1<<" n2 = "<<n2<<std::endl;

		   for(;;)
		   {
			while(!p_n->isFullyLinked() && !p_n->isMarked());
			p_n->node_lock.lock();
			k_n = p_n->key_nlink.load();
			key_n = (K)(k_n >> 64);
			//if(key1 == 1979802) std::cout <<" n1 = "<<n1<<" key1 = "<<key1<<" key_n = "<<key_n<<" head = "<<head.load()<<std::endl;
			n1_nodes.push_back(p_n);
			if(key_n == key1 || p_n->isBottomNode()) break;
			p_n = (skipnode<K,T>*)k_n;
		   }

		   p_n = n2->bottom.load();
		   kn = n2->key_nlink.load();
		   key2 = (K)(kn >> 64);

		   for(;;)
		   {
		      while(!p_n->isFullyLinked() && !p_n->isMarked());
		      p_n->node_lock.lock();
		      k_n = p_n->key_nlink.load();
		      key_n = (K)(k_n >> 64);
		      n2_nodes.push_back(p_n);
		      if(key_n == key2 || p_n->isBottomNode()) break;
		      p_n = (skipnode<K,T>*)k_n;
		   }

		   if(!n1_nodes[0]->isBottomNode())
		   {
		     bool first_node = false;
		     if(k <= key1) first_node = true;

		     bool d = merge_nodes(n1,n2,first_node,n1_nodes,n2_nodes);
		     kn = n1->key_nlink.load();
		     key1 = (K)(kn >> 64);
		     if(!n2->isMarked()) 
		     {
		       kn = n2->key_nlink.load();
		       key2 = (K)(kn >> 64);
		     }
		     skipnode<K,T> *n = (k <= key1) ? n1 : n2;
		     std::vector<skipnode<K,T>*> nodes_n;
		     nodes_n.assign(n1_nodes.begin(),n1_nodes.end());
		     for(int i=0;i<n2_nodes.size();i++) nodes_n.push_back(n2_nodes[i]); 

		     d = drop_key(n,k,nodes_n);
		     if(!n1_nodes[0]->isBottomNode() && n1_nodes[0]->bottom.load()->isBottomNode()) leaflevel = true;

		     if(!leaflevel)
		     {
			int pos = -1;
			int n1_pos = -1;
			for(int i=0;i<nodes_n.size();i++)
			{
			   boost::int128_type k_nn = nodes_n[i]->key_nlink.load();
			   K key_nn = (K)(k_nn >> 64);
			   if(k <= key_nn)
			   {
				pos = i; break;
			   }
			   if(key_nn == key1) n1_pos = i;
			}
		        
			if(pos==0 || pos-n1_pos==1)
			{
			    n1_next = nodes_n[pos]; n2_next = nodes_n[pos+1];
			}
			else 
			{
			    n1_next = nodes_n[pos-1]; n2_next = nodes_n[pos];
			}
		        	
		     }
		   }

		}

		for(int i=0;i<n1_nodes.size();i++) n1_nodes[i]->node_lock.unlock();
		for(int i=0;i<n2_nodes.size();i++) n2_nodes[i]->node_lock.unlock();

	     }
	     if(n1 != head.load()) n2->node_lock.unlock();
	     n1->node_lock.unlock();


	     if(n1_next != nullptr && n2_next != nullptr)
		     found = Erase(n1_next,n2_next,k);

	     return found;
	  }

	  bool EraseData(K &k)
	  {
		skipnode<K,T> *n = head.load();
		bool valid = false;
		bool b = false;

		//std::cout <<" Erase k = "<<k<<std::endl;
		b = Erase(n,n,k);
		//std::cout <<" Erase k end = "<<k<<std::endl;
		

		if(!b)
		{
		   DecreaseDepth();
		}

		return false;	
	  }

	  bool Insert(skipnode<K,T> *n,K &k, T& data)
	  {
		bool found = false;
		bool invalid = false;
                std::vector<skipnode<K,T> *> nodes;
		std::vector<K> keys;
		skipnode<K,T> *p_n = nullptr;
		bool leaf_level = false;
		bool next_node = false;
		skipnode<K,T> *nn = nullptr;

		while(!n->isFullyLinked() && !n->isMarked());
		invalid = n->isBottomNode() || n->isTailNode() || n->isMarked();
                skipnode<K,T> *b = n->bottom.load();
		skipnode<K,T> *bb = b->bottom.load();
		leaf_level = !b->isBottomNode() && bb->isBottomNode();

		if(invalid) return false;


		bool d = false;
		if(leaf_level)
		{
		   d = addleafnode(n,k,data);
		   d = add_node(n,k,data);
		   return d;
		}
		else
		{
		   bool f = add_node(n,k,data);
		}

		p_n = n->bottom.load();
		boost::int128_type kn = n->key_nlink.load();
		nn = (skipnode<K,T>*)kn;
		boost::int128_type k_n;
		K key = (K)(kn >> 64);

		if(k > key) 
		{
		   found = Insert(nn,k,data);
		   return found;
		}

		for(;;)
		{
		   while(!p_n->isFullyLinked() && !p_n->isMarked());
		   nodes.push_back(p_n);
		   k_n = p_n->key_nlink.load();
		   K key_n = (K)(k_n >> 64);
		   keys.push_back(key_n);
		   if(key_n == key || p_n->isBottomNode()) break;
		   p_n = (skipnode<K,T>*)k_n;
		}

		int pos = -1;
		for(int i=0;i<nodes.size();i++)
			if(k <= keys[i])
			{
			   pos = i; break;
			}

		if(pos != -1) 
		{
			found = Insert(nodes[pos],k,data);
			return found;
		}


		return found;
	  }

	  bool InsertData(K& k,T &data)
	  {
	     bool b = false;

	     skipnode<K,T> *n = head.load();
	     skipnode<K,T> *nn = nullptr;
	     bool valid = false;
		
	     //std::cout <<"Insert k = "<<k<<std::endl;
	        b = Insert(n,k,data);
		//std::cout <<" Insert = "<<k<<std::endl;

		if(!b)	 
		IncreaseDepth();

	     return b;
	  }

	  std::vector<K> & RangeSearch(K&,K&);

	  int Find(skipnode<K,T> *n, K &k)
	  {
		int found;
		bool invalid = false;
		bool leaflevel = false;
		bool next_node = false;
		bool empty = false;
		skipnode<K,T> *p_n = nullptr;
		skipnode<K,T> *n_n = nullptr;
		skipnode<K,T> *pn = nullptr;
		skipnode<K,T> *b = nullptr, *bb = nullptr;
		K key;
		std::vector<skipnode<K,T>*> nodes;
		std::vector<K> keys;
		bool fully_linked = true;

		while(!n->isFullyLinked() && !n->isMarked());
		invalid = n->isBottomNode() || n->isTailNode();
		if(n->isMarked()) invalid = true;
		if(!invalid)
		{
		  boost::int128_type n_k = n->key_nlink.load();
		  if(n==head.load())
		  {
		    n_n = (skipnode<K,T>*)n_k;
		    while(!n_n->isFullyLinked() && !n_n->isMarked());
	 	    if(!n_n->isTailNode()) invalid = true;	  
		  }
		}

		if(invalid) return 0;

		b = n->bottom.load();
		while(!b->isFullyLinked() && !b->isMarked());
		if(n==head.load() && b->isBottomNode()) empty = true;

		if(empty) return 1;

		bb = b->bottom.load();
		if(b != bb) 
		{
		    while(!bb->isFullyLinked() && !bb->isMarked());
		    if(!b->isBottomNode() && bb->isBottomNode()) leaflevel = true;
		}

		boost::int128_type nn = n->key_nlink.load();
		K key_n = (K)(nn >> 64);
		p_n = n->bottom.load();
		boost::int128_type kn;

		if(p_n != n) 
		{
		   for(;;)
		   {
		      while(!p_n->isFullyLinked() && !p_n->isMarked());
		      kn = p_n->key_nlink.load();
		      key = (K)(kn >> 64);
		      if(!p_n->isMarked())
		      {
			nodes.push_back(p_n); keys.push_back(key);
		      }
		      if(key == key_n || p_n->isBottomNode()) break;
		      p_n = (skipnode<K,T>*)kn;
		   }
		}

		int pos = -1;

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
		    
		    if(leaflevel) 
		    {
			found = (keys[pos] == k) ? 1 : 2;
			return found;
		    }
		    else
		    {
			found = Find(nodes[pos],k);
			return found;
		    }
		}
		else
		{
			return 0;
		}

		return found;
	  }
	  int FindData(K &k)
	  {
	      int b = 0;
              bool valid = false;	
	      skipnode<K,T> *n = head.load();
		
	      b = Find(n,k);
	      return b;
	  }
	  void check_list()
	  {
		skipnode<K,T> *t = head.load();
		//assert (t->nlink.load()->isTailNode());

		skipnode<K,T> *b = head.load();
		boost::int128_type k_n = b->key_nlink.load();
		K key = (K)(k_n >> 64);
		boost::int128_type kn = b->bottom.load()->key_nlink.load();
		K key_n = (K)(kn >> 64);
		std::cout <<" level = "<<0<<" b data = "<<key<<" l data = "<<key_n<<std::endl;
	
	        b = head.load()->bottom.load();
		std::cout <<" level = "<<1<<std::endl;

		while(!b->isTailNode() && !b->isBottomNode())
		{
			k_n = b->key_nlink.load();
			key = (K)(k_n >> 64);
			kn = b->bottom.load()->key_nlink.load();
			key_n = (K)(kn >> 64);
			std::cout <<" b data = "<<key<<" l data = "<<key_n<<std::endl;
			b = (skipnode<K,T>*)k_n;
		}

		b = head.load()->bottom.load()->bottom.load();

		std::cout <<" level = "<<2<<std::endl;
		while(!b->isTailNode() && !b->isBottomNode())
		{
			//if(b->key_ == b->bottom.load()->key_) assert(b->bottom.load()->isBottomNode());
			k_n = b->key_nlink.load();
			key = (K)(k_n >> 64);
			kn = b->bottom.load()->key_nlink.load();
			key_n = (K)(kn >> 64);
			std::cout <<" b data = "<<key<<" l data = "<<key_n<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;	
			b = (skipnode<K,T>*)k_n; 
		
		}

		std::cout <<" level = "<<3<<std::endl;
		
		b = head.load()->bottom.load()->bottom.load()->bottom.load();

		while(!b->isTailNode() && !b->isBottomNode())
		{
		   //if(b->key_==b->bottom.load()->key_) assert(b->bottom.load()->isBottomNode());
		   k_n = b->key_nlink.load();
		   key = (K)(k_n >> 64);
		   kn = b->bottom.load()->key_nlink.load();
		   key_n = (K)(kn >> 64);
		   std::cout <<" b data = "<<key<<" l data = "<<key_n<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;
		   b = (skipnode<K,T>*)k_n;
		}

		std::cout <<" level = "<<4<<std::endl;

		b = head.load()->bottom.load()->bottom.load()->bottom.load()->bottom.load();

		while(!b->isTailNode() && !b->isBottomNode())
		{
		   k_n = b->key_nlink.load();
		   key = (K)(k_n >> 64);
		   kn = b->bottom.load()->key_nlink.load();
		   key_n = (K)(kn >> 64);
		   std::cout <<" b data = "<<key<<" l data = "<<key_n<<" bn = "<<b->bottom.load()->isBottomNode()<<std::endl;
		   b = (skipnode<K,T>*)k_n;
		}


	  }

};


#include "Skiplist.cpp"

#endif
