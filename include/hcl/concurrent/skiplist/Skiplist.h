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
	     bool added = false;

	     if(n->bottom.load()->isBottomNode())
             {
                 skipnode<K,T> *t = pl->memory_pool_pop();
		 n->bottom.load()->key_ = k;
		 n->bottom.load()->data_ = data;
		     
		 skipnode<K,T> *b = pl->memory_pool_pop();
		 b->bottom.store(b);
		 b->nlink.store(b);
		 b->setIsBottomNode();
		 t->bottom.store(b);
                 t->nlink.store(n->nlink.load());
                 n->nlink.store(t);
		 t->setKey(n->key_);
                 t->setData(n->data_);
		 n->setKey(n->bottom.load()->nlink.load()->key_);
                 n->setData(n->bottom.load()->nlink.load()->data_);
		 added = true;
             }
	     else
	     {
		skipnode<K,T> *p = b;
		skipnode<K,T> *r = b;
		skipnode<K,T> *u = b;
		int nlinks = 0;

		while(true)
		{	
		   if(p->key_> n->key_) break;
		   if(p->key_ == n->key_ && nlinks >= 3)
		   {
			skipnode<K,T> *t = pl->memory_pool_pop();
			t->nlink.store(n->nlink.load());
			t->bottom.store(r);
			n->nlink.store(t);
			t->setKey(n->key_);
			t->setData(n->data_);
			n->setKey(u->key_);
			n->setData(u->data_);
			added = true;
		   }
		   u = r;
		   r = p;
		   p = p->nlink.load();
		   nlinks++;
	        }
	     }
	     return added; 
	  }
		
	  bool addleafnode(skipnode<K,T> *n,K &k, T& data)
	  {

		  skipnode<K,T> *b = n->bottom.load();
		  skipnode<K,T> *p = b;
		  skipnode<K,T> *bb = b;

		  while(true)
		  {
		     if(p->key_==k) return true;
		     if(k < p->key_)
		     {
			skipnode<K,T> *t = pl->memory_pool_pop();
			skipnode<K,T> *r = pl->memory_pool_pop();
			t->key_ = p->key_;
			t->data_ = p->data_;
			t->bottom.store(r);
			r->setIsBottomNode();
			r->nlink.store(r);
			r->bottom.store(r);
			t->nlink.store(p->nlink.load());
			p->key_ = k;
			p->data_ = data;
			p->nlink.store(t);
			return true;
		     }
		     if(p->key_==n->key_) break;
		     p = p->nlink.load();
		  }
		  return false;
	  }

	  bool IncreaseDepth()
	  {
            if(!head.load()->nlink.load()->isTailNode())
            {
              skipnode<K,T> *t = pl->memory_pool_pop();
              t->bottom.store(head.load()->bottom.load());
              t->nlink.store(head.load()->nlink.load());
              t->setKey(head.load()->key_);
	      t->setData(head.load()->data_);
              head.load()->bottom.store(t);
	      head.load()->nlink.store(tail.load());
	      head.load()->setKey(maxValue);
	      return true;
            }
	    else return false;
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

		if(n->isBottomNode() || n->isTailNode() || (n==head.load() && !ValidHead()) || (n==head.load() && n->key_ == n->bottom.load()->key_))
		{
		    n->node_lock.unlock(); return false; 
		}

		   
		std::vector<skipnode<K,T>*> nodes;

	        skipnode<K,T> *p_n = n->bottom.load();

		if(p_n != n)
		{
		  for(;;)
		  {
		   p_n->node_lock.lock();
		   nodes.push_back(p_n);
		   if(p_n->key_ == n->key_ || p_n->isBottomNode()) break;
		   p_n = p_n->nlink.load();
	         }
		}

		for(int i=0;i<nodes.size();i++)
		{
		  assert (nodes[i]!=n);
		}
		
		if(nodes.size()>1)
		for(int i=1;i<nodes.size();i++)
		{
		    assert (nodes[i-1] != nodes[i]);
		}

		bool d = false;
		if(!n->bottom.load()->isBottomNode()  && n->bottom.load()->bottom.load()->isBottomNode())
		{
		   d = addleafnode(n,k,data);
		}

		bool f = add_node(n,k,data);
		int pos = -1;

		for(int i=0;i<nodes.size();i++)
		{
		   if(k <= nodes[i]->key_)
		   {
			pos = i; break;
		   }
		}

		if(pos != -1 && !n->bottom.load()->isBottomNode() && !n->bottom.load()->bottom.load()->isBottomNode())
		{
		    for(int i=0;i<nodes.size();i++)
			    if(i != pos) nodes[i]->node_lock.unlock();
		    n->node_lock.unlock();
		    found = Insert(nodes[pos],k,data);
		}
		else 
		{
		    assert (n->bottom.load()->bottom.load()->isBottomNode());
		    for(int i=0;i<nodes.size();i++) nodes[i]->node_lock.unlock();
		    n->node_lock.unlock();
		    return d; 
		}

		return found;
	  }

	  bool InsertData(K& k,T &data)
	  {
	     bool b = false;

	     {
	     skipnode<K,T> *n = head.load();
	       n->node_lock.lock();
	       if(ValidHead())
	       {
	        b = Insert(n,k,data);
	       }
	       else
		  n->node_lock.unlock();
             }

	     {
		skipnode<K,T> *n = head.load();
		boost::upgrade_lock<boost::upgrade_mutex> lk0(n->node_lock);
		    bool d = IncreaseDepth();
	     }

	     return b;
	  }

	  bool DecreaseDepth()
	  {
		skipnode<K,T> *n = head.load();

		if(n->key_ == n->bottom.load()->key_ && !n->bottom.load()->isBottomNode())
		{
		    skipnode<K,T> *n1 = n->bottom.load()->bottom.load();
		    boost::upgrade_lock<boost::upgrade_mutex> lk0(n1->node_lock);
		    n->bottom.store(n1);
		    return true;
		
		}
		return false;
	  }

	  K getminkey(skipnode<K,T> *n)
	  {
		skipnode<K,T> *p = n->bottom.load();
		K minkey = UINT32_MAX;

		while(true)
		{
		    if(p->key_ == n->key_) break;
		    minkey = p->key_;
		    p = p->nlink.load();
		}
		return minkey;

	  }

	  int getnumnodes(skipnode<K,T> *n)
	  {
		skipnode<K,T> *p = n->bottom.load();

		int numnodes = 0;

		while(true)
		{
		    numnodes++;
		    if(p->key_ == n->key_) break;
		    p = p->nlink.load();
		}
		return numnodes;
	  }

	  bool drop_key(skipnode<K,T> *n,K &k,std::vector<skipnode<K,T>*> &deleted_nodes)
	  {
	        std::vector<skipnode<K,T> *> nodes;
	        skipnode<K,T> *t = n->bottom.load();
	        skipnode<K,T> *p_t = t;

		while(true)
		{
		   nodes.push_back(t);
		   if(t->key_ == n->key_) break;
		   p_t = t;
		   t = t->nlink.load();
		}
		
		int pos = -1;
		for(int i=0;i<nodes.size();i++)
		{
		   if(nodes[i]->key_ == k)
		   {
		     pos = i; break;
		   }
		}

		if(pos != -1)
		{
		   assert (pos >= 0 && pos<nodes.size()-1);

		   skipnode<K,T> *p_n = nodes[pos]->bottom.load();
		   std::vector<skipnode<K,T>*> n1_nodes, n2_nodes;

		   int numnodes=0;
		   if(p_n != nodes[pos])
		   for(;;)
		   {
		     p_n->node_lock.lock();
		     n1_nodes.push_back(p_n);
		     if(p_n->key_ == nodes[pos]->key_ || p_n->isBottomNode()) break;
		     p_n = p_n->nlink.load();
		   }

		   numnodes = n1_nodes.size();
		   p_n = nodes[pos+1]->bottom.load();

		   if(p_n != nodes[pos+1])
		   for(;;)
		   {
		     p_n->node_lock.lock();
		     n2_nodes.push_back(p_n);
		     if(p_n->key_ == nodes[pos+1]->key_ || p_n->isBottomNode()) break;
		     p_n = p_n->nlink.load();
		   }

		   if(numnodes > 2)
		   {
		      t = nodes[pos]->bottom.load();
		      p_t = t;

		      while(true)
		      {
			if(t->key_ == nodes[pos]->key_) break;
			p_t = t;
			t = t->nlink.load();
		      }

		      nodes[pos]->key_ = p_t->key_;
		      nodes[pos+1]->bottom.store(t);
		   }
		   else
		   {
			nodes[pos]->key_ = nodes[pos+1]->key_;
			nodes[pos]->nlink.store(nodes[pos+1]->nlink.load());
			deleted_nodes.push_back(nodes[pos+1]);

		   }

		   for(int i=0;i<n1_nodes.size();i++) n1_nodes[i]->node_lock.unlock();
		   for(int i=0;i<n2_nodes.size();i++) n2_nodes[i]->node_lock.unlock();

		}
	
		if(pos != -1) return true;
		else return false;
	
	  }

	  bool merge_borrow_nodes(skipnode<K,T>*n1, skipnode<K,T> *n2,std::vector<skipnode<K,T>*> &deleted_nodes)
	  {
		std::vector<skipnode<K,T>*> n1nodes, n2nodes;

		skipnode<K,T> *t = n1->bottom.load();

		if(n1==n2 && !n1->nlink.load()->isTailNode())
		{
		    while(true)
		    {  
			  n1nodes.push_back(t);
			  if(t->key_==n1->key_ || t->isBottomNode()) break;
			  t = t->nlink.load();
		    }

		    t = n1->nlink.load()->bottom.load();

		    while(true)
		    {
			n2nodes.push_back(t);
			if(t->key_ == n1->nlink.load()->key_ || t->isBottomNode()) break;
			t = t->nlink.load();
		    }

		    assert (n1nodes.size() > 0 && n2nodes.size() > 0);

		    if(n1nodes.size() < 3)
		    {
			if(n2nodes.size()<3)
			{
			   n1->key_ = n1->nlink.load()->key_;
			   deleted_nodes.push_back(n1->nlink.load());
			   n1->nlink.store(n1->nlink.load()->nlink.load());
			}
			else
			{
			  if(n1nodes.size()==1)
			  {
			     if(n2nodes.size() >= 4)
			     {
				n1->key_ = n2nodes[1]->key_;
				n1->nlink.load()->bottom.store(n2nodes[2]);
			     }
			     else
			     {
				n1->key_ = n1->nlink.load()->key_;
				deleted_nodes.push_back(n1->nlink.load());
				n1->nlink.store(n1->nlink.load()->nlink.load());
			     }
			  }
			  else
			  {
			      n1->key_ = n2nodes[0]->key_;
			      n1->nlink.load()->bottom.store(n2nodes[1]);
			  } 

			}
		    }

		}
		else
		{
		    t = n1->bottom.load();
		    
		    while(true)
		    {
			n1nodes.push_back(t);
		        if(t->key_ == n1->key_||t->isBottomNode()) break;
			t = t->nlink.load();
		    }

		    t = n2->bottom.load();

		    while(true)
		    {
			n2nodes.push_back(t);
	    		if(t->key_ == n2->key_||t->isBottomNode()) break;
			t = t->nlink.load();
		    }		

		    assert (n1nodes.size() > 0 && n2nodes.size() > 0);

		    if(n2nodes.size() < 3)
		    {
			if(n1nodes.size() < 3)
			{
			    n1->key_ = n2->key_;
			    deleted_nodes.push_back(n2);
    			    n1->nlink.store(n2->nlink.load());			    
			}
			else
			{
			   if(n2nodes.size()==1)
			   {
				if(n1nodes.size()>=4)
				{
				   n2->bottom.store(n1nodes[n1nodes.size()-2]);
				   n1->key_ = n1nodes[n1nodes.size()-3]->key_;
				}
				else
				{
				   n1->key_ = n2->key_;
				   deleted_nodes.push_back(n2);
				   n1->nlink.store(n2->nlink.load());
				}
			   }
			   else
			   {
			     n2->bottom.store(n1nodes[n1nodes.size()-1]); 
			     n1->key_ = n1nodes[n1nodes.size()-2]->key_;
			   }
			}

		    }


		}
		return true;

	  }

	  void acquire_locks(skipnode<K,T> *n1, skipnode<K,T> *n2,std::vector<skipnode<K,T>*> &n1_node_ptrs, std::vector<skipnode<K,T>*> &n2_node_ptrs)
	  {

		if(n1==n2)
		{
		   skipnode<K,T> *b = n1->bottom.load();
		   skipnode<K,T> *p_n = b;

		   if(b != n1)
		   for(;;)
		   {
		      p_n->node_lock.lock();
		      n1_node_ptrs.push_back(p_n);
		      if(p_n->key_ == n1->key_ || p_n->isBottomNode()) break;
		      p_n = p_n->nlink.load();
		   }

		   for(int i=0;i<n1_node_ptrs.size();i++)
		   {
		      assert (n1 != n1_node_ptrs[i]);
		   }
		   if(n1_node_ptrs.size() > 1)
		   for(int i=1;i<n1_node_ptrs.size();i++)
		   {
		      assert(n1_node_ptrs[i-1] != n1_node_ptrs[i]);
		   }
		   b = n1->nlink.load();

		   if(!b->isTailNode())
		   {
			p_n = b->bottom.load();
	
			if(p_n != b)
			for(;;)
			{
			   p_n->node_lock.lock();
			   n2_node_ptrs.push_back(p_n);
			   if(p_n->key_ == b->key_ || p_n->isBottomNode()) break;
			   p_n = p_n->nlink.load();
			}
		   }

		   for(int i=0;i<n2_node_ptrs.size();i++)
			   assert (b != n2_node_ptrs[i]);

		   if(n2_node_ptrs.size() > 1)
		     for(int i=1;i<n2_node_ptrs.size();i++)
			     assert (n2_node_ptrs[i-1] != n2_node_ptrs[i]);

		}
		else
		{
		   skipnode<K,T> *b = n1->bottom.load();
		   skipnode<K,T> *p_n = b;

		   if(b != n1)
		   for(;;)
		   {
		      p_n->node_lock.lock();
		      n1_node_ptrs.push_back(p_n);
		      if(p_n->key_ == n1->key_ || p_n->isBottomNode()) break;
		      p_n = p_n->nlink.load();
		   }

		   for(int i=0;i<n1_node_ptrs.size();i++)
			   assert (n1 != n1_node_ptrs[i]);

		   if(n1_node_ptrs.size() > 1)
			   for(int i=1;i<n1_node_ptrs.size();i++)
				   assert (n1_node_ptrs[i-1] != n1_node_ptrs[i]);

		   b = n2->bottom.load();
		   p_n = b;

		   if(b != n2)
		   for(;;)
		   {
		      p_n->node_lock.lock();
		      n2_node_ptrs.push_back(p_n);
		      if(p_n->key_ == n2->key_ || p_n->isBottomNode()) break;
		      p_n = p_n->nlink.load();
		   }

		   for(int i=0;i<n2_node_ptrs.size();i++)
			   assert (n2 != n2_node_ptrs[i]);

		   if(n2_node_ptrs.size() > 1)
			   for(int i=1;i<n2_node_ptrs.size();i++)
				   assert(n2_node_ptrs[i-1] != n2_node_ptrs[i]);

		}

	  }

	  void find_nodes(std::vector<skipnode<K,T>*> &n1_node_ptrs, std::vector<skipnode<K,T>*> &n2_node_ptrs, std::vector<skipnode<K,T>*> &nodes, int &pos1, int &pos2, std::vector<int> &npos1,std::vector<int> &npos2)
	  {

                   for(int i=0;i<n1_node_ptrs.size();i++)
                   {
                      if(n1_node_ptrs[i]==nodes[pos1])
                      {
                          npos1[0]=i;
                      }
                      if(n1_node_ptrs[i]==nodes[pos2])
                      {
                          npos1[1]=i;
                      }
                   }

		   int npos21 = -1, npos22 = -1;
                   for(int i=0;i<n2_node_ptrs.size();i++)
                   {
                       if(n2_node_ptrs[i]==nodes[pos1])
                       {
                          npos2[0] = i;
                       }
                       if(n2_node_ptrs[i]==nodes[pos2])
                       {
                          npos2[1] = i;
                       }
                   }


	  }

	  bool Erase(skipnode<K,T> *n1,skipnode<K,T> *n2, K&k)
	  {
	       bool found = false;

	       if(n1->isBottomNode() || n1->isTailNode() || n2->isBottomNode() || n2->isTailNode() || (n1==head.load() && !ValidHead())) 
	       {
		       n1->node_lock.unlock();
		       if(n1 != head.load()) 
		       {
			    n1->nlink.load()->node_lock.unlock();
		       }
		       return false;
	       }

	       if(n1->bottom.load()->isBottomNode())
	       {
		   n1->node_lock.unlock();
		  if(n1 != head.load())
		  {
		     n1->nlink.load()->node_lock.unlock();
		  }
	       } 

	       if(!n1->bottom.load()->isBottomNode())
	       {
		  std::vector<skipnode<K,T>*> n1_node_ptrs, n2_node_ptrs;

		  acquire_locks(n1,n2,n1_node_ptrs,n2_node_ptrs);

		  std::vector<skipnode<K,T>*> nodes;
		  skipnode<K,T> *nn = nullptr;
		  int pos = -1;
		  bool dropped = false;

		  if(n1==n2)
		  {
			K key_p = n1->key_;
			dropped = false;
			std::vector<skipnode<K,T>*> nodes_d;
			std::vector<skipnode<K,T>*> nodes_m;
			if(n1==head.load())
			{
		           
			    bool d = drop_key(n1,k,nodes_d);
			    dropped = d;

			    if(n1->bottom.load()->key_ == n1->key_)
			    {
				for(int i=0;i<n1_node_ptrs.size();i++) n1_node_ptrs[i]->node_lock.unlock();
				n1->node_lock.unlock();
				return false; 
			    }

			}
			else
			{
			
			    nn = n1->nlink.load();	
			    bool d = drop_key(n1,k,nodes_d);
			    dropped = d;

			    d = merge_borrow_nodes(n1,n2,nodes_m);

			}

			skipnode<K,T> *t = n1->bottom.load();

			while(true)
			{
			   nodes.push_back(t);
			   if(t->key_ == n1->key_) break;		
			   t=t->nlink.load();
			}
		
			for(int i=0;i<nodes.size();i++)
			{
		           if(nodes[i]->key_ > k)
			   {
			      pos = i;
			      break;
			   }
			}
		  }
		  else 
		  {

			std::vector<skipnode<K,T>*> nodes_d;
			std::vector<skipnode<K,T>*> nodes_m;

			dropped = false;
			bool d = drop_key(n2,k,nodes_d);
			dropped = d;

			K n1_key = n1->key_;
			K n2_key = n2->key_;

			d = merge_borrow_nodes(n1,n2,nodes_m);
			
			if(n1->key_ > n1_key)
			{
			   skipnode<K,T> *t = n1->bottom.load();

			   while(true)
			   {
			      nodes.push_back(t);
			      if(t->key_ == n1->key_) break;
			      t = t->nlink.load();
			   }
		
		           assert(nodes.size()>=2);	   
			   for(int i=0;i<nodes.size();i++)
			   {
				   if(nodes[i]->key_ > k)
				   {
					 pos = i; break;
				   }
			   }
			}
			else
			{
			    skipnode<K,T> *t = n2->bottom.load();

			    while(true)
			    {
				nodes.push_back(t);
				if(t->key_ == n2->key_) break;
				t = t->nlink.load();
			    }

			    assert(nodes.size()>=2);
			    for(int i=0;i<nodes.size();i++)
			    {
				    if(nodes[i]->key_ > k)
				    {
					 pos = i; break;
				    }
			    }
			}
		  }

		  if(pos != -1 && !nodes[pos]->bottom.load()->isBottomNode())
	          {
		     int pos1, pos2;
		     if(pos==0)
		     {
			pos1 = pos; pos2 = pos+1;
			assert (nodes.size() >= 2);
		     }
		     else
	             {
			pos1 = pos-1; pos2 = pos;
		     }

		     std::vector<int> npos1,npos2;
                     npos1.resize(2);npos2.resize(2);
                     std::fill(npos1.begin(),npos1.end(),-1);
                     std::fill(npos2.begin(),npos2.end(),-1);
                     find_nodes(n1_node_ptrs, n2_node_ptrs, nodes,pos1,pos2,npos1,npos2);

		     for(int i=0;i<n1_node_ptrs.size();i++)
			     if(i != npos1[0] && i != npos1[1]) n1_node_ptrs[i]->node_lock.unlock();
		     for(int i=0;i<n2_node_ptrs.size();i++)
			     if(i != npos2[0] && i != npos2[1]) n2_node_ptrs[i]->node_lock.unlock();

		     n1->node_lock.unlock(); 
		     if(n1 != head.load() && n1==n2) 
		     {
			  nn->node_lock.unlock();
		     }
		     else if(n1 != n2) n2->node_lock.unlock();
		     if(pos==0)
		     {
			found = Erase(nodes[pos],nodes[pos],k);
		     }
		     else found = Erase(nodes[pos-1],nodes[pos],k);

		}
		else 
		{
			for(int i=0;i<n1_node_ptrs.size();i++) n1_node_ptrs[i]->node_lock.unlock();
			for(int i=0;i<n2_node_ptrs.size();i++) n2_node_ptrs[i]->node_lock.unlock();
			n1->node_lock.unlock();
			if(n1 != head.load() && n1==n2) nn->node_lock.unlock();
			else if(n1 != n2) n2->node_lock.unlock();
			return dropped;
		}

	}
			
	return found;

    }


	  bool EraseData(K &k)
	  {
		skipnode<K,T> *n = head.load();
		bool b = false;
		n->node_lock.lock();
		if(ValidHead() && n->key_ != n->bottom.load()->key_)
		b = Erase(n,n,k);
		else n->node_lock.unlock();
		//if(lk0.owns_lock()) std::cout <<" k = "<<k<<" n = "<<n<<" owns lock"<<std::endl;


		n = head.load();
		boost::upgrade_lock<boost::upgrade_mutex> lk1(n->node_lock);
		boost::upgrade_lock<boost::upgrade_mutex> lk2(n->bottom.load()->node_lock);
          	DecreaseDepth();

		return b;
	  }

	  std::vector<K> & RangeSearch(K&,K&);

	  bool Find(skipnode<K,T> *n, K &k)
	  {
		bool found = false;

		if(n->isBottomNode() || n->isTailNode()) 
		{
			n->node_lock.unlock();
			found = false;
		}

		if(n==head && !ValidHead()) 
		{
			n->node_lock.unlock();
			return false;
		}

		std::vector<skipnode<K,T>*> nodes;

		skipnode<K,T> *p_n = n->bottom.load();
	
		int pos = -1;

		if(p_n != n)
		{
		    for(;;)
		    {
			p_n->node_lock.lock();
			nodes.push_back(p_n);
			if(p_n->key_==n->key_ || p_n->isBottomNode()) break;
			p_n = p_n->nlink.load();
		    }
		}

		for(int i=0;i<nodes.size();i++)
		{
			if(k <= nodes[i]->key_)
			{
			   pos = i; break;
			}
		}

		if(pos!=-1)
		{
		    if(nodes[pos]->bottom.load()->isBottomNode())
		    {

			if(nodes[pos]->key_ == k) 
			{
				found = true;
			}
			else found = false;
			for(int i=0;i<nodes.size();i++)
			  nodes[i]->node_lock.unlock();
			n->node_lock.unlock();
			return found;

		    }
		    else
		    {
			for(int i=0;i<nodes.size();i++)
			{
				if(i != pos) nodes[i]->node_lock.unlock();
			}
			n->node_lock.unlock();
			found = Find(nodes[pos],k);
		    }
		}
		else
		{
			for(int i=0;i<nodes.size();i++)
				nodes[i]->node_lock.unlock();
			n->node_lock.unlock();
			return false;
		}

		return found;
	  }
	  bool FindData(K &k)
	  {
	      bool b = false;
	
	      skipnode<K,T> *n = head.load();
	      n->node_lock.lock();
	      if(ValidHead())
		b = Find(n,k);
	      else n->node_lock.unlock();	      

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
