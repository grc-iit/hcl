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

	  bool Insert(boost::shared_lock<boost::upgrade_mutex> &lk1, skipnode<K,T> *n, K & k, T&data)
	  {
		 bool found = false;

		 K p = n->key_;

		 if(n->isBottomNode() || n->isTailNode()) return false;

		 if(n!=head.load() && n->bottom.load()->isBottomNode()) return false;

		 if(n==head.load() && !ValidHead()) return false;

		 if((k > n->key_))
		 {
		     skipnode<K,T> *n1 = n->nlink.load();
		     boost::shared_lock<boost::upgrade_mutex> lk2(n1->node_lock);
		     if(n==head.load() && !ValidHead()) return false;
		     lk1.unlock(); lk1.release();
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
			  K c = n->key_;
			  if(n==head && !ValidHead()) return false;
		          skipnode<K,T> *b = n->bottom.load();
			  skipnode<K,T> *t = n->nlink.load();
			  if(k <= c)
			  {
			     int numnodes=0;

			     if(!b->isBottomNode())
			     {
				 skipnode<K,T> *p_n = b;

				 while(true)
				 {
				    numnodes++;
				    if(p_n->key_ == n->key_) break;
				    p_n = p_n->nlink.load();
				 }
			     }
			     else numnodes=1;

			 boost::upgrade_lock<boost::upgrade_mutex> *array[numnodes];

			 skipnode<K,T> *p_n = b;
			 for(int i=0;i<numnodes;i++) 
			 {
			    array[i] = new boost::upgrade_lock<boost::upgrade_mutex> (p_n->node_lock);
			    p_n = p_n->nlink.load();
			 }
			 
		          if(b->isBottomNode()){ b->key_ = k; b->data_ = data;}
			     
			  bool f = add_node(n,k,data);

			  if(k <= n->key_ && !b->isBottomNode() && b->bottom.load()->isBottomNode())
			  {
				//std::cout <<" n = "<<n<<" n data = "<<n->key_<<" head = "<<head.load()<<" b data = "<<b->key_<<" add leaf node data = "<<k<<std::endl;
				found = addleafnode(n,k,data);
			        //if(!found) std::cout <<" n = "<<n<<" head = "<<head.load()<<" node not added data = "<<k<<std::endl;
			  }
			  p = n->key_;
			  
			  for(int i=0;i<numnodes;i++)
			  {
				  assert (array[i]->owns_lock());
				  delete array[i];
			  }
			  if(found) return true;
			 }

			 boost::shared_lock<boost::upgrade_mutex> lk3(std::move(lk2));
			 assert(lk3.owns_lock()==true);
			 assert(lk2.owns_lock()==false);
			 if(n==head.load() && !ValidHead()) return false;
			 b = n->bottom.load();
			 if(isLeafNode(b)) found = Insert(lk3,n,k,data);
			 else
			 {
			    boost::shared_lock<boost::upgrade_mutex> lk4(b->node_lock);
			    lk3.unlock();lk3.release();
			    found = Insert(lk4,b,k,data);
			 }
			 break;
		      }
		    }

		 }

		  return found;
	  }
	  bool InsertData(K& k,T &data)
	  {
	     bool b = false;

	     {
	     skipnode<K,T> *n = head.load();
	     boost::shared_lock<boost::upgrade_mutex> lk0(n->node_lock);
	       if(ValidHead())
	       {
	        b = Insert(lk0,n,k,data);
	       }
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
		    n->bottom.store(n->bottom.load()->bottom.load());
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

	  bool drop_key(skipnode<K,T> *n,K &k)
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
		   int numnodes = nodes[pos]->bottom.load()->isBottomNode() ? 0 : getnumnodes(nodes[pos]);

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
		   }

		}
	
		if(pos != -1) return true;
		else return false;
	
	  }

	  bool merge_borrow_nodes(skipnode<K,T>*n1, skipnode<K,T> *n2)
	  {
		std::vector<skipnode<K,T>*> n1nodes, n2nodes;

		skipnode<K,T> *t = n1->bottom.load();

		if(n1==n2 && !n1->nlink.load()->isTailNode())
		{
		    while(true)
		    {  
			  n1nodes.push_back(t);
			  if(t->key_==n1->key_) break;
			  t = t->nlink.load();
		    }

		    t = n1->nlink.load()->bottom.load();

		    while(true)
		    {
			n2nodes.push_back(t);
			if(t->key_ == n1->nlink.load()->key_) break;
			t = t->nlink.load();
		    }

		    if(n1nodes.size() < 3)
		    {
			if(n2nodes.size()<3)
			{
			   n1->key_ = n1->nlink.load()->key_;
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
		        if(t->key_ == n1->key_) break;
			t = t->nlink.load();
		    }

		    t = n2->bottom.load();

		    while(true)
		    {
			n2nodes.push_back(t);
	    		if(t->key_ == n2->key_) break;
			t = t->nlink.load();
		    }		

		    if(n2nodes.size() < 3)
		    {
			if(n1nodes.size() < 3)
			{
			    n1->key_ = n2->key_;
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

	  bool EraseSeq(skipnode<K,T> *n1,skipnode<K,T> *n2, K&k)
	  {
	       bool found = false;

	       if(n1->isBottomNode() || n1->isTailNode() || n2->isBottomNode() || n2->isTailNode()) return false;

	       if(!n1->bottom.load()->isBottomNode())
	       {
		  if(n1==n2)
		  {
			K key_p = n1->key_;
			bool dropped = false;
			if(n1==head.load())
			{
			    bool d = drop_key(n1,k);
			    dropped = d;

			    if(n1->bottom.load()->key_ == n1->key_)
			    {
				return false; 
			    }

			}
			else
			{	
			    bool d = drop_key(n1,k);
			    dropped = d;

			    d = merge_borrow_nodes(n1,n2);

			}

			std::vector<skipnode<K,T>*> nodes;

			skipnode<K,T> *t = n1->bottom.load();

			while(true)
			{
			   nodes.push_back(t);
			   if(t->key_ == n1->key_) break;		
			   t=t->nlink.load();
			}
			
			int pos = -1;

			for(int i=0;i<nodes.size();i++)
			{
				if(nodes[i]->key_ > k)
				{
				   pos = i;
				   break;
				}
			}

			
			//std::cout <<" n = "<<nodes[pos]->key_<<" k = "<<k<<" pos = "<<pos<<" n = "<<n1->key_<<std::endl;
			if(!nodes[pos]->bottom.load()->isBottomNode())
			{
			   if(pos==0) found = EraseSeq(nodes[pos],nodes[pos],k);
			   else found = EraseSeq(nodes[pos-1],nodes[pos],k);
			}
			else return dropped;

		  }
		  else 
		  {

			bool dropped = false;
			bool d = drop_key(n2,k);
			dropped = d;

			K n1_key = n1->key_;
			K n2_key = n2->key_;

			d = merge_borrow_nodes(n1,n2);
			
			if(n1->key_ > n1_key)
			{
			   std::vector<skipnode<K,T>*> nodes;
			   skipnode<K,T> *t = n1->bottom.load();

			   while(true)
			   {
			      nodes.push_back(t);
			      if(t->key_ == n1->key_) break;
			      t = t->nlink.load();
			   }

			   int pos = -1;
			   for(int i=0;i<nodes.size();i++)
			   {
				   if(nodes[i]->key_ > k)
				   {
					 pos = i; break;
				   }
			   }

			   if(!nodes[pos]->bottom.load()->isBottomNode())
			   {
			     if(pos==0) found = EraseSeq(nodes[pos],nodes[pos],k);
			     else found = EraseSeq(nodes[pos-1],nodes[pos],k);
			   }
			   else return dropped;

			}
			else
			{
			    std::vector<skipnode<K,T>*> nodes;
			    skipnode<K,T> *t = n2->bottom.load();

			    while(true)
			    {
				nodes.push_back(t);
				if(t->key_ == n2->key_) break;
				t = t->nlink.load();
			    }

			    int pos = -1;
			    for(int i=0;i<nodes.size();i++)
			    {
				    if(nodes[i]->key_ > k)
				    {
					 pos = i; break;
				    }
			    }

			    if(!nodes[pos]->bottom.load()->isBottomNode())
			    {
			       if(pos == 0) found = EraseSeq(nodes[pos],nodes[pos],k);
			       else found = EraseSeq(nodes[pos-1],nodes[pos],k);
			    }
			    else return dropped;

			}
			

		  }

	       }

		return found;

	  }


	  bool Erase(boost::upgrade_lock<boost::upgrade_mutex> &lk0, boost::upgrade_lock<boost::upgrade_mutex> &lk1, skipnode<K,T> *n1, skipnode<K,T> *n2, K& k)
	  {
		bool found = false;

		return found;
		
	  }

	  bool EraseData(K &k)
	  {
		skipnode<K,T> *n = head.load();
		bool b = false;
		/*boost::upgrade_lock<boost::upgrade_mutex> lk0(n->node_lock);
		if(ValidHead())
		{
		   b = Erase(lk0,lk0,n,n,k);
		}*/
		b = EraseSeq(n,n,k);

		DecreaseDepth();

		return b;
	  }

	  std::vector<K> & RangeSearch(K&,K&);

	  bool Find(boost::shared_lock<boost::upgrade_mutex> &lk0, skipnode<K,T> *n, K &k)
	  {
		bool found = false;

		if(n->isBottomNode() || n->isTailNode()) found = false;

		if(n==head && !ValidHead()) return false;

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
