#ifndef INCLUDE_HCL_SKIPLIST_NODE_H_
#define INCLUDE_HCL_SKIPLIST_NODE_H_

#include <algorithm>
#include <atomic>
#include <climits>
#include <cmath>
#include <memory>
#include <type_traits>
#include <vector>
#include <cassert>
#include <boost/atomic.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/cstdint.hpp>
#include <boost/config.hpp>


enum : uint32_t
{
   IS_HEAD_NODE = 1,
   MARKED_FOR_REMOVAL = 2,
   FULLY_LINKED = 4,
   IS_TAIL_NODE = 8,
   IS_BOTTOM_NODE = 16,
};

template<class K, class T>
class skipnode
{
   public:
   boost::upgrade_mutex node_lock;
   K key_;
   T data_;
   boost::atomic<boost::int128_type> key_nlink;
   std::atomic<uint32_t> flags_;
   std::atomic<skipnode<K,T>*> nlink;
   //std::atomic<struct skipnode<T>*> plink;
   std::atomic<skipnode<K,T>*> bottom;

   skipnode()
   {
	new (&key_) K();
	new (&data_) T();
	new (&node_lock) boost::upgrade_mutex();
	key_nlink.store(0);
	nlink.store(0);
	bottom.store(nullptr);
	flags_.store(0);
   }
   ~skipnode()
   {

   }

    uint32_t getFlags()
    {
       return flags_.load();
    }
    void setFlags(uint32_t flags)
    {
       flags_.store(flags);
    }
    void setKey(K &k)
    {
      key_ = k;
    }
    K& key()
    {
      return key_;
    }    
    void setData(T &d)
    {
      data_ = d;
    }
    T& data() 
    { 
       return data_; 
    }
    bool isHeadNode() 
    { 
       return flags_.load() & IS_HEAD_NODE; 
    }
    void setIsHeadNode() 
    { 
       setFlags(flags_.load() | IS_HEAD_NODE); 
    }
    bool isMarked()
    {
	return flags_.load() & MARKED_FOR_REMOVAL;
    }
    void setMarkNode()
    {
	setFlags(flags_.load() | MARKED_FOR_REMOVAL);
    }
    void unsetMarkNode()
    {
	uint32_t mask = UINT32_MAX;
	mask = mask << 2;
	mask = mask | (uint32_t)1;
	setFlags(flags_.load() & mask); 
    }
    void setFullyLinked()
    {
       setFlags(flags_.load() | FULLY_LINKED);
    }
    bool isFullyLinked()
    {
       return flags_.load() & FULLY_LINKED;
    }
    void unsetFullyLinked()
    {
	uint32_t mask = UINT32_MAX;
	mask = mask << 3;
	mask = mask | (uint32_t)3;
	setFlags(flags_.load() & mask);
    }
    bool isTailNode()
    {
      return flags_.load() & IS_TAIL_NODE;
    }
    void setIsTailNode()
    {
      setFlags(flags_.load() |IS_TAIL_NODE);
    }
    bool isBottomNode()
    {
      return flags_.load() & IS_BOTTOM_NODE;
    }
    void setIsBottomNode()
    {
      setFlags(flags_.load() |IS_BOTTOM_NODE);
    }
};

#endif
