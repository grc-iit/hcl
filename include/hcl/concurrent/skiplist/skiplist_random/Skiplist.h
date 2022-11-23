#pragma once

#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <type_traits>
#include <cassert>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include "Skiplist-inl.h"

template <typename T,typename Comp = std::less<T>,typename NodeAlloc = std::allocator<char>,int MAX_HEIGHT = 24>
class ConcurrentSkipList 
{
  //assert(MAX_HEIGHT >= 2 && MAX_HEIGHT < 64);
  //typedef boost::unique_lock<boost::mutex> ScopedLocker;
  typedef ConcurrentSkipList<T, Comp, NodeAlloc, MAX_HEIGHT> SkipListType;

 public:
  typedef SkipListNode<T> NodeType;
  typedef T value_type;
  typedef T key_type;

  typedef csl_iterator<value_type, NodeType> iterator;
  typedef csl_iterator<const value_type, NodeType> const_iterator;

  class Accessor;
  class Skipper;

  explicit ConcurrentSkipList(int height, const NodeAlloc& alloc)
      : recycler_(alloc),
        head_(recycler_.pop(height,true))//NodeType::create(recycler_.alloc(), height, value_type(), true)) 
  {
  }

  explicit ConcurrentSkipList(int height)
      : recycler_(),
        head_(recycler_.pop(height,true))//NodeType::create(recycler_.alloc(), height, value_type(), true)) 
  {
  }

  static Accessor create(int height, const NodeAlloc& alloc) 
  {
    return Accessor(createInstance(height, alloc));
  }

  static Accessor create(int height = 1) 
  {
    return Accessor(createInstance(height));
  }

  static std::shared_ptr<SkipListType> createInstance(int height, const NodeAlloc& alloc) 
  {
    return std::make_shared<ConcurrentSkipList>(height, alloc);
  }

  static std::shared_ptr<SkipListType> createInstance(int height = 1) 
  {
    return std::make_shared<ConcurrentSkipList>(height);
  }

  size_t size() const { return size_.load(std::memory_order_relaxed); }
  bool empty() const { return size() == 0; }

  ~ConcurrentSkipList() 
  {
    /*if (NodeType::template DestroyIsNoOp<NodeAlloc>::value) 
    {
      return;
    }*/
    for (NodeType* current = head_.load(std::memory_order_relaxed); current;) 
    {
      NodeType* tmp = current->skip(0);
      NodeType::destroy(recycler_.alloc(), current);
      current = tmp;
    }
  }

 private:
  static bool greater(const value_type& data, const NodeType* node) 
  {
    return node && Comp()(node->data(), data);
  }

  static bool less(const value_type& data, const NodeType* node) 
  {
    return (node == nullptr) || Comp()(data, node->data());
  }

  static int findInsertionPoint(NodeType* cur,int cur_layer,const value_type& data,NodeType* preds[],NodeType* succs[]) 
  {
    int foundLayer = -1;
    NodeType* pred = cur;
    NodeType* foundNode = nullptr;
    for (int layer = cur_layer; layer >= 0; --layer) 
    {
      NodeType* node = pred->skip(layer);
      while (greater(data, node)) 
      {
        pred = node;
        node = node->skip(layer);
      }
      if (foundLayer == -1 && !less(data, node)) 
      { 
        foundLayer = layer;
        foundNode = node;
      }
      preds[layer] = pred;

      succs[layer] = foundNode ? foundNode : node;
    }
    return foundLayer;
  }

  int height() const { return head_.load(std::memory_order_acquire)->height(); }

  int maxLayer() const { return height() - 1; }

  size_t incrementSize(int delta) 
  {
    return size_.fetch_add(delta, std::memory_order_relaxed) + delta;
  }

  NodeType* find(const value_type& data) 
  {
    auto ret = findNode(data);
    if (ret.second && !ret.first->markedForRemoval()) 
    {
      return ret.first;
    }
    return nullptr;
  }

  bool lockNodesForChange(int nodeHeight,bool guards[MAX_HEIGHT],NodeType* preds[MAX_HEIGHT],NodeType* succs[MAX_HEIGHT],bool adding = true) 
  {
    NodeType *pred, *succ, *prevPred = nullptr;
    bool valid = true;
    for (int layer = 0; valid && layer < nodeHeight; ++layer) 
    {
      pred = preds[layer];
      assert(pred != nullptr);
      succ = succs[layer];
      if (pred != prevPred) 
      {
        guards[layer] = pred->acquireGuard();
        prevPred = pred;
      }
      valid = !pred->markedForRemoval() && pred->skip(layer) == succ; 

      if (adding) 
      { 
        valid = valid && (succ == nullptr || !succ->markedForRemoval());
      }
    }

    return valid;
  }

  template <typename U>
  std::pair<NodeType*, size_t> addOrGetData(U&& data) 
  {
    NodeType *preds[MAX_HEIGHT], *succs[MAX_HEIGHT];
    NodeType* newNode;
    size_t newSize;
    while (true) 
    {
      int max_layer = 0;
      int layer = findInsertionPointGetMaxLayer(data, preds, succs, &max_layer);

      if (layer >= 0) 
      {
        NodeType* nodeFound = succs[layer];
        assert(nodeFound != nullptr);
        if (nodeFound->markedForRemoval()) 
	{
          continue; 
        }
        while (!nodeFound->fullyLinked()) 
	{
        }
        return std::make_pair(nodeFound, 0);
      }

      int nodeHeight = SkipListRandomHeight::instance()->getHeight(max_layer + 1);

      bool guards[MAX_HEIGHT];
      for(int i=0;i<MAX_HEIGHT;i++) guards[i] = false;
      if (!lockNodesForChange(nodeHeight, guards, preds, succs)) 
      {
	for(int i=0;i<MAX_HEIGHT;i++)
		if(guards[i]) preds[i]->releaseGuard();
        continue; 
      }

      newNode = recycler_.pop(nodeHeight,false);//NodeType::create(recycler_.alloc(), nodeHeight, std::forward<U>(data));
      newNode->storeData(std::forward<U>(data));
      for (int k = 0; k < nodeHeight; ++k) 
      {
        newNode->setSkip(k, succs[k]);
        preds[k]->setSkip(k, newNode);
      }

      newNode->setFullyLinked();
      newSize = incrementSize(1);
      for(int i=0;i<MAX_HEIGHT;i++) 
	      if(guards[i]) preds[i]->releaseGuard();
      break;
    }

    int hgt = height();
    size_t sizeLimit = SkipListRandomHeight::instance()->getSizeLimit(hgt);

    if (hgt < MAX_HEIGHT && newSize > sizeLimit) 
    {
      growHeight(hgt + 1);
    }
    assert(newSize > 0);
    return std::make_pair(newNode, newSize);
  }

  bool remove(const value_type& data) 
  {
    NodeType* nodeToDelete = nullptr;
    bool isMarked = false;
    int nodeHeight = 0;
    NodeType *preds[MAX_HEIGHT], *succs[MAX_HEIGHT];

    while (true) 
    {
      int max_layer = 0;
      int layer = findInsertionPointGetMaxLayer(data, preds, succs, &max_layer);
      if (!isMarked && (layer < 0 || !okToDelete(succs[layer], layer))) {
        return false;
      }

      if (!isMarked) 
      {
        nodeToDelete = succs[layer];
        nodeHeight = nodeToDelete->height();
        bool ng = nodeToDelete->acquireGuard();
        if (nodeToDelete->markedForRemoval()) 
	{
	  nodeToDelete->releaseGuard();
          return false;
        }
        nodeToDelete->setMarkedForRemoval();
	nodeToDelete->releaseGuard();
        isMarked = true;
      }

      bool guards[MAX_HEIGHT];
      for(int i=0;i<MAX_HEIGHT;i++) guards[i] = false;
      if (!lockNodesForChange(nodeHeight, guards, preds, succs, false)) 
      {
	for(int i=0;i<MAX_HEIGHT;i++)
		if(guards[i]) preds[i]->releaseGuard();
        continue; 
      }

      for (int k = nodeHeight - 1; k >= 0; --k) 
      {
        preds[k]->setSkip(k, nodeToDelete->skip(k));
      }

      incrementSize(-1);
      for(int i=0;i<MAX_HEIGHT;i++)
	      if(guards[i]) preds[i]->releaseGuard();
      break;
    }
    recycler_.push(nodeToDelete->height(),nodeToDelete);
    //recycle(nodeToDelete);
    return true;
  }

  const value_type* first() const 
  {
    auto node = head_.load(std::memory_order_acquire)->skip(0);
    return node ? &node->data() : nullptr;
  }

  const value_type* last() const 
  {
    NodeType* pred = head_.load(std::memory_order_acquire);
    NodeType* node = nullptr;
    for (int layer = maxLayer(); layer >= 0; --layer) 
    {
      do {
        node = pred->skip(layer);
        if (node) {
          pred = node;
        }
      } while (node != nullptr);
    }
    return pred == head_.load(std::memory_order_relaxed) ? nullptr
                                                         : &pred->data();
  }

  static bool okToDelete(NodeType* candidate, int layer) 
  {
    assert(candidate != nullptr);
    return candidate->fullyLinked() && candidate->maxLayer() == layer &&
        !candidate->markedForRemoval();
  }

  int findInsertionPointGetMaxLayer(const value_type& data,NodeType* preds[],NodeType* succs[],int* max_layer) const 
  {
    *max_layer = maxLayer();
    return findInsertionPoint(head_.load(std::memory_order_acquire), *max_layer, data, preds, succs);
  }

  std::pair<NodeType*, int> findNode(const value_type& data) const 
  {
    return findNodeDownRight(data);
  }

  std::pair<NodeType*, int> findNodeDownRight(const value_type& data) const 
  {
    NodeType* pred = head_.load(std::memory_order_acquire);
    int ht = pred->height();
    NodeType* node = nullptr;

    bool found = false;
    while (!found) 
    {
      for (; ht > 0 && less(data, node = pred->skip(ht - 1)); --ht) 
      {
      }
      if (ht == 0) 
      {
        return std::make_pair(node, 0); 
      }
      --ht;

      while (greater(data, node)) 
      {
        pred = node;
        node = node->skip(ht);
      }
      found = !less(data, node);
    }
    return std::make_pair(node, found);
  }

  std::pair<NodeType*, int> findNodeRightDown(const value_type& data) const 
  {
    NodeType* pred = head_.load(std::memory_order_acquire);
    NodeType* node = nullptr;
    auto top = maxLayer();
    int found = 0;
    for (int layer = top; !found && layer >= 0; --layer) 
    {
      node = pred->skip(layer);
      while (greater(data, node)) 
      {
        pred = node;
        node = node->skip(layer);
      }
      found = !less(data, node);
    }
    return std::make_pair(node, found);
  }

  NodeType* lower_bound(const value_type& data) const 
  {
    auto node = findNode(data).first;
    while (node != nullptr && node->markedForRemoval()) 
    {
      node = node->skip(0);
    }
    return node;
  }

  void growHeight(int height) 
  {
    NodeType* oldHead = head_.load(std::memory_order_acquire);
    if (oldHead->height() >= height) 
    {
      return;
    }

    NodeType* newHead = recycler_.pop(height,true);
        //NodeType::create(recycler_.alloc(), height, value_type(), true);

    { 
      bool g = oldHead->acquireGuard();
      newHead->copyHead(oldHead);
      NodeType* expected = oldHead;
      if (!head_.compare_exchange_strong(
              expected, newHead, std::memory_order_release)) 
      {
        NodeType::destroy(recycler_.alloc(), newHead);
	oldHead->releaseGuard();
        return;
      }
      oldHead->setMarkedForRemoval();
      oldHead->releaseGuard();
    }
    recycler_.push(oldHead->height(),oldHead);
    //recycle(oldHead);
  }

  void recycle(NodeType* node) { recycler_.add(node); }

  NodeRecycler<NodeType, NodeAlloc> recycler_;
  std::atomic<NodeType*> head_;
  std::atomic<size_t> size_{0};
};

template <typename T, typename Comp, typename NodeAlloc, int MAX_HEIGHT>
class ConcurrentSkipList<T, Comp, NodeAlloc, MAX_HEIGHT>::Accessor 
{
  typedef SkipListNode<T> NodeType;
  typedef ConcurrentSkipList<T, Comp, NodeAlloc, MAX_HEIGHT> SkipListType;

 private:
  SkipListType* sl_;
  std::shared_ptr<SkipListType> slHolder_;

 public:
  typedef T value_type;
  typedef T key_type;
  typedef T& reference;
  typedef T* pointer;
  typedef const T& const_reference;
  typedef const T* const_pointer;
  typedef size_t size_type;
  typedef Comp key_compare;
  typedef Comp value_compare;

  typedef typename SkipListType::iterator iterator;
  typedef typename SkipListType::const_iterator const_iterator;
  typedef typename SkipListType::Skipper Skipper;

  explicit Accessor(std::shared_ptr<ConcurrentSkipList> skip_list)
      : slHolder_(std::move(skip_list)) 
  {
    sl_ = slHolder_.get();
    assert(sl_ != nullptr);
    sl_->recycler_.addRef();
  }

  explicit Accessor(ConcurrentSkipList* skip_list) : sl_(skip_list) 
  {
    assert(sl_ != nullptr);
    sl_->recycler_.addRef();
  }

  Accessor(const Accessor& accessor)
      : sl_(accessor.sl_), slHolder_(accessor.slHolder_) 
  {
    sl_->recycler_.addRef();
  }

  Accessor& operator=(const Accessor& accessor) 
  {
    if (this != &accessor) {
      slHolder_ = accessor.slHolder_;
      sl_->recycler_.releaseRef();
      sl_ = accessor.sl_;
      sl_->recycler_.addRef();
    }
    return *this;
  }

  ~Accessor() 
  {
    sl_->recycler_.releaseRef(); 
  }

  bool empty() const { return sl_->size() == 0; }
  size_t size() const { return sl_->size(); }
  size_type max_size() const { return std::numeric_limits<size_type>::max(); }

  iterator find(const key_type& value) { return iterator(sl_->find(value)); }
  const_iterator find(const key_type& value) const {
    return iterator(sl_->find(value));
  }
  size_type count(const key_type& data) const { return contains(data); }

  iterator begin() const 
  {
    NodeType* head = sl_->head_.load(std::memory_order_acquire);
    return iterator(head->next());
  }
  iterator end() const { return iterator(nullptr); }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  template <typename U,typename =typename std::enable_if<std::is_convertible<U, T>::value>::type>
  std::pair<iterator, bool> insert(U&& data) 
  {
    auto ret = sl_->addOrGetData(std::forward<U>(data));
    return std::make_pair(iterator(ret.first), ret.second);
  }
  size_t erase(const key_type& data) { return remove(data); }

  iterator lower_bound(const key_type& data) const {
    return iterator(sl_->lower_bound(data));
  }

  size_t height() const { return sl_->height(); }

  const key_type* first() const { return sl_->first(); }
  const key_type* last() const { return sl_->last(); }

  bool pop_back() 
  {
    auto last = sl_->last();
    return last ? sl_->remove(*last) : false;
  }

  std::pair<key_type*, bool> addOrGetData(const key_type& data) 
  {
    auto ret = sl_->addOrGetData(data);
    return std::make_pair(&ret.first->data(), ret.second);
  }

  SkipListType* skiplist() const { return sl_; }

  bool contains(const key_type& data) const { return sl_->find(data); }
  bool add(const key_type& data) { return sl_->addOrGetData(data).second; }
  bool remove(const key_type& data) { return sl_->remove(data); }
};

template <class D, class V, class Tag>
class IteratorFacade {
 public:
  using value_type = V;
  using reference = value_type&;
  using pointer = value_type*;
  using difference_type = ssize_t;
  using iterator_category = Tag;

  friend bool operator==(D const& lhs, D const& rhs) { return equal(lhs, rhs); }

  friend bool operator!=(D const& lhs, D const& rhs) { return !(lhs == rhs); }

  V& operator*() const { return asDerivedConst().dereference(); }

  V* operator->() const { return std::addressof(operator*()); }

  D& operator++() {
    asDerived().increment();
    return asDerived();
  }

  D operator++(int) {
    auto ret = asDerived(); 
    asDerived().increment();
    return ret;
  }

    D& operator--() {
    asDerived().decrement();
    return asDerived();
  }

  D operator--(int) {
    auto ret = asDerived(); 
    asDerived().decrement();
    return ret;
  }

 private:
  D& asDerived() { return static_cast<D&>(*this); }

  D const& asDerivedConst() const { return static_cast<D const&>(*this); }

  static bool equal(D const& lhs, D const& rhs) { return lhs.equal(rhs); }
};



template <typename ValT, typename NodeT>
class csl_iterator : public IteratorFacade<
                                 csl_iterator<ValT, NodeT>,
                                 ValT,
                                 std::forward_iterator_tag> {
 public:
  typedef ValT value_type;
  typedef value_type& reference;
  typedef value_type* pointer;
  typedef ptrdiff_t difference_type;

  explicit csl_iterator(NodeT* node = nullptr) : node_(node) {}

  template <typename OtherVal, typename OtherNode>
  csl_iterator(
      const csl_iterator<OtherVal, OtherNode>& other,
      typename std::enable_if<
          std::is_convertible<OtherVal*, ValT*>::value>::type* = nullptr)
      : node_(other.node_) {}

  size_t nodeSize() const 
  {
    return node_ == nullptr ? 0
                            : node_->height() * sizeof(NodeT*) + sizeof(*this);
  }

  bool good() const { return node_ != nullptr; }

 private:
  template <class, class>
  friend class csl_iterator;
  friend class IteratorFacade<csl_iterator, ValT, std::forward_iterator_tag>;

  void increment() { node_ = node_->next(); }
  bool equal(const csl_iterator& other) const { return node_ == other.node_; }
  value_type& dereference() const { return node_->data(); }

  NodeT* node_;
};

template <typename T, typename Comp, typename NodeAlloc, int MAX_HEIGHT>
class ConcurrentSkipList<T, Comp, NodeAlloc, MAX_HEIGHT>::Skipper {
  typedef SkipListNode<T> NodeType;
  typedef ConcurrentSkipList<T, Comp, NodeAlloc, MAX_HEIGHT> SkipListType;
  typedef typename SkipListType::Accessor Accessor;

 public:
  typedef T value_type;
  typedef T& reference;
  typedef T* pointer;
  typedef ptrdiff_t difference_type;

  Skipper(std::shared_ptr<SkipListType> skipList)
      : accessor_(std::move(skipList)) 
  {
    init();
  }

  Skipper(const Accessor& accessor) : accessor_(accessor) { init(); }

  void init() 
  {
    NodeType* head_node = head();
    headHeight_ = head_node->height();
    for (int i = 0; i < headHeight_; ++i) {
      preds_[i] = head_node;
      succs_[i] = head_node->skip(i);
    }
    int max_layer = maxLayer();
    for (int i = 0; i < max_layer; ++i) {
      hints_[i] = uint8_t(i + 1);
    }
    hints_[max_layer] = max_layer;
  }

  Skipper& operator++() 
  {
    preds_[0] = succs_[0];
    succs_[0] = preds_[0]->skip(0);
    int height = curHeight();
    for (int i = 1; i < height && preds_[0] == succs_[i]; ++i) 
    {
      preds_[i] = succs_[i];
      succs_[i] = preds_[i]->skip(i);
    }
    return *this;
  }

  Accessor& accessor() { return accessor_; }
  const Accessor& accessor() const { return accessor_; }

  bool good() const { return succs_[0] != nullptr; }

  int maxLayer() const { return headHeight_ - 1; }

  int curHeight() const 
  {
    return succs_[0] ? std::min(headHeight_, succs_[0]->height()) : 0;
  }

  const value_type& data() const 
  {
    assert(succs_[0] != nullptr);
    return succs_[0]->data();
  }

  value_type& operator*() const 
  {
    assert(succs_[0] != nullptr);
    return succs_[0]->data();
  }

  value_type* operator->() 
  {
    assert(succs_[0] != nullptr);
    return &succs_[0]->data();
  }

  bool to(const value_type& data) 
  {
    int layer = curHeight() - 1;
    if (layer < 0) 
    {
      return false; 
    }

    int lyr = hints_[layer];
    int max_layer = maxLayer();
    while (SkipListType::greater(data, succs_[lyr]) && lyr < max_layer) 
    {
      ++lyr;
    }
    hints_[layer] = lyr; 

    int foundLayer = SkipListType::findInsertionPoint(preds_[lyr], lyr, data, preds_, succs_);
    if (foundLayer < 0) 
    {
      return false;
    }

    assert(succs_[0] != nullptr);
    return !succs_[0]->markedForRemoval();
  }

 private:
  NodeType* head() const 
  {
    return accessor_.skiplist()->head_.load(std::memory_order_acquire);
  }

  Accessor accessor_;
  int headHeight_;
  NodeType *succs_[MAX_HEIGHT], *preds_[MAX_HEIGHT];
  uint8_t hints_[MAX_HEIGHT];
};

