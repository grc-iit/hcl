#include "Skiplist.h"

#include <atomic>
#include <memory>
#include <set>
#include <system_error>
#include <thread>
#include <vector>

#include "Skiplist.h"

#define num_threads 12
typedef SkipListNode<int> SkipListNodeType;
typedef ConcurrentSkipList<int> SkipListType;
typedef SkipListType::Accessor SkipListAccessor;
static const int kHeadHeight = 2;
static const int kMaxValue = 5000;

//using std::vector;
/*
typedef int ValueType;
typedef SkipListNode<ValueType> SkipListNodeType;
typedef ConcurrentSkipList<ValueType> SkipListType;
typedef SkipListType::Accessor SkipListAccessor;
typedef vector<ValueType> VectorType;
typedef std::set<ValueType> SetType;

static const int kHeadHeight = 2;
static const int kMaxValue = 5000;

static void randomAdding(
    int size,
    SkipListAccessor skipList,
    SetType* verifier,
    int maxValue = kMaxValue) {
  for (int i = 0; i < size; ++i) {
    int32_t r = rand() % maxValue;
    verifier->insert(r);
    skipList.add(r);
  }
}

static void randomRemoval(
    int size,
    SkipListAccessor skipList,
    SetType* verifier,
    int maxValue = kMaxValue) {
  for (int i = 0; i < size; ++i) {
    int32_t r = rand() % maxValue;
    verifier->insert(r);
    skipList.remove(r);
  }
}

static void sumAllValues(SkipListAccessor skipList, int64_t* sum) {
  *sum = 0;
  FOR_EACH (it, skipList) { *sum += *it; }
  //VLOG(20) << "sum = " << sum;
}

static void concurrentSkip(const vector<ValueType>* values, SkipListAccessor skipList) 
{
  int64_t sum = 0;
  SkipListAccessor::Skipper skipper(skipList);
  FOR_EACH (it, *values) 
  {
    if (skipper.to(*it)) 
    {
      sum += *it;
    }
  }
  //VLOG(20) << "sum = " << sum;
}

bool verifyEqual(SkipListAccessor skipList, const SetType& verifier) 
{
  EXPECT_EQ(verifier.size(), skipList.size());
  FOR_EACH (it, verifier) 
  {
    CHECK(skipList.contains(*it)) << *it;
    SkipListType::const_iterator iter = skipList.find(*it);
    CHECK(iter != skipList.end());
    EXPECT_EQ(*iter, *it);
  }
  EXPECT_TRUE(std::equal(verifier.begin(), verifier.end(), skipList.begin()));
  return true;
}

TEST(ConcurrentSkipList, SequentialAccess) 
{
  {
    LOG(INFO) << "nodetype size=" << sizeof(SkipListNodeType);

    auto skipList(SkipListType::create(kHeadHeight));
    EXPECT_TRUE(skipList.first() == nullptr);
    EXPECT_TRUE(skipList.last() == nullptr);

    skipList.add(3);
    EXPECT_TRUE(skipList.contains(3));
    EXPECT_FALSE(skipList.contains(2));
    EXPECT_EQ(3, *skipList.first());
    EXPECT_EQ(3, *skipList.last());

    EXPECT_EQ(3, *skipList.find(3));
    EXPECT_FALSE(skipList.find(3) == skipList.end());
    EXPECT_TRUE(skipList.find(2) == skipList.end());

    {
      SkipListAccessor::Skipper skipper(skipList);
      skipper.to(3);
      CHECK_EQ(3, *skipper);
    }

    skipList.add(2);
    EXPECT_EQ(2, *skipList.first());
    EXPECT_EQ(3, *skipList.last());
    skipList.add(5);
    EXPECT_EQ(5, *skipList.last());
    skipList.add(3);
    EXPECT_EQ(5, *skipList.last());
    auto ret = skipList.insert(9);
    EXPECT_EQ(9, *ret.first);
    EXPECT_TRUE(ret.second);

    ret = skipList.insert(5);
    EXPECT_EQ(5, *ret.first);
    EXPECT_FALSE(ret.second);

    EXPECT_EQ(2, *skipList.first());
    EXPECT_EQ(9, *skipList.last());
    EXPECT_TRUE(skipList.pop_back());
    EXPECT_EQ(5, *skipList.last());
    EXPECT_TRUE(skipList.pop_back());
    EXPECT_EQ(3, *skipList.last());

    skipList.add(9);
    skipList.add(5);

    CHECK(skipList.contains(2));
    CHECK(skipList.contains(3));
    CHECK(skipList.contains(5));
    CHECK(skipList.contains(9));
    CHECK(!skipList.contains(4));

    auto it = skipList.lower_bound(5);
    EXPECT_EQ(5, *it);
    it = skipList.lower_bound(4);
    EXPECT_EQ(5, *it);
    it = skipList.lower_bound(9);
    EXPECT_EQ(9, *it);
    it = skipList.lower_bound(12);
    EXPECT_FALSE(it.good());

    it = skipList.begin();
    EXPECT_EQ(2, *it);

    SkipListAccessor::Skipper skipper(skipList);
    skipper.to(3);
    EXPECT_EQ(3, skipper.data());
    skipper.to(5);
    EXPECT_EQ(5, skipper.data());
    CHECK(!skipper.to(7));

    skipList.remove(5);
    skipList.remove(3);
    CHECK(skipper.to(9));
    EXPECT_EQ(9, skipper.data());

    CHECK(!skipList.contains(3));
    skipList.add(3);
    CHECK(skipList.contains(3));
    int pos = 0;
    for (auto entry : skipList) {
      LOG(INFO) << "pos= " << pos++ << " value= " << entry;
    }
  }

  {
    auto skipList(SkipListType::create(kHeadHeight));

    SetType verifier;
    randomAdding(10000, skipList, &verifier);
    verifyEqual(skipList, verifier);

    SkipListAccessor::Skipper skipper(skipList);
    int num_skips = 1000;
    for (int i = 0; i < num_skips; ++i) {
      int n = i * kMaxValue / num_skips;
      bool found = skipper.to(n);
      EXPECT_EQ(found, (verifier.find(n) != verifier.end()));
    }
  }
}

static std::string makeRandomeString(int len) {
  std::string s;
  for (int j = 0; j < len; j++) {
    s.push_back((rand() % 26) + 'A');
  }
  return s;
}

TEST(ConcurrentSkipList, TestStringType) {
  typedef folly::ConcurrentSkipList<std::string> SkipListT;
  std::shared_ptr<SkipListT> skip = SkipListT::createInstance();
  SkipListT::Accessor accessor(skip);
  {
    for (int i = 0; i < 100000; i++) {
      std::string s = makeRandomeString(7);
      accessor.insert(s);
    }
  }
  EXPECT_TRUE(std::is_sorted(accessor.begin(), accessor.end()));
}

struct UniquePtrComp {
  bool operator()(
      const std::unique_ptr<int>& x, const std::unique_ptr<int>& y) const {
    if (!x) {
      return false;
    }
    if (!y) {
      return true;
    }
    return *x < *y;
  }
};

TEST(ConcurrentSkipList, TestMovableData) {
  typedef folly::ConcurrentSkipList<std::unique_ptr<int>, UniquePtrComp>
      SkipListT;
  auto sl = SkipListT::createInstance();
  SkipListT::Accessor accessor(sl);

  static const int N = 10;
  for (int i = 0; i < N; ++i) {
    accessor.insert(std::make_unique<int>(i));
  }

  for (int i = 0; i < N; ++i) {
    EXPECT_TRUE(
        accessor.find(std::unique_ptr<int>(new int(i))) != accessor.end());
  }
  EXPECT_TRUE(
      accessor.find(std::unique_ptr<int>(new int(N))) == accessor.end());
}

TEST(ConcurrentSkipList, ConcurrentAdd) {
  int numThreads = 100;
  auto skipList(SkipListType::create(kHeadHeight));

  vector<std::thread> threads;
  vector<SetType> verifiers(numThreads);
  try {
    for (int i = 0; i < numThreads; ++i) {
      threads.push_back(std::thread(
          &randomAdding, 1000000, skipList, &verifiers[i], kMaxValue));
    }
  } catch (const std::system_error& e) {
    LOG(WARNING) << "Caught " << exceptionStr(e) << ": could only create "
                 << threads.size() << " threads out of " << numThreads;
  }
  for (size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }

  SetType all;
  FOR_EACH (s, verifiers) { all.insert(s->begin(), s->end()); }
  verifyEqual(skipList, all);
}

void testConcurrentRemoval(int numThreads, int maxValue) {
  auto skipList = SkipListType::create(kHeadHeight);
  for (int i = 0; i < maxValue; ++i) {
    skipList.add(i);
  }

  vector<std::thread> threads;
  vector<SetType> verifiers(numThreads);
  try {
    for (int i = 0; i < numThreads; ++i) {
      threads.push_back(
          std::thread(&randomRemoval, 100, skipList, &verifiers[i], maxValue));
    }
  } catch (const std::system_error& e) {
    LOG(WARNING) << "Caught " << exceptionStr(e) << ": could only create "
                 << threads.size() << " threads out of " << numThreads;
  }
  FOR_EACH (t, threads) { (*t).join(); }

  SetType all;
  FOR_EACH (s, verifiers) { all.insert(s->begin(), s->end()); }

  CHECK_EQ(maxValue, all.size() + skipList.size());
  for (int i = 0; i < maxValue; ++i) {
    if (all.find(i) != all.end()) {
      CHECK(!skipList.contains(i)) << i;
    } else {
      CHECK(skipList.contains(i)) << i;
    }
  }
}

TEST(ConcurrentSkipList, ConcurrentRemove) {
  for (int numThreads = 10; numThreads < 1000; numThreads += 100) {
    testConcurrentRemoval(numThreads, 100 * numThreads);
  }
}

static void testConcurrentAccess(
    int numInsertions, int numDeletions, int maxValue) {
  auto skipList = SkipListType::create(kHeadHeight);

  vector<SetType> verifiers(FLAGS_num_threads);
  vector<int64_t> sums(FLAGS_num_threads);
  vector<vector<ValueType>> skipValues(FLAGS_num_threads);

  for (int i = 0; i < FLAGS_num_threads; ++i) {
    for (int j = 0; j < numInsertions; ++j) {
      skipValues[i].push_back(rand() % (maxValue + 1));
    }
    std::sort(skipValues[i].begin(), skipValues[i].end());
  }

  vector<std::thread> threads;
  for (int i = 0; i < FLAGS_num_threads; ++i) {
    switch (i % 8) {
      case 0:
      case 1:
        threads.push_back(std::thread(
            randomAdding, numInsertions, skipList, &verifiers[i], maxValue));
        break;
      case 2:
        threads.push_back(std::thread(
            randomRemoval, numDeletions, skipList, &verifiers[i], maxValue));
        break;
      case 3:
        threads.push_back(
            std::thread(concurrentSkip, &skipValues[i], skipList));
        break;
      default:
        threads.push_back(std::thread(sumAllValues, skipList, &sums[i]));
        break;
    }
  }

  FOR_EACH (t, threads) { (*t).join(); }
}

TEST(ConcurrentSkipList, ConcurrentAccess) {
  testConcurrentAccess(10000, 100, kMaxValue);
  testConcurrentAccess(100000, 10000, kMaxValue * 10);
  testConcurrentAccess(1000000, 100000, kMaxValue);
}*/

int main(int argc, char* argv[]) 
{
   std::cout <<" size of node = "<<sizeof(SkipListNodeType)<<std::endl;
   auto skipList(SkipListType::create(kHeadHeight));
   assert(skipList.first() == nullptr);
   assert(skipList.last() == nullptr);
   skipList.insert(1);
   skipList.insert(5);
   skipList.insert(4);
   assert(skipList.contains(1)==true);
   skipList.remove(4);
   assert(skipList.contains(4)==false);

}
