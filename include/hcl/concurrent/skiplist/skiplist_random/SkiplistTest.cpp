#include "Skiplist.h"

#include <atomic>
#include <memory>
#include <set>
#include <system_error>
#include <thread>
#include <vector>

#include "Skiplist.h"

typedef SkipListNode<int> SkipListNodeType;
typedef ConcurrentSkipList<int> SkipListType;
typedef SkipListType::Accessor SkipListAccessor;
static const int kHeadHeight = 2;
static const int kMaxValue = 5000;

struct thread_arg
{
   int op;
   int tid;
   int num_operations;
};

SkipListAccessor s(SkipListType::create(kHeadHeight));

void list_operations(struct thread_arg *t)
{

    for(int i=0;i<t->num_operations;i++)
    {
	int key = random()%10000000;

	int op = random()%3;
	if(op==0)
	{
	  s.insert(key);
	}
	else if(op==1)
	{
	   s.contains(key);
	}
	else
	s.remove(key);
    }

}

int main(int argc, char* argv[]) 
{
   int num_threads = 12;
   std::vector<struct thread_arg> t_args(num_threads);
   std::vector<std::thread> workers(num_threads);

   int num_operations = 1000000;
   int nops = num_operations/num_threads;
   int rem = num_operations%num_threads;

   auto t1 = std::chrono::high_resolution_clock::now();

   for(int i=0;i<num_threads;i++)
   {
	t_args[i].tid = i;
	if(i < rem) t_args[i].num_operations = nops+1;
	else t_args[i].num_operations = nops;

	std::thread t{list_operations,&t_args[i]};
	workers[i] = std::move(t);
   }

   for(int i=0;i<num_threads;i++)
	   workers[i].join();

   auto t2 = std::chrono::high_resolution_clock::now();
   double t = std::chrono::duration<double>(t2-t1).count();
   std::cout <<" num_operations = "<<num_operations<<" num_threads = "<<num_threads<<" time taken = "<<t<<" seconds"<<std::endl;
}
