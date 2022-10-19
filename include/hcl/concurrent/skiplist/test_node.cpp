#define BOOST_THREAD_PROVIDES_SHARED_MUTEX_UPWARDS_CONVERSIONS
#define BOOST_THREAD_PROVIDES_EXPLICIT_LOCK_CONVERSION
#define BOOST_THREAD_PROVIDES_GENERIC_SHARED_MUTEX_ON_WIN


#include <iostream>
#include <type_traits>
#include "Skiplist.h"

Skiplist<int,int> *s;

struct thread_arg
{ 
   int tid;
   int num_operations;
};

void operations(struct thread_arg *t)
{

	for(int i=0;i<t->num_operations;i++)
	{
           int key = random()%10000;
	   int data = 1;
	   //std::cout <<" tid = "<<t->tid<<" data = "<<data<<std::endl;
	   bool b = false;
	   b = s->InsertData(key,data);
	   /*if(b) 
	   {
	      b = s->FindData(key);

	   }*/
	   //if(!b) std::cout <<" Not Found data = "<<key<<std::endl;
	   //std::cout <<" tid = "<<t->tid<<" data end = "<<data<<std::endl;
	}
}


int main(int argc, char **argv)
{

   memory_allocator<int,int> *m = new memory_allocator<int,int> (100);

   s = new Skiplist<int,int>(m);

   int num_operations = 100000;

   int num_threads = 12;
   int nops = num_operations/num_threads;
   int rem = num_operations%num_threads;

   std::vector<struct thread_arg> t_args(num_threads);
   std::vector<std::thread> workers(num_threads);

   for(int i=0;i<num_threads;i++)
   {
	 t_args[i].tid = i;
	 if(i < rem) t_args[i].num_operations = nops+1;
	 else t_args[i].num_operations = nops;
	 std::thread t{operations,&t_args[i]};
	 workers[i] = std::move(t);
   }

   for(int i=0;i<num_threads;i++)
	   workers[i].join();

   std::cout <<" check list"<<std::endl;
   s->check_list();

   delete s;
   delete m;
}
