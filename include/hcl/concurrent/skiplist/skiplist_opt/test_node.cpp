#define BOOST_THREAD_PROVIDES_SHARED_MUTEX_UPWARDS_CONVERSIONS
#define BOOST_THREAD_PROVIDES_EXPLICIT_LOCK_CONVERSION
#define BOOST_THREAD_PROVIDES_GENERIC_SHARED_MUTEX_ON_WIN


#include <iostream>
#include <type_traits>
#include "Skiplist.h"
#include <chrono>

Skiplist<int,int> *s;

struct thread_arg
{ 
   int tid;
   int num_operations;
   int op;
};

void operations(struct thread_arg *t)
{
	if(t->op==0)
	for(int i=0;i<t->num_operations;i++)
	{
	   int op = random()%2;
           int key = random()%10000000;
	   int data = 1;
	   bool b = false;
	   //std::cout <<" op = "<<op<<" key = "<<key<<std::endl;
	   //if(op==0)
	   {
             b = false;
	     do
	     {
	      b = s->InsertData(key,data);
	      }while(!b);

	   }
	   /*else if(op==1)
	   {
	     int d = 0;
	     
	     do
	     {
		d = s->FindData(key);
	     }while(d==0);

	     //s->EraseData(key);

	   }*/
	}
	else
	{
	   for(int i=0;i<t->num_operations;i++)
	   {
	      int key = random()%10000000;
	      bool b = false;

	      b = s->EraseData(key);
	   }
	}
}

int main(int argc, char **argv)
{

   memory_allocator<int,int> *m = new memory_allocator<int,int> (100);

   s = new Skiplist<int,int>(m);

   int num_operations = 10000000;
   int num_threads = 12;
   int nops = num_operations/num_threads;
   int rem = num_operations%num_threads;

   std::vector<struct thread_arg> t_args(12);
   std::vector<std::thread> workers(12);

   //auto t1 = std::chrono::high_resolution_clock::now();

   for(int i=0;i<num_threads;i++)
   {
	 t_args[i].tid = i;
	 t_args[i].op = 0;
	 if(i < rem) t_args[i].num_operations = nops+1;
	 else t_args[i].num_operations = nops;
	 std::thread t{operations,&t_args[i]};
	 workers[i] = std::move(t);
   }

   for(int i=0;i<num_threads;i++)
	   workers[i].join();

   num_threads = 1;

   nops = num_operations/num_threads;
   rem = num_operations%num_threads;

   //s->check_list();
   auto t1 = std::chrono::high_resolution_clock::now();

   for(int i=0;i<num_threads;i++)
   {
	t_args[i].tid = i;
	t_args[i].op = 2;
	if(i < rem) t_args[i].num_operations = nops+1;
	else t_args[i].num_operations = nops;
	std::thread t{operations,&t_args[i]};
	workers[i] = std::move(t);
   }

   for(int i=0;i<num_threads;i++)
	   workers[i].join();
   
   auto t2 = std::chrono::high_resolution_clock::now();
   double t = std::chrono::duration<double> (t2-t1).count();
   std::cout <<" operations = "<<num_operations<<" num_threads = "<<num_threads<<" t = "<<t<<std::endl;
   //s->check_list();

   delete s;
   delete m;
}
