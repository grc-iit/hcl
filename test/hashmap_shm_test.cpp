#include <sys/types.h>
#include <unistd.h>

#include <functional>
#include <utility>
#include <mpi.h>
#include <iostream>
#include <signal.h>
#include <execinfo.h>
#include <chrono>
#include <queue>
#include <hcl/common/data_structures.h>
#include <hcl/concurrent/unordered_map/unordered_map.h>
#include <thread>

struct thread_arg
{
   int tid;
   int num_operations;
};

hcl::unordered_map_concurrent<uint32_t,uint32_t> *block_map;

#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
template<typename A>
void serialize(A &ar, uint32_t &a) {
    ar & a;
}
/*
template<typename A>
void serialize(A &ar, uint64_t &a) {
	ar & a;
}*/
#endif

void map_operations(struct thread_arg *t)
{

	for(int i=0;i<t->num_operations;i++)
	{
		uint32_t op = random()%3;
		uint32_t k = random()%100000;
		if(op==0)
		uint32_t ret = block_map->LocalInsert(k,k);
		else if(op==1)
			bool s = block_map->LocalFind(k);
		else if(op==2)
			bool s = block_map->LocalErase(k);
	}


}

int main(int argc,char **argv)
{

   int provided;
   MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&provided);
   int comm_size, rank;
   MPI_Comm_size(MPI_COMM_WORLD,&comm_size);
   MPI_Comm_rank(MPI_COMM_WORLD,&rank);

   int ranks_per_server = comm_size;
   bool is_server = ((rank+1)%ranks_per_server)==0;
   bool server_on_node = false;
   int my_server=rank/ranks_per_server;
   int num_servers=comm_size/ranks_per_server;

   assert (num_servers==1);

   HCL_CONF->IS_SERVER = is_server;
   HCL_CONF->MY_SERVER = my_server;
   HCL_CONF->NUM_SERVERS = num_servers;
   HCL_CONF->SERVER_ON_NODE = server_on_node || is_server;
   HCL_CONF->SERVER_LIST_PATH = "./server_list";

   uint64_t total_length = 1024;
   block_map = new hcl::unordered_map_concurrent<uint32_t,uint32_t> ();
 
   
   block_map->initialize_tables(total_length,num_servers,my_server,UINT32_MAX);

   int num_threads = 8;

   std::vector<struct thread_arg> t_args(num_threads);
   std::vector<std::thread> workers(num_threads);

   MPI_Barrier(MPI_COMM_WORLD);

   if(is_server)
   {
	   int num_operations = 1000000;
	   int nops = num_operations/num_threads;
	   int rem = num_operations%num_threads;

	   for(int i=0;i<num_threads;i++)
	   {
		t_args[i].tid = i;
		if(i < rem) t_args[i].num_operations = nops+1;
		else t_args[i].num_operations = nops;
	   }

	   for(int i=0;i<num_threads;i++)
	   {
	       std::thread t{map_operations,&t_args[i]};
	       workers[i] = std::move(t);
	   }

	   for(int i=0;i<num_threads;i++)
		   workers[i].join();
	    
   }

   MPI_Barrier(MPI_COMM_WORLD);

  delete block_map;
   MPI_Finalize();
}
