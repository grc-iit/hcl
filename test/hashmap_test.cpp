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
#include <random>

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
   long size_of_request = 1000;
   int num_request = 100;

   HCL_CONF->IS_SERVER = is_server;
   HCL_CONF->MY_SERVER = my_server;
   HCL_CONF->NUM_SERVERS = num_servers;
   HCL_CONF->SERVER_ON_NODE = server_on_node || is_server;
   HCL_CONF->SERVER_LIST_PATH = "./server_list";

   std::default_random_engine rd;
   std::uniform_int_distribution<int32_t> dist(0,100000000);

   rd.seed(rank);
   auto die = std::bind(dist,rd);

   hcl::unordered_map_concurrent<uint32_t,uint32_t> *block_map;

   if(is_server)
   std::cout <<" server rank = "<<rank<<std::endl;

   uint64_t total_length = 1024;
   block_map = new hcl::unordered_map_concurrent<uint32_t,uint32_t> ();
 
   
   block_map->initialize_tables(total_length,num_servers,my_server,UINT32_MAX);

   MPI_Barrier(MPI_COMM_WORLD);

   if(!is_server)
   {

   int num_operations = num_request/comm_size;
   int rem = num_request%comm_size;
   if(rank < rem) num_operations++;

   for(int i=0;i<num_operations;i++)
   {
     int op = random()%3;
     uint32_t k = dist(rd);
     uint32_t server = block_map->serverLocation(k);
     if(op==0)
     uint32_t ret = block_map->Insert(server,k,k);
     else if(op==1)
     {
	 bool s = block_map->Find(server,k);
     }
     else if(op==2)
     {
	 bool s = block_map->Erase(server,k);
     }

   }

   }

   MPI_Barrier(MPI_COMM_WORLD);

  delete block_map;
   MPI_Finalize();
}
