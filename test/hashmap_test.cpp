/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <execinfo.h>
#include <hcl/common/data_structures.h>
#include <hcl/concurrent/unordered_map/unordered_map.h>
#include <mpi.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <utility>
#include <random>
#include <cassert>

#if defined(HCL_ENABLE_THALLIUM_TCP) || defined(HCL_ENABLE_THALLIUM_ROCE)
template <typename A>
void serialize(A &ar, int &a) {
  ar &a;
}
#endif

int main(int argc, char *argv[]) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if (provided < MPI_THREAD_MULTIPLE) {
    printf("Didn't receive appropriate MPI threading specification\n");
    exit(EXIT_FAILURE);
  }
  int comm_size, my_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int ranks_per_server = comm_size, num_request = 100;
  long size_of_request = 1000;
  bool debug = false;
  bool server_on_node = false;
  if (argc > 1) ranks_per_server = atoi(argv[1]);
  if (argc > 2) num_request = atoi(argv[2]);
  if (argc > 3) size_of_request = (long)atol(argv[3]);
  if (argc > 4) server_on_node = (bool)atoi(argv[4]);
  if (argc > 5) debug = (bool)atoi(argv[5]);

  int len;
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  MPI_Get_processor_name(processor_name, &len);
  if (debug) {
    printf("%s/%d: %d\n", processor_name, my_rank, getpid());
  }

  if (debug && my_rank == 0) {
    printf("%d ready for attach\n", comm_size);
    fflush(stdout);
    getchar();
  }
  bool is_server = (my_rank + 1) % ranks_per_server == 0;
  int my_server = my_rank / ranks_per_server;
  int num_servers = comm_size / ranks_per_server;

  assert (comm_size%ranks_per_server==0);

  std::string proc_name = std::string(processor_name);

  size_t size_of_elem = sizeof(int);

  /*printf("rank %d, is_server %d, my_server %d, num_servers %d\n", my_rank,
         is_server, my_server, num_servers);*/

  const int array_size = TEST_REQUEST_SIZE;

  if (size_of_request != array_size) 
  {
    if(my_rank==0)
	 std::cout <<" Please set request size and number of RPC threads in constants.h. Default value for request size is "<<TEST_REQUEST_SIZE<<" and number of RPC_THREADS is 1"<<std::endl;
  }

  std::array<int, array_size> my_vals = std::array<int, array_size>();

  HCL_CONF->IS_SERVER = is_server;
  HCL_CONF->MY_SERVER = my_server;
  HCL_CONF->NUM_SERVERS = num_servers;
  HCL_CONF->SERVER_ON_NODE = server_on_node || is_server;
  HCL_CONF->SERVER_LIST_PATH = "./server_list";

  std::default_random_engine rd;
  std::uniform_int_distribution<int> dist(0,100000000);

  rd.seed(my_rank);
  auto die = std::bind(dist,rd);

  hcl::unordered_map_concurrent<int,int> *map = new hcl::unordered_map_concurrent<int,int>();

  uint64_t total_size = 8192;
  map->initialize_tables(total_size,num_servers,my_server,INT32_MAX);

  MPI_Barrier(MPI_COMM_WORLD);
   
  num_request = 1000000;

  MPI_Comm client_comm;
  MPI_Comm_split(MPI_COMM_WORLD, !is_server, my_rank, &client_comm);
  int client_comm_size;
  int client_rank;
  MPI_Comm_size(client_comm, &client_comm_size);
  MPI_Comm_rank(client_comm, &client_rank);

  int num_ops = num_request / client_comm_size;
  int rem = num_request % client_comm_size;
  if(client_rank < rem) num_ops++;

  MPI_Barrier(MPI_COMM_WORLD);
  if (!is_server) 
  {

    MPI_Barrier(client_comm);
    auto t1 = std::chrono::high_resolution_clock::now(); 

    for(int i=0;i<num_ops;i++)
    {
	int op = dist(rd)%3;
	if(op==0)
	{
	    int k = dist(rd);
	    uint64_t r = map->serverLocation(k);
	    bool s = map->Insert(r,k,k);
	}
	else if(op==1)
	{
	   int k = dist(rd);
	   uint64_t r = map->serverLocation(k);
	   bool s = map->Find(r,k);
	}
	else if(op==2)
	{
	  int k = dist(rd);
	  uint64_t r = map->serverLocation(k);
	  bool s = map->Erase(r,k);
	}
    }

    auto t2 = std::chrono::high_resolution_clock::now();
    double t = std::chrono::duration<double>(t2-t1).count();
    MPI_Barrier(client_comm);

    double total_time = 0;
    MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,client_comm);
    if(client_rank==0) std::cout <<" Number of request = "<<num_request<<" Total time taken = "<<t<<" seconds"<<std::endl;

  }
  MPI_Barrier(MPI_COMM_WORLD);
  if(is_server) 
    std::cout <<" Number of elements allocated = "<<map->allocated()<<" removed = "<<map->removed()<<std::endl;
  delete (map);
  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
