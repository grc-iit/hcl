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
#include <hcl/concurrent/queue/queue.h>
#include <mpi.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <iostream>
#include <utility>
#include <random>

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

  /* if(comm_size/ranks_per_server < 2){
       perror("comm_size/ranks_per_server should be atleast 2 for this test\n");
       exit(-1);
   }*/
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
  MPI_Barrier(MPI_COMM_WORLD);
  bool is_server = (my_rank + 1) % ranks_per_server == 0;
  int my_server = my_rank / ranks_per_server;
  int num_servers = comm_size / ranks_per_server;

  // The following is used to switch to 40g network on Ares.
  // This is necessary when we use RoCE on Ares.
  std::string proc_name = std::string(processor_name);
  /*int split_loc = proc_name.find('.');
  std::string node_name = proc_name.substr(0, split_loc);
  std::string extra_info = proc_name.substr(split_loc+1, string::npos);
  proc_name = node_name + "-40g." + extra_info;*/

  size_t size_of_elem = sizeof(int);

  printf("rank %d, is_server %d, my_server %d, num_servers %d\n", my_rank,
         is_server, my_server, num_servers);

  const int array_size = TEST_REQUEST_SIZE;

  if (size_of_request != array_size && my_rank==0) {
    printf(
        "Please set TEST_REQUEST_SIZE in include/hcl/common/constants.h "
        "instead. Testing with %d\n",
        array_size);
  }

  HCL_CONF->IS_SERVER = is_server;
  HCL_CONF->MY_SERVER = my_server;
  HCL_CONF->NUM_SERVERS = num_servers;
  HCL_CONF->SERVER_ON_NODE = server_on_node || is_server;
  HCL_CONF->SERVER_LIST_PATH = "./server_list";

  hcl::concurrent_queue<int> *queue = new hcl::concurrent_queue<int> ();

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm client_comm;
  MPI_Comm_split(MPI_COMM_WORLD, !is_server, my_rank, &client_comm);
  int client_comm_size;
  MPI_Comm_size(client_comm, &client_comm_size);
  
  std::default_random_engine rd;
  std::uniform_int_distribution<int> dist(0,100000000);

  rd.seed(my_rank);
  auto die = std::bind(dist,rd);

  MPI_Barrier(MPI_COMM_WORLD);
  if (!is_server) 
  {

      for (int i = 0; i < num_request; i++) 
      {
        int value = dist(rd)%10000;
        uint64_t s = random()%num_servers;
        queue->Push(s,value);
      }
      
      for (int i = 0; i < num_request; i++) 
      {
	uint64_t s = random()%num_servers;
	std::pair<bool,int> r = queue->Pop(s);
      }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  delete (queue);
  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
