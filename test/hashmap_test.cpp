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
  MPI_Barrier(MPI_COMM_WORLD);
  bool is_server = (my_rank + 1) % ranks_per_server == 0;
  int my_server = my_rank / ranks_per_server;
  int num_servers = comm_size / ranks_per_server;

  std::string proc_name = std::string(processor_name);

  size_t size_of_elem = sizeof(int);

  printf("rank %d, is_server %d, my_server %d, num_servers %d\n", my_rank,
         is_server, my_server, num_servers);

  const int array_size = TEST_REQUEST_SIZE;

  if (size_of_request != array_size) 
  {
    if(my_rank==0)
    printf(
        "Please set TEST_REQUEST_SIZE in include/hcl/common/constants.h "
        "instead. Testing with %d\n",
        array_size);
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

  uint64_t total_size = 1024;
  map->initialize_tables(total_size,num_servers,my_server,INT32_MAX);

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Comm client_comm;
  MPI_Comm_split(MPI_COMM_WORLD, !is_server, my_rank, &client_comm);
  int client_comm_size;
  MPI_Comm_size(client_comm, &client_comm_size);
  MPI_Barrier(MPI_COMM_WORLD);
  if (!is_server) {
    Timer llocal_map_timer = Timer();

    MPI_Barrier(client_comm);

    if (HCL_CONF->SERVER_ON_NODE) {
      Timer local_map_timer = Timer();
      /*Local map test*/
      for (int i = 0; i < num_request; i++) 
      {
        int val = dist(rd);
	int v = 1;
        auto key = map->serverLocation(val);
        local_map_timer.resumeTime();
        map->Insert(key,val,v);
        local_map_timer.pauseTime();
      }
      double local_map_throughput = num_request /
                                    local_map_timer.getElapsedTime() * 1000 *
                                    size_of_elem * my_vals.size() / 1024 / 1024;

      Timer local_get_map_timer = Timer();
      /*Local map test*/
      for (int i = 0; i < num_request; i++) {
        int val = dist(rd);
        auto key = map->serverLocation(val);
        local_get_map_timer.resumeTime();
        auto result = map->Find(key,val);
        local_get_map_timer.pauseTime();
      }

      double local_get_map_throughput =
          num_request / local_get_map_timer.getElapsedTime() * 1000 *
          size_of_elem * my_vals.size() / 1024 / 1024;

      double local_put_tp_result, local_get_tp_result;
      if (client_comm_size > 1) {
        MPI_Reduce(&local_map_throughput, &local_put_tp_result, 1, MPI_DOUBLE,
                   MPI_SUM, 0, client_comm);
        MPI_Reduce(&local_get_map_throughput, &local_get_tp_result, 1,
                   MPI_DOUBLE, MPI_SUM, 0, client_comm);
        local_put_tp_result /= client_comm_size;
        local_get_tp_result /= client_comm_size;
      } else {
        local_put_tp_result = local_map_throughput;
        local_get_tp_result = local_get_map_throughput;
      }

      if (my_rank == 0) {
        printf("local_map_throughput put: %f\n", local_put_tp_result);
        printf("local_map_throughput get: %f\n", local_get_tp_result);
      }
    }
    MPI_Barrier(client_comm);

    if (!HCL_CONF->SERVER_ON_NODE) {
      Timer remote_map_timer = Timer();
      /*Remote map test*/
      for (int i = 0; i < num_request; i++) {
        int val = dist(rd);
	int v = 1;
        auto key = map->serverLocation(val);
        remote_map_timer.resumeTime();
        map->Insert(key,val,v);
        remote_map_timer.pauseTime();
      }
      double remote_map_throughput =
          num_request / remote_map_timer.getElapsedTime() * 1000 *
          size_of_elem * my_vals.size() / 1024 / 1024;

      MPI_Barrier(client_comm);

      Timer remote_get_map_timer = Timer();
      /*Remote map test*/
      for (int i = 0; i < num_request; i++) {
        int val = dist(rd);
        auto key = map->serverLocation(val);
        remote_get_map_timer.resumeTime();
        map->Find(key,val);
        remote_get_map_timer.pauseTime();
      }
      double remote_get_map_throughput =
          num_request / remote_get_map_timer.getElapsedTime() * 1000 *
          size_of_elem * my_vals.size() / 1024 / 1024;

      double remote_put_tp_result, remote_get_tp_result;
      if (client_comm_size > 1) {
        MPI_Reduce(&remote_map_throughput, &remote_put_tp_result, 1, MPI_DOUBLE,
                   MPI_SUM, 0, client_comm);
        remote_put_tp_result /= client_comm_size;
        MPI_Reduce(&remote_get_map_throughput, &remote_get_tp_result, 1,
                   MPI_DOUBLE, MPI_SUM, 0, client_comm);
        remote_get_tp_result /= client_comm_size;
      } else {
        remote_put_tp_result = remote_map_throughput;
        remote_get_tp_result = remote_get_map_throughput;
      }

      if (my_rank == 0) {
        printf("remote map throughput (put): %f\n", remote_put_tp_result);
        printf("remote map throughput (get): %f\n", remote_get_tp_result);
      }
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  delete (map);
  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
