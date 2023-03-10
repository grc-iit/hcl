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
#include <hcl/concurrent/skiplist/skiplist.h>
#include <mpi.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <iostream>
#include <utility>
#include <random>
#include <set>
#include <mutex>

struct thread_arg
{
  int tid;
  int num_operations;
};

std::mutex m;
std::set<int> *stl_set;
hcl::concurrent_skiplist<int> *s;

void stl_set_operations(struct thread_arg *t)
{
   for(int i=0;i<t->num_operations;i++)
   {
	int op = random()%3;
	int k = random()%1000000;

	m.lock();
	if(op==0)
	{
	    stl_set->insert(k);
	}
	else if(op==1)
	{
	   stl_set->find(k);
	}
	else if(op==2)
	{
	  stl_set->erase(k);
	}
	m.unlock();
   }
}

void hcl_set_operations(struct thread_arg *t)
{

  for(int i=0;i<t->num_operations;i++)
  {
       int op = random()%3;
       int k = random()%1000000;
       if(op==0)
       {
	     auto b = s->LocalInsert(k);
       }
       else if(op==1)
       {
	       auto b = s->LocalFind(k);
       }
       else
       {
	       auto b = s->LocalErase(k);
       }
  }
}


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
  int ranks_per_server = comm_size, num_request = 10000;
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

  HCL_CONF->IS_SERVER = is_server;
  HCL_CONF->MY_SERVER = my_server;
  HCL_CONF->NUM_SERVERS = num_servers;
  HCL_CONF->SERVER_ON_NODE = server_on_node || is_server;
  HCL_CONF->SERVER_LIST_PATH = "./server_list";

  stl_set = new std::set<int> ();
  s = new hcl::concurrent_skiplist<int> ();

  s->initialize_sets(num_servers,my_server);

  double local_set_throughput_l=0,local_set_throughput=0;
  double elapsed_time =0;
  double remote_set_throughput_l=0,remote_set_throughput=0;
  double baseline_l=0,baseline=0;

  if(is_server)
  {
    int num_threads = 8;
    std::vector<struct thread_arg> t_args(num_threads);
    std::vector<std::thread> workers(num_threads);

    int num_ops = num_request/num_threads;
    int rem = num_request%num_threads;

    auto t1 = std::chrono::high_resolution_clock::now();

    for(int i=0;i<num_threads;i++)
    {
       t_args[i].tid = i;
       if(i < rem) t_args[i].num_operations = num_ops+1;
       else t_args[i].num_operations = num_ops;
       std::thread t{stl_set_operations,&t_args[i]};
       workers[i] = std::move(t);
    }

    for(int i=0;i<num_threads;i++)
	    workers[i].join();
    auto t2 = std::chrono::high_resolution_clock::now();
    elapsed_time = std::chrono::duration<double>(t2-t1).count();
    baseline_l = num_request/elapsed_time;

    t1 = std::chrono::high_resolution_clock::now();
    for(int i=0;i<num_threads;i++)
    {
       t_args[i].tid = i;
       if(i < rem) t_args[i].num_operations = num_ops+1;
       else t_args[i].num_operations = num_ops;
       std::thread t{hcl_set_operations,&t_args[i]};
       workers[i] = std::move(t);
    }

    for(int i=0;i<num_threads;i++)
           workers[i].join();

    t2 = std::chrono::high_resolution_clock::now();
    elapsed_time = std::chrono::duration<double>(t2-t1).count();
    local_set_throughput_l = (double) num_request / elapsed_time;
  }
  MPI_Allreduce(&baseline_l,&baseline,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  if(my_rank==0) std::cout <<" Baseline throughput = "<<baseline<<" reqs/sec"<<std::endl;
  MPI_Allreduce(&local_set_throughput_l,&local_set_throughput,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  if(my_rank==0) std::cout <<" Local_operations_throughput = "<<local_set_throughput<<" reqs/sec"<<std::endl;

  MPI_Comm client_comm;
  MPI_Comm_split(MPI_COMM_WORLD, !is_server, my_rank, &client_comm);
  int client_comm_size;
  int client_rank;
  MPI_Comm_size(client_comm, &client_comm_size);

  if(!is_server)
  {
    MPI_Comm_rank(client_comm,&client_rank);

    std::default_random_engine rd;
    std::uniform_int_distribution<int> dist(0,100000000);

    rd.seed(my_rank);
    auto die = std::bind(dist,rd);

    int num_ops = num_request / client_comm_size;
    int rem = num_request % client_comm_size;
    if(client_rank < rem) num_ops++;

    MPI_Barrier(client_comm);

    auto t1 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_ops; i++) 
    {
	int op = random()%3;
        int value = dist(rd)%10000;
	if(op==0)
	{
           s->Insert(value);
	}
	else if(op==1)
	{
	   s->Find(value);
	}
	else
	   s->Erase(value);
     }
  
     auto t2 = std::chrono::high_resolution_clock::now();
     elapsed_time = std::chrono::duration<double>(t2-t1).count();
     MPI_Barrier(client_comm);
     remote_set_throughput_l = num_request/elapsed_time;
  }

  MPI_Allreduce(&remote_set_throughput_l,&remote_set_throughput,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  if(my_rank==0) std::cout <<" Remote_operations_throughput = "<<remote_set_throughput<<" reqs/sec"<<std::endl;
  delete (s);
  delete (stl_set);
  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
