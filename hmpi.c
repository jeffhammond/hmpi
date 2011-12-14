#define _GNU_SOURCE

#ifdef MPI
#define MPI_FOO
#undef MPI
#endif
#include "hmpi.h"
#ifdef MPI_FOO
#define MPI
#else
#undef MPI
#endif


//#define _PROFILE 1
//#define _PROFILE_HMPI 1
//#define _PROFILE_PAPI_EVENTS 1
#include "profile2.h"

#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <hwloc.h>
//#include "pipelinecopy.h"
#include "lock.h"

#define BLOCK_SIZE 8192

//Wrappers to GCC/ICC extensions

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

//if *ptr == oldval, then write *newval
#define CAS_PTR(ptr, oldval, newval) \
  __sync_val_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define FETCH_ADD(ptr, val) \
    __sync_fetch_and_add(ptr, val)


PROFILE_DECLARE();
PROFILE_VAR(memcpy);
//PROFILE_VAR(cpy_send);
//PROFILE_VAR(cpy_recv);
PROFILE_VAR(allreduce);
PROFILE_VAR(op);


hwloc_topology_t g_hwloc_topo;

int g_nthreads=-1;                  //Threads per node
int g_rank=-1;                      //Underlying MPI rank for this node
static int g_size=-1;               //Underlying MPI world size
static __thread int g_hmpi_rank=-1; //HMPI rank for this thread
static __thread int g_tl_tid=-1;    //HMPI node-local rank for this thread (tid)

HMPI_Comm HMPI_COMM_WORLD;

static MPI_Comm* g_tcomms;
lock_t g_mpi_lock;      //Used to protect ANY_SRC Iprobe/Recv sequence

//Argument passthrough
static int g_argc;
static char** g_argv;
static void (*g_entry)(int argc, char** argv);

//static __thread uint32_t g_sendmatches = 0;
//static __thread uint32_t g_recvmatches = 0;

//Each thread has a list of send and receive requests.
//The receive requests are managed only by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them.

//Each thread has a globally visible list of posted receives.
//Sender iterates over this and tries to match.
// If a match is found, matching send_req's lock is set and recv_req->match_req
// points to the send req.
// Sender copies data, completes the send, but does not complete the receive.
// Receiver progresses the recv:
//  If match is locked, that means the sender matched.  need to sort this better
// 
//static __thread HMPI_Request* g_recv_reqs = NULL;
static HMPI_Request** g_recv_reqs = NULL;

static HMPI_Request** g_send_reqs = NULL;


static inline void add_send_req(HMPI_Request *req, int tid) {
  //Set req->next = head
  //CAS head with req
  //if result is not head, repeat
  HMPI_Request* next;
  do {
      next = req->next = g_send_reqs[tid];
  } while(!CAS_PTR_BOOL(&g_send_reqs[tid], next, req));
}


static inline void add_recv_req(HMPI_Request *req) {
  int tid = g_tl_tid;

  req->next = g_recv_reqs[tid];
  req->prev = NULL;
  if(req->next != NULL) {
    req->next->prev = req;
  }

  g_recv_reqs[tid] = req;
}


static inline void remove_recv_req(HMPI_Request *req) {
    int tid = g_tl_tid;

    if(req->prev == NULL) {
        //Head of list
        g_recv_reqs[tid] = req->next;
    } else {
        req->prev->next = req->next;
    }

    if(req->next != NULL) {
        req->next->prev = req->prev;
    }
}


#if 0
static inline int match_send(int dest, int tag, HMPI_Request** recv_req) {
    int rank = g_hmpi_rank;
    HMPI_Request* cur;
    HMPI_Request* prev;

    //wrong list!  need the rank of the receiver
    for(cur = g_recv_reqs[dest]; cur != NULL; cur = cur->next) {
        if((cur->proc == rank || cur->proc == MPI_ANY_SOURCE) &&
                (cur->tag == tag || cur->tag == MPI_ANY_TAG)) {
            //Match!
            //g_sendmatches += 1;
            *recv_req = cur;
            return 1;
        }
    }

    return 0;
}
#endif

static inline int match_recv(HMPI_Request* recv_req, HMPI_Request** send_req) {
    HMPI_Request* cur;
    HMPI_Request* prev;

    for(prev = NULL, cur = g_send_reqs[g_tl_tid];
            cur != NULL; prev = cur, cur = cur->next) {
//        printf("%d match recv %d %d %d cur %d %d ANY %d %d\n", g_tl_tid, recv_req->type, recv_req->proc,
//                recv_req->tag, cur->proc, cur->tag, MPI_ANY_SOURCE, MPI_ANY_TAG); fflush(stdout);
//        printf("%d proc match? %d %d\n", g_tl_tid, cur->proc == recv_req->proc, recv_req->proc == MPI_ANY_SOURCE); fflush(stdout);
//        printf("%d tag match? %d %d\n", g_tl_tid, cur->tag == recv_req->tag,
//                recv_req->tag == MPI_ANY_TAG); fflush(stdout);

        //The send request can't have ANY_SOURCE or ANY_TAG, so don't check for that.
        if((cur->proc == recv_req->proc ||
                recv_req->proc == MPI_ANY_SOURCE) &&
                (cur->tag == recv_req->tag || recv_req->tag == MPI_ANY_TAG)) {
            //If this element is the head, CAS head with cur->next
            if(prev == NULL) {
                //Head of list -- CAS to remove
                if(!CAS_PTR_BOOL(&g_send_reqs[g_tl_tid], cur, cur->next)) {
                    //Element is no longer the head.. find its prev then
                    // remove it.
                    for(prev = g_send_reqs[g_tl_tid];
                            prev->next != cur; prev = prev->next);
                    prev->next = cur->next;
                }
            } else {
                //Not at head of list, just remove.
                prev->next = cur->next;
            }

            //g_recvmatches += 1;

            recv_req->proc = cur->proc;
            recv_req->tag = cur->tag;
            *send_req = cur;
            return 1;
        }
    }

    return 0;
}


static inline void update_reqstat(HMPI_Request *req, int stat) {
  req->stat = stat;
  //OPA_swap_int(&req->stat, stat);
}


static inline int get_reqstat(HMPI_Request *req) {
  int stat;
  stat = req->stat;
  //return OPA_load_int(&req->stat);
  return stat;
}


// this is called by pthread create and then calls the real function!
void* trampoline(void* tid) {
    int rank = (int)(uintptr_t)tid;

    {
        //Spread threads evenly across cores
        //Each thread should bind itself according to its rank.
        // Compute rs = num_ranks / num_sockets
        //         c = r % rs, s = r / rs
        //         idx = s * num_cores + c
        hwloc_obj_t obj;
        hwloc_cpuset_t cpuset;
        int depth;

        int num_sockets =
            hwloc_get_nbobjs_by_type(g_hwloc_topo, HWLOC_OBJ_SOCKET);
        if(num_sockets == 0) {
            num_sockets = 1;
        }

        int num_cores =
            hwloc_get_nbobjs_by_type(g_hwloc_topo, HWLOC_OBJ_CORE);
        if(num_cores == 0) {
            num_cores = 1;
        }

        if(num_cores < g_nthreads) {
            printf("%d ERROR requested %d threads but only %d cores\n",
                    g_rank, g_nthreads, num_cores);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }

        num_cores /= num_sockets; //cores per socket

#if 0
        if(g_nthreads <= num_sockets) {
            idx = rank * num_cores;
        } else { 
            int rs = g_nthreads / num_sockets;
            idx = ((rank / rs) * num_cores) + (rank % rs);
            printf("rank %d num_cores %d g_nthreads %d num_sockets %d rs %d idx %d pid %d\n", rank, num_cores, g_nthreads, num_sockets, rs, idx, getpid());
            fflush(stdout);
        }
#endif

        //Spread ranks evenly across sockets, grouping adjacent ranks into one socket.
        int idx;
        int rms = g_nthreads % num_sockets;
        int rs = (g_nthreads / num_sockets) + 1;
        if(rank < rms * rs) {
            idx = ((rank / rs) * num_cores) + (rank % rs);
        } else {
            int rmd = rank - (rms * rs);
            rs -= 1;
            idx = (rmd / rs) * num_cores + (rmd % rs) + rms * num_cores;
        }

        //printf("Rank %d binding to index %d\n", rank, idx);

        obj = hwloc_get_obj_by_type(g_hwloc_topo, HWLOC_OBJ_CORE, idx);
        if(obj == NULL) {
            printf("%d ERROR got NULL hwloc core object idx %d\n", rank, idx);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }

        cpuset = hwloc_bitmap_dup(obj->cpuset);
        hwloc_bitmap_singlify(cpuset);

        hwloc_set_cpubind(g_hwloc_topo, cpuset, HWLOC_CPUBIND_THREAD);
        hwloc_set_membind(g_hwloc_topo, cpuset, HWLOC_MEMBIND_BIND, HWLOC_CPUBIND_THREAD);
    }


    // save thread-id in thread-local storage
    g_tl_tid = rank;
    g_hmpi_rank = g_rank*g_nthreads+rank;

    PROFILE_INIT(g_tl_tid);

    //printf("%d:%d entered trampoline\n", g_rank, g_tl_tid); fflush(stdout);

    // barrier to avoid race in tid ...
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);

    //printf("%d:%d g_entry now\n", g_rank, g_tl_tid); fflush(stdout);
    // call user function
    g_entry(g_argc, g_argv);

    return NULL;
}


int HMPI_Init(int *argc, char ***argv, int nthreads, void (*start_routine)(int argc, char** argv))
{
  pthread_t* threads;
  int provided;
  long int thr;

  //printf("before MPI_Init\n"); fflush(stdout);
  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
  assert(MPI_THREAD_MULTIPLE == provided);
  //printf("HMPI INIT %d threads\n", nthreads); fflush(stdout);
#ifdef DEBUG
  printf("after MPI_Init\n"); fflush(stdout);
#endif

  g_argc = *argc;
  g_argv = *argv; 
  g_entry = start_routine;
  g_nthreads = nthreads;
  LOCK_INIT(&g_mpi_lock, 0);

  HMPI_COMM_WORLD = (HMPI_Comm_info*)malloc(sizeof(HMPI_Comm_info));
  HMPI_COMM_WORLD->mpicomm = MPI_COMM_WORLD;
  barrier_init(&HMPI_COMM_WORLD->barr, nthreads);

  HMPI_COMM_WORLD->sbuf = (volatile void**)malloc(sizeof(void*) * nthreads);
  HMPI_COMM_WORLD->rbuf = (volatile void**)malloc(sizeof(void*) * nthreads);
  HMPI_COMM_WORLD->scount = (volatile int*)malloc(sizeof(int) * nthreads);
  HMPI_COMM_WORLD->rcount = (volatile int*)malloc(sizeof(int) * nthreads);
  HMPI_COMM_WORLD->stype = (volatile MPI_Datatype*)malloc(sizeof(MPI_Datatype) * nthreads);
  HMPI_COMM_WORLD->rtype = (volatile MPI_Datatype*)malloc(sizeof(MPI_Datatype) * nthreads);
  //HMPI_COMM_WORLD->flag = (volatile uint8_t*)malloc(sizeof(uint8_t) * nthreads);

  //Allreduce requires sbuf be initialized to NULL
  //memset((void*)HMPI_COMM_WORLD->flag, 0, sizeof(uint8_t) * nthreads);

  threads = (pthread_t*)malloc(sizeof(pthread_t) * nthreads);

  g_recv_reqs = (HMPI_Request**)malloc(sizeof(HMPI_Request*) * nthreads);
  g_send_reqs = (HMPI_Request**)malloc(sizeof(HMPI_Request*) * nthreads);

  g_tcomms = (MPI_Comm*)malloc(sizeof(MPI_Comm) * nthreads);

  MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &g_size);

 
  //Do per-thread initialization that must be complete for all threads before
  // actually starting the threads. 
  for(thr=0; thr < nthreads; thr++) {
    // create one world communicator for each thread 
    MPI_Comm_dup(MPI_COMM_WORLD, &g_tcomms[thr]);

    // Initialize send requests list and lock
    g_recv_reqs[thr] = NULL;
    g_send_reqs[thr] = NULL;
  }

  //How can I spread threads across sockets?
  // Asume 12 cores 2 sockets
  // first nthreads / 2 go on thr
  // next nthreads/2 go on 6 + thr - nthreads/2
  // spawn threads locally
  //int threads_per_socket = nthreads / 2;
  //cpu_set_t cpuset[2];
  //CPU_ZERO(&cpuset[0]);
  //CPU_ZERO(&cpuset[1]);

  //for(thr = 0; thr < 6; thr++) {
  //  CPU_SET(thr, &cpuset[0]);
  //  CPU_SET(thr + 6, &cpuset[1]);
  //}


  //Ranks set their own affinity in trampoline.
  hwloc_topology_init(&g_hwloc_topo);
  hwloc_topology_load(g_hwloc_topo);


//TODO - should use hwloc to do this right in general case
//#define CORES 6 //Sierra

  pthread_attr_t attr;
  for(thr=0; thr < nthreads; thr++) {
    //Create the thread
    pthread_attr_init(&attr);
    //pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM); //SYSTEM is default
    int rc = pthread_create(&threads[thr], &attr, trampoline, (void *)thr);

#if 0
    //Set affinity -- pin each thread to one core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

#if 0
    //Distribute across two sockets
    if(thr < nthreads / 2) {
        //printf("thread %d core %d\n", thr, thr);
        CPU_SET(thr, &cpuset);
    } else {
        //printf("thread %d core %d\n", thr, thr - (nthreads / 2) + CORES);
        CPU_SET(thr - (nthreads / 2) + CORES, &cpuset);
    }
#endif

    //Fill a socket before moving to next
    CPU_SET(thr, &cpuset);

    rc = pthread_setaffinity_np(threads[thr], sizeof(cpu_set_t), &cpuset);
    if(rc) {
      printf("%d:%ld pthread_setaffinity_np error %s\n", g_rank, thr, strerror(rc));
      MPI_Abort(MPI_COMM_WORLD, 0);
    }
#endif
  }

  for(thr=0; thr<nthreads; thr++) {
    pthread_join(threads[thr], NULL);
  }

  free(g_recv_reqs);
  free(g_send_reqs);
  free(threads);
  free(g_tcomms);
  return 0;
}


int HMPI_Comm_rank(HMPI_Comm comm, int *rank) {
  
  //printf("[%i] HMPI_Comm_rank()\n", g_rank*g_nthreads+g_tl_tid);
#if HMPI_SAFE 
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif

  //printf("HMPI_Comm_rank %d thread %d nthreads %d mpi rank %d size %d\n",
  //  g_hmpi_rank, g_tl_tid, g_nthreads, g_rank, g_size);
  //fflush(stdout);
  *rank = g_hmpi_rank;
  return 0;
}


int HMPI_Comm_size ( HMPI_Comm comm, int *size ) {
#if HMPI_SAFE 
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
    
  *size = g_size*g_nthreads;
  return 0;
}


static inline void barrier_iprobe(void)
{
    int flag;
    MPI_Status st;

    //PROFILE_START(mpi);
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &st);
    //PROFILE_STOP(mpi);
}


// AWF new function - barrier only among local threads
void HMPI_Barrier_local(HMPI_Comm comm)
{
  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
}


int HMPI_Finalize() {

  HMPI_Barrier(HMPI_COMM_WORLD);

  //printf("%d send %d recv %d\n", g_hmpi_rank, g_sendmatches, g_recvmatches);
  //int r;
  //HMPI_Comm_rank(HMPI_COMM_WORLD, &r);
  //PROFILE_SHOW(memcpy);
  //PROFILE_SHOW(cpy_send);
  //PROFILE_SHOW(cpy_recv);

  //PROFILE_SHOW_REDUCE(copy, r);
  //PROFILE_SHOW_REDUCE(send, r);
  //PROFILE_SHOW_REDUCE(add_send_req, r);
  //PROFILE_SHOW_REDUCE(barrier, r);
  //PROFILE_SHOW_REDUCE(alltoall, r);
  PROFILE_SHOW_REDUCE(allreduce);
  PROFILE_SHOW_REDUCE(op);
  PROFILE_SHOW_REDUCE(memcpy);

  barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);

  if(g_tl_tid == 0) {
    MPI_Finalize();
  }

  return 0;
}


// global progress function
static inline int HMPI_Progress_request(HMPI_Request *req);
static inline int HMPI_Progress_recv(HMPI_Request *recv_req);


//We assume req->type == HMPI_SEND and req->stat == 0 (uncompleted send)
static inline int HMPI_Progress_send(HMPI_Request* send_req) {

    //Poll local receives in the recv reqs list.
    // We do this to prevent deadlock when local threads are exchange messages
    // with each other.  Normally no work is done on a send request, but if
    // the app blocks waiting for it to complete, a neighbor thread could also
    // block on its send and neither of their receives are ever completed.

    //TODO - this visits most rescent receives first.  maybe iterate backwards
    // to progress oldest receives first?
#if 0
    HMPI_Request* cur;

    for(cur = g_recv_reqs[g_tl_tid]; cur != NULL; cur = cur->next) {
        if(cur->type == HMPI_RECV) {
            HMPI_Progress_recv(cur);
        } else { //req->type  == MPI_RECV
            int flag;
            MPI_Test(&cur->req, &flag, MPI_STATUS_IGNORE);
            update_reqstat(cur, flag);
        }
    }
#endif

    //Write blocks on this send req if receiver has matched it.
    //If mesage is short, receiver won't bother setting recv_buf, and instead
    // just does the copy and moves on.
    if(LOCK_TRY(&send_req->match)) {
        //PROFILE_START(cpy_send);
        HMPI_Request* recv_req = (HMPI_Request*)send_req->match_req;
        uintptr_t rbuf = (uintptr_t)recv_req->buf;
        //We know the receiver has arrived, so work on copying.
        size_t size = (send_req->size < recv_req->size ?
                send_req->size : recv_req->size);
        uintptr_t sbuf = (uintptr_t)send_req->buf;
        size_t* offsetptr = (size_t*)&send_req->offset;
        size_t offset;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(offsetptr, BLOCK_SIZE)) < size) {
            size_t left = size - offset;
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }
        //PROFILE_STOP(cpy_send);

        //Possible for the receiver to still be copying here.
        LOCK_CLEAR(&send_req->match);

        //Receiver will set completion soon, wait rather than running off.
        while(get_reqstat(send_req) != 1);
        return 1;
    }

    return get_reqstat(send_req);
}


static inline int HMPI_Progress_recv(HMPI_Request *recv_req) {
    //Try to match from the local send reqs list
    HMPI_Request* send_req = NULL;

#if 0
    if(get_reqstat(recv_req) == HMPI_REQ_RECV_COMPLETE) {
        //Sender matched and completed this receive.. finish up and return.
        remove_recv_req(recv_req);
        update_reqstat(recv_req, HMPI_REQ_COMPLETE);
        return 1;
    }
#endif

    //printf("%d prog recv\n", g_tl_tid); fflush(stdout);

    if(!match_recv(recv_req, &send_req)) {
        return 0;
    }

    remove_recv_req(recv_req);

    //TODO - move this into match_recv?
    //LOCK_SET(&send_req->match);
    //recv_req->match_req = send_req;

#if 0
    if(status != MPI_STATUS_IGNORE) {
        status->MPI_SOURCE = send_req->proc;
        status->MPI_TAG = send_req->tag;
        status->MPI_ERROR = MPI_SUCCESS;
    }
#endif

#ifdef DEBUG
    printf("[%i] [recv] found send from %i (%x) for buf %x in uq (tag: %i, size: %i, status: %x)\n",
            g_hmpi_rank, send_req->proc, send_req->buf, recv_req->buf, send_req->tag, send_req->size, get_reqstat(send_req));
    fflush(stdout);
#endif

    //Remove the recv from the request list
    //remove_recv_req(recv_req);

    size_t size = send_req->size;
    if(unlikely(size > recv_req->size)) {
        //printf("[recv] message of size %i truncated to %i (doesn't fit in matching receive buffer)!\n", sendsize, recv_req->size);
        size = recv_req->size;
    }

    //printf("[%i] memcpy %p -> %p (%i)\n",
    //        g_hmpi_rank, send_req->buf, recv_req->buf, sendsize);
    //fflush(stdout);


    if(size < BLOCK_SIZE * 2) {
    //if(size < 128) {
    //if(size < 1024 * 1024) {
        //PROFILE_START(memcpy);
        memcpy((void*)recv_req->buf, send_req->buf, size);
        //PROFILE_STOP(memcpy);
    } else {
        //The setting of send_req->match_req signals to sender that they can
        // start doing copying as well, if they are testing the req.
        //PROFILE_START(cpy_recv);

        //recv_req->match_req = send_req; //TODO - keep this here?
        send_req->match_req = recv_req;
        LOCK_CLEAR(&send_req->match);

        uintptr_t rbuf = (uintptr_t)recv_req->buf;
        uintptr_t sbuf = (uintptr_t)send_req->buf;
        size_t* offsetptr = (size_t*)&send_req->offset;
        size_t offset = 0;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(offsetptr, BLOCK_SIZE)) < size) {
            size_t left = size - offset;

            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }

        //PROFILE_STOP(cpy_recv);

        //Wait if the sender is copying.
        LOCK_SET(&send_req->match);
    }

    //Mark send and receive requests done
    update_reqstat(send_req, HMPI_REQ_COMPLETE);
    update_reqstat(recv_req, HMPI_REQ_COMPLETE);

    return 1;
}


static inline void HMPI_Progress() {
    //TODO - this visits most rescent receives first.  maybe iterate backwards
    // to progress oldest receives first?
    HMPI_Request* cur;

    for(cur = g_recv_reqs[g_tl_tid]; cur != NULL; cur = cur->next) {
        if(cur->type == HMPI_RECV) {
            HMPI_Progress_recv(cur);
        } else if(cur->type == MPI_RECV) {
            int flag;
            MPI_Test(&cur->req, &flag, MPI_STATUS_IGNORE);
            update_reqstat(cur, flag);
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            if(HMPI_Progress_recv(cur)) {
                //Local match & completion
                continue;
            } else if(g_size > 1) {
                // check if we can get something via the MPI library
                int flag=0;
                MPI_Status status;

                //TODO - probably need to lock this..
                LOCK_SET(&g_mpi_lock);
                MPI_Iprobe(MPI_ANY_SOURCE, cur->tag, g_tcomms[g_tl_tid], &flag, &status);
                if(flag) {
                  MPI_Recv((void*)cur->buf, cur->size, cur->datatype, status.MPI_SOURCE, cur->tag, g_tcomms[g_tl_tid], &status);
                  LOCK_CLEAR(&g_mpi_lock);
                  remove_recv_req(cur);

                  cur->proc = status.MPI_SOURCE;
                  cur->tag = status.MPI_TAG;
                  update_reqstat(cur, HMPI_REQ_RECV_COMPLETE);
                } else {
                  LOCK_CLEAR(&g_mpi_lock);
                }
            }
        }
    }
}


static inline int HMPI_Progress_request(HMPI_Request *req)
{
  HMPI_Progress();

  if(req->type == HMPI_SEND) {
      return HMPI_Progress_send(req);
      //return get_reqstat(req);
  } else if(req->type == HMPI_RECV) {
      //return HMPI_Progress_recv(req);
      return get_reqstat(req);
  } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
    int flag;
    //Recv is tacked on here, even though it just got tested in HMPI_Progress()

    //PROFILE_START(mpi);
    MPI_Test(&req->req, &flag, MPI_STATUS_IGNORE);
    //PROFILE_STOP(mpi);

    //TODO - is this right?
    update_reqstat(req, flag);
    return flag;
  } else /*if(req->type == HMPI_RECV_ANY_SOURCE)*/ {
      return get_reqstat(req);
#if 0
    if(HMPI_Progress_recv(req)) {
        return 1;
    }

    if(g_size > 1) {
        // check if we can get something via the MPI library
        int flag=0;
        MPI_Status status;
        MPI_Iprobe(MPI_ANY_SOURCE, req->tag, g_tcomms[g_tl_tid], &flag, &status);
        if(flag) {
          MPI_Recv((void*)req->buf, req->size, req->datatype, status.MPI_SOURCE, req->tag, g_tcomms[g_tl_tid], &status);
          remove_recv_req(req);

          req->proc = status.MPI_SOURCE;
          req->tag = status.MPI_TAG;
          update_reqstat(cur, HMPI_REQ_RECV_COMPLETE);
          return 1;
        }
    }
#endif 
  } //HMPI_RECV_ANY_SOURCE
  return 0;
}


int HMPI_Test(HMPI_Request *req, int *flag, MPI_Status *status)
{
  if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
      *flag = HMPI_Progress_request(req);
  } else {
      *flag = 1;
  }

  if(*flag && status != MPI_STATUS_IGNORE) {
      status->MPI_SOURCE = req->proc;
      status->MPI_TAG = req->tag;
      status->MPI_ERROR = MPI_SUCCESS;
  }

  return MPI_SUCCESS;
}


int HMPI_Wait(HMPI_Request *request, MPI_Status *status) {
  int flag=0;
#ifdef DEBUG
  printf("[%i] HMPI_Wait(%x, %x) type: %i\n", g_hmpi_rank, request, status, request->type);
  fflush(stdout);
#endif

  do {
    HMPI_Test(request, &flag, status);
  } while (flag!=1);

  return MPI_SUCCESS;
}


int HMPI_Isend(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *req) {
  
#ifdef DEBUG
    printf("[%i] HMPI_Isend(%p, %i, %p, %i, %i, %p, %p) (proc null: %i)\n", g_hmpi_rank, buf, count, datatype, dest, tag, comm, req, MPI_PROC_NULL);
    fflush(stdout);
#endif

    if(unlikely(dest == MPI_PROC_NULL)) { 
        update_reqstat(req, HMPI_REQ_COMPLETE);
        return MPI_SUCCESS;
    }

    //req->status = MPI_STATUS_IGNORE;
#if 0
  int size;
  MPI_Type_size(datatype, &size);
#ifdef HMPI_SAFE
  MPI_Aint extent, lb;
  MPI_Type_get_extent(datatype, &lb, &extent);
  if(extent != size) {
    printf("non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
#endif

    int target_mpi_rank = dest / g_nthreads;
    if(target_mpi_rank == g_rank) {
        int size;
        MPI_Type_size(datatype, &size);

        // send to other thread in my process
        //TODO - delay filling this stuff in until we know theres no match
        //Try to match the send
#if 0
        HMPI_Request* recv_req;
        if(match_send(dest, tag, &recv_req)) {

            //Match! just do the copy right here.
            size_t len = size*count;
            if(unlikely(len > recv_req->size)) {
                len = recv_req->size;
            }

            //if(len < BLOCK_SIZE * 2) {
                memcpy((void*)recv_req->buf, buf, len);
#if 0
            } else {
                req->type = HMPI_SEND;
                req->proc = g_hmpi_rank; // my local rank
                req->tag = tag;
                req->size = size*count;
                req->buf = buf;
                //req->match_req = NULL;
                //req->offset = 0;
                LOCK_INIT(&req->match, 1);
                //LOCK_INIT(&req->match, 0);

                update_reqstat(req, HMPI_REQ_ACTIVE);
                int target_mpi_thread = dest % g_nthreads;
                add_send_req(req, target_mpi_thread);

                //Do fancy block loop

            }
#endif

            //Mark completion.  Receive req goes into a special state indicating
            //it is complete, but needs to be removed from recv req list.
            update_reqstat(recv_req, HMPI_REQ_RECV_COMPLETE);
            update_reqstat(req, HMPI_REQ_COMPLETE);
        } else {
#endif
            req->type = HMPI_SEND;
            req->proc = g_hmpi_rank; // my local rank
            req->tag = tag;
            req->size = size*count;
            req->buf = buf;
            req->match_req = NULL;
            req->offset = 0;
            //LOCK_INIT(&req->recver_match, 1);
            LOCK_INIT(&req->match, 1);

            update_reqstat(req, HMPI_REQ_ACTIVE);
            int target_mpi_thread = dest % g_nthreads;
            //printf("[%i] LOCAL sending to thread %i at rank %i\n", g_nthreads*g_rank+g_tl_tid, target_mpi_thread, target_mpi_rank);
            add_send_req(req, target_mpi_thread);
     //   }
    } else {
        //int target_mpi_thread = dest % g_nthreads;
        //printf("[%i] MPI sending to thread %i at rank %i\n", g_nthreads*g_rank+g_tl_tid, target_mpi_thread, target_mpi_rank);
        int size;
        MPI_Type_size(datatype, &size);
        req->type = MPI_SEND;
        req->proc = g_hmpi_rank; // my local rank
        req->tag = tag;
        req->size = size*count;
        req->buf = buf;
        update_reqstat(req, HMPI_REQ_ACTIVE);

        int target_thread = dest % g_nthreads;
        MPI_Isend(buf, count, datatype, target_mpi_rank, tag, g_tcomms[target_thread], &req->req);
    }

    return MPI_SUCCESS;
}


int HMPI_Send(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm) {
  HMPI_Request req;
  HMPI_Isend(buf, count, datatype, dest, tag, comm, &req);
  HMPI_Wait(&req, MPI_STATUS_IGNORE);
  return MPI_SUCCESS;
}


int HMPI_Irecv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *req) {

  //if(unlikely(source == MPI_ANY_SOURCE)) source = HMPI_ANY_SOURCE;
  //if(unlikely(tag == MPI_ANY_TAG)) tag = HMPI_ANY_TAG;

#ifdef DEBUG
  printf("[%i] HMPI_Irecv(%x, %i, %x, %i, %i, %x, %x) (proc null: %i)\n", g_hmpi_rank, buf, count, datatype, source, tag, comm, req, MPI_PROC_NULL);
  fflush(stdout);
#endif


  if(unlikely(source == MPI_PROC_NULL)) { 
    update_reqstat(req, HMPI_REQ_COMPLETE);
    return MPI_SUCCESS;
  }

  //req->status = MPI_STATUS_IGNORE;
//  int size;
//  MPI_Type_size(datatype, &size);

#if 0
#ifdef HMPI_SAFE
  MPI_Aint extent, lb;
  MPI_Type_get_extent(datatype, &lb, &extent);
  if(extent != size) {
    printf("non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif 
#endif

  update_reqstat(req, HMPI_REQ_ACTIVE);
  
  int size;
  MPI_Type_size(datatype, &size);

  req->proc = source;
  req->tag = tag;
  req->size = size*count;
  req->buf = buf;

  int source_mpi_rank = source / g_nthreads;
  if(unlikely(source == MPI_ANY_SOURCE)) {
    // test both layers and pick first 
    req->type = HMPI_RECV_ANY_SOURCE;
    req->proc = MPI_ANY_SOURCE;
    //req->proc = source;
    //req->tag = tag;
    //req->size = size*count;
    //req->buf = buf;

    //req->comm = g_tcomms[g_tl_tid]; // not 100% sure -- this doesn't catch all messages -- Probe would need to loop over all thread comms and lock :-(
    req->datatype = datatype;

    add_recv_req(req);
    HMPI_Progress_recv(req);
  } else if(source_mpi_rank == g_rank) {
    // recv from other thread in my process
    req->type = HMPI_RECV;
    //req->match_req = NULL;

    //int tests=0;
    //while(HMPI_Progress_request(req) != 1 && ++tests<10);

    add_recv_req(req);
    HMPI_Progress_recv(req);
  } else /*if(source != MPI_ANY_SOURCE)*/ {
    int source_mpi_thread = source % g_nthreads;
    //printf("%d buf %p count %d src %d tag %d req %p\n", g_rank*g_nthreads+g_tl_tid, buf, count, source, tag, req);

    int size;
    MPI_Type_size(datatype, &size);
    //req->proc = source;
    //req->tag = tag;
    //req->size = size*count;
    //req->buf = buf;

    //PROFILE_START(mpi);
    //MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, g_tcomms[source_mpi_thread], &req->req);
    MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, g_tcomms[g_tl_tid], &req->req);
    //PROFILE_STOP(mpi);

    req->type = MPI_RECV;
  //} else /*if(source == MPI_ANY_SOURCE)*/ {
  }

  return MPI_SUCCESS;
}


int HMPI_Recv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, MPI_Status *status) {
  HMPI_Request req;
  HMPI_Irecv(buf, count, datatype, source, tag, comm, &req);
  HMPI_Wait(&req, status);
  return MPI_SUCCESS;
}


//
// Collectives
//

int HMPI_Barrier(HMPI_Comm comm) {
#ifdef DEBUG
  printf("in HMPI_Barrier\n"); fflush(stdout);
#endif

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  // all root-threads perform MPI_Barrier 
  if(g_tl_tid == 0) {
      //int rank;
      //MPI_Comm_rank(comm->mpicomm, &rank);
      MPI_Barrier(comm->mpicomm);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);
  return MPI_SUCCESS;
}


// declaration
//extern "C" {
int NBC_Operation(void *buf3, void *buf1, void *buf2, MPI_Op op, MPI_Datatype type, int count);
//}

int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm)
{
    int size;
    int i;

    MPI_Type_size(datatype, &size);

#ifdef DEBUG
    if(g_tl_tid == 0) {
    printf("[%i %i] HMPI_Reduce(%p, %p, %i, %p, %p, %d, %p)\n", g_hmpi_rank, g_tl_tid, sendbuf, recvbuf, count, datatype, op, root, comm);
    fflush(stdout);
    }
#endif

    //Root rank mod g_nthreads takes lead.
    //Root rank allocs a buffer for local ranks to reduce into.
    //Then does an MPI reduce into the receive buf, and we're done.
    if(g_tl_tid == root % g_nthreads) {
        void* localbuf;
        if(g_hmpi_rank == root) {
            localbuf = recvbuf;
        } else {
            localbuf = memalign(4096, size * count);
        }

        //TODO eliminate this memcpy by folding into a reduce call
        memcpy(localbuf, sendbuf, size * count);

        barrier_cb(&comm->barr, 0, barrier_iprobe);

        for(i=0; i<g_nthreads; ++i) {
            if(i == g_tl_tid) continue;
            NBC_Operation(localbuf,
                    localbuf, (void*)comm->sbuf[i], op, datatype, count);
        }

        //Other local ranks are free to go.
        barrier(&comm->barr, 0);

        if(g_size > 1) {
            MPI_Reduce(MPI_IN_PLACE,
                    localbuf, count, datatype, op, root / g_nthreads, comm->mpicomm);
        }

        if(g_hmpi_rank != root) {
            free(localbuf);
        }
    } else {
        //First barrier signals to root that all buffers are ready.
        comm->sbuf[g_tl_tid] = sendbuf;
        barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

        //Wait for root to copy our data; were free when it's done.
        barrier(&comm->barr, g_tl_tid);
    }

    return MPI_SUCCESS;
}


int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    MPI_Aint extent, lb;
    int size;
    int i;

    MPI_Type_size(datatype, &size);
    //MPI_Type_get_extent(datatype, &lb, &extent);
    //MPI_Type_extent(datatype, &extent);

#if 0
    if(extent != size) {
        printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
        fflush(stdout);
        MPI_Abort(comm->mpicomm, 0);
    }
#endif

#ifdef DEBUG
    if(g_tl_tid == 0) {
    printf("[%i %i] HMPI_Allreduce(%p, %p, %i, %p, %p, %p)\n", g_hmpi_rank, g_tl_tid, sendbuf, recvbuf,  count, datatype, op, comm);
    fflush(stdout);
    }
#endif

    //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    // Do the MPI allreduce, then each rank can copy out.
    if(g_tl_tid == 0) {
        comm->rbuf[0] = recvbuf;

        barrier_cb(&comm->barr, 0, barrier_iprobe);

        //TODO eliminate this memcpy by folding into a reduce call
        memcpy(recvbuf, sendbuf, size * count);
        //while(comm->flag[1] == 0);
        //NBC_Operation(recvbuf, sendbuf, (void*)comm->sbuf[1], op, datatype, count);
        //comm->flag[1] = 0;

        for(i=1; i<g_nthreads; ++i) {
            //Wait for flag to be set by each thread before reading their buf
            //while(comm->flag[i] == 0);
            NBC_Operation(recvbuf, recvbuf, (void*)comm->sbuf[i], op, datatype, count);
            //comm->flag[i] = 0;
        }

        if(g_size > 1) {
            MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->mpicomm);
        }

        //STORE_FENCE();
        //comm->flag[0] = 1;
        //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
        barrier(&comm->barr, 0);
    } else {
        comm->sbuf[g_tl_tid] = sendbuf;
        //STORE_FENCE();
        //comm->flag[g_tl_tid] = 1;

        barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

        //Threads cannot start until 0 arrives -- OK if others havent arrived
        //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
        barrier(&comm->barr, g_tl_tid);
        //while(comm->flag[0] == 0);

        //printf("%d doing allreduce copy\n", g_tl_tid); fflush(stdout);
        //while(comm->flag[0] == 0);
        memcpy(recvbuf, (void*)comm->rbuf[0], count*size);
    }

    // protect from early leave (rootrbuf)
    //0 can't leave until all threads arrive.. all others can go
    //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    barrier(&comm->barr, g_tl_tid);
    //if(g_tl_tid == 0) {
    //    comm->flag[0] = 0;
    //}

    //printf("%d done with allreduce\n", g_tl_tid); fflush(stdout);
    return MPI_SUCCESS;
}


#define HMPI_SCAN_TAG 7546348

int HMPI_Scan(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    MPI_Aint extent, lb;
    MPI_Request req;
    int size;
    int i;

    MPI_Type_size(datatype, &size);
    //MPI_Type_get_extent(datatype, &lb, &extent);
    //MPI_Type_extent(datatype, &extent);

#if 0
    if(extent != size) {
        printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
        fflush(stdout);
        MPI_Abort(comm->mpicomm, 0);
    }
#endif

#ifdef DEBUG
    if(g_tl_tid == 0) {
    printf("[%i %i] HMPI_Scan(%p, %p, %i, %p, %p, %d, %p)\n", g_hmpi_rank, g_tl_tid, sendbuf, recvbuf, count, datatype, op, root, comm);
    fflush(stdout);
    }
#endif


    //Each rank makes its send buffer available
    comm->sbuf[g_tl_tid] = sendbuf;

    //One rank posts a recv if mpi rank > 0
    if(g_tl_tid == 0 && g_rank > 0) {
        comm->rbuf[0] = memalign(4096, size * count);
        MPI_Irecv((void*)comm->rbuf[0], count, datatype,
                g_rank - 1, HMPI_SCAN_TAG, MPI_COMM_WORLD, &req);
    }

    //Each rank reduces local ranks below it
    //Copy my own receive buffer first
    memcpy(recvbuf, sendbuf, size * count);

    barrier_cb(&comm->barr, 0, barrier_iprobe);

    for(i = 0; i < g_tl_tid; i++) {
        NBC_Operation(recvbuf,
                recvbuf, (void*)comm->sbuf[i], op, datatype, count);
    }

    //Wait on recv; all ranks reduce if mpi rank > 0
    if(g_tl_tid == 0 && g_rank > 0) {
        MPI_Wait(&req, MPI_STATUS_IGNORE);
    }

    barrier(&comm->barr, 0);

    if(g_rank > 0) {
        NBC_Operation(recvbuf,
                recvbuf, (void*)comm->rbuf[0], op, datatype, count);
    }

    //Last rank sends result to next mpi rank if < size - 1
    if(g_tl_tid == g_nthreads - 1 && g_rank < g_size - 1) {
        MPI_Send(recvbuf, count, datatype,
                g_rank + 1, HMPI_SCAN_TAG, MPI_COMM_WORLD);
        free((void*)comm->rbuf[0]);
    }

    return MPI_SUCCESS;
}


int HMPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, HMPI_Comm comm) {
  MPI_Aint extent, lb;
  int size;

  MPI_Type_size(datatype, &size);
  MPI_Type_get_extent(datatype, &lb, &extent);
  //MPI_Type_extent(datatype, &extent);

#ifdef HMPI_SAFE
  if(extent != size) {
    printf("bcast non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
  
#ifdef DEBUG
  printf("[%i] HMPI_Bcast(%x, %i, %x, %i, %x)\n", g_rank*g_nthreads+g_tl_tid, buffer, count, datatype, root, comm);
#endif

  //We need a buffer set on all MPI ranks, so use thread root % tid
  //if(root == g_nthreads*g_rank+g_tl_tid) {
  if(root % g_nthreads == g_tl_tid) {
//      printf("%d set buffer\n", g_tl_tid);
      comm->sbuf[0] = buffer;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(g_tl_tid == 0) {
    MPI_Bcast((void*)comm->sbuf[0], count, datatype, root, comm->mpicomm);
  }

  barrier(&comm->barr, g_tl_tid);
  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //TODO - skip if g_rank == root / g_nthreads
  if(root % g_nthreads != g_tl_tid) {
//      printf("%d copy buffer\n", g_tl_tid);
    memcpy(buffer, (void*)comm->sbuf[0], count*size);
  }

//  fflush(stdout);
  barrier(&comm->barr, g_tl_tid);
  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

#if 0
  //TODO AWF -- uhh this wont work when root != 0
  if(g_tl_tid == 0) {
    comm->sbuf[0]=buffer;
    MPI_Bcast(buffer, count, datatype, root, comm->mpicomm);
  }

  barrier(&comm->barr);
  barrier_wait(&comm->barr);

  if(g_tl_tid != 0) memcpy(buffer, (void*)comm->sbuf[0], count*size);

  barrier(&comm->barr);
  barrier_wait(&comm->barr);
#endif
  return MPI_SUCCESS;
}


// TODO - scatter and gather may not work right for count > 1

int HMPI_Scatter(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm) {
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  MPI_Type_size(sendtype, &send_size);
  MPI_Type_size(recvtype, &recv_size);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);
  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  size = recv_size * recvcount;

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("scatter non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(size != send_size * sendcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
 
#ifdef DEBUG
  printf("[%i] HMPI_Scatter(%p, %i, %p, %p, %i, %p, %i, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
#endif

  //On the proc with the root, pass the send buffer to thread 0
  if(g_rank * g_nthreads + g_tl_tid == root) {
      comm->sbuf[0] = sendbuf;
      comm->scount[0] = sendcount;
      comm->stype[0] = sendtype;
  }

  if(g_tl_tid == 0) {
    comm->rbuf[0] = memalign(4096, size * g_nthreads);

    if(root / g_nthreads != g_rank) {
        //root is not on this node, set the send type to something
        comm->sbuf[0] = NULL;
        comm->scount[0] = recvcount;
        comm->stype[0] = recvtype;
    }
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //Just do a scatter!
  if(g_tl_tid == 0) {
    MPI_Scatter((void*)comm->sbuf[0], (int)comm->scount[0] * g_nthreads,
            (MPI_Datatype)comm->stype[0], (void*)comm->rbuf[0],
            recvcount * g_nthreads, recvtype, root / g_nthreads, comm->mpicomm);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //Each thread copies out of the root buffer
  if(recvbuf == MPI_IN_PLACE) {
      printf("in place scatter\n");
    memcpy(sendbuf, (void*)((uintptr_t)comm->rbuf[0] + size * g_tl_tid), size);
  } else {
    memcpy(recvbuf, (void*)((uintptr_t)comm->rbuf[0] + size * g_tl_tid), size);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(g_tl_tid == 0) {
      free((void*)comm->rbuf[0]);
  }

  return MPI_SUCCESS;
}


// TODO - scatter and gather may not work right for count > 1

int HMPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm)
{
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  MPI_Type_size(sendtype, &send_size);
  MPI_Type_size(recvtype, &recv_size);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);
  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  size = send_size * sendcount;

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("gather non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(size != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
 
#ifdef DEBUG
  printf("[%i] HMPI_Gather(%p, %i, %p, %p, %i, %p, %i, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
#endif

  //How do I want to do this?
  //Gather using the root threads, then pass the buffer pointer to the root
  //On the proc with the root, pass the send buffer to thread 0
  if(g_rank * g_nthreads + g_tl_tid == root) {
      comm->rbuf[0] = recvbuf;
      comm->rcount[0] = recvcount;
      comm->rtype[0] = recvtype;
  }

  if(g_tl_tid == 0) {
      comm->sbuf[0] = memalign(4096, size * g_nthreads);

    if(root / g_nthreads != g_rank) {
        //root is not on this node, set the recv type to something
        comm->rbuf[0] = NULL;
        comm->rcount[0] = sendcount;
        comm->rtype[0] = sendtype;
    }
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //Each thread copies into the send buffer
  memcpy((void*)((uintptr_t)comm->sbuf[0] + size * g_tl_tid), sendbuf, size);

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(g_tl_tid == 0) {
    MPI_Gather((void*)comm->sbuf[0], sendcount * g_nthreads,
            sendtype, (void*)comm->rbuf[0],
            recvcount * g_nthreads, recvtype, root / g_nthreads, comm->mpicomm);
    free((void*)comm->sbuf[0]);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  return MPI_SUCCESS;
}


int HMPI_Allgather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm)
{
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  MPI_Type_size(sendtype, &send_size);
  MPI_Type_size(recvtype, &recv_size);

  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);
  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  size = send_size * sendcount;

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("gather non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(size != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
 
#ifdef DEBUG
  printf("[%i] HMPI_Allgather(%p, %i, %p, %p, %i, %p, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
#endif

  //How do I want to do this?
  //Gather locally to sbuf[0]
  // MPI allgather to rbuf[0]
  // Each thread copies into its own rbuf, except 0.
  if(g_tl_tid == 0) {
      //Use this node's spot rank in tid 0's recvbuf, as the send buffer.
      comm->sbuf[0] =
          (void*)((uintptr_t)recvbuf + (size * g_nthreads * g_rank));

      //root is not on this node, set the recv type to something
      comm->rbuf[0] = recvbuf;
      //comm->rcount[0] = recvcount;
      //comm->rtype[0] = recvtype;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  //Each thread copies into the send buffer
  memcpy((void*)((uintptr_t)comm->sbuf[0] + size * g_tl_tid), sendbuf, size);

  barrier(&comm->barr, g_tl_tid);

  if(g_size > 1) {
    if(g_tl_tid == 0) {
        MPI_Allgather(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL,
                recvbuf, recvcount * g_nthreads, recvtype, comm->mpicomm);
    }

    barrier(&comm->barr, g_tl_tid);
  }

  if(g_tl_tid != 0) {
      //All threads but 0 copy from 0's receive buffer
      memcpy(recvbuf, (void*)comm->rbuf[0], size * g_nthreads * g_size);
  }

  barrier(&comm->barr, g_tl_tid);

  return MPI_SUCCESS;
}


int HMPI_Allgatherv(void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int* recvcounts, int *displs, MPI_Datatype recvtype, HMPI_Comm comm)
{
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  MPI_Type_size(sendtype, &send_size);
  MPI_Type_size(recvtype, &recv_size);

  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);
  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  size = send_size * sendcount;

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("gather non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(size != recv_size * recvcounts[g_hmpi_rank]) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
 
#ifdef DEBUG
  printf("[%i] HMPI_Allgatherv(%p, %i, %p, %p, %i, %p, %p, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, displs, recvtype, comm);
#endif

  //How do I want to do this?
  //Gather locally to sbuf[0]
  // MPI allgather to rbuf[0]
  // Each thread copies into its own rbuf, except 0.
  if(g_tl_tid == 0) {
      //Use this node's spot rank in tid 0's recvbuf, as the send buffer.
      //Have to use the displacements to get the right spot.
      comm->sbuf[0] = recvbuf;
          //(void*)((uintptr_t)recvbuf + (size * displs[g_hmpi_rank]));

      //root is not on this node, set the recv type to something
      comm->rbuf[0] = recvbuf;
      //comm->rcount[0] = recvcount;
      //comm->rtype[0] = recvtype;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  //Each thread copies into the send buffer
  memcpy((void*)((uintptr_t)comm->sbuf[0] + (send_size * displs[g_hmpi_rank])),
          sendbuf, size);

  barrier(&comm->barr, g_tl_tid);

  if(g_size > 1) {
    if(g_tl_tid == 0) {
        //Need to make a new displacements list, grouping threads in each proc.
        //TODO - this only works if the displacements were contiguous!
        //Well that's kind of silly, might was well just use allgather.
        //The right thing to do would be to build a datatype... shouldnt be bad
        MPI_Datatype dtype;
        MPI_Datatype* basetype;
        int i;

        //Do a series of bcasts, rotating the root to each node.
        //Have to build a dtype for each node to cover its uniques displs.
        for(i = 0; i < g_size; i++) {
            if(i == g_rank) {
                basetype = &sendtype;
            } else {
                basetype = &recvtype;
            }

            MPI_Type_indexed(g_nthreads, &recvcounts[i * g_nthreads],
                    &displs[i * g_nthreads], *basetype, &dtype);
            MPI_Type_commit(&dtype);

            MPI_Bcast(recvbuf, 1, dtype, i, MPI_COMM_WORLD);
      
            MPI_Type_free(&dtype);
        }
    }

    barrier(&comm->barr, g_tl_tid);
  }

  if(g_tl_tid != 0) {
      //All threads but 0 copy from 0's receive buffer
      //memcpy(recvbuf, (void*)comm->rbuf[0], size * g_nthreads * g_size);
      //Ugh, have to do one memcpy per rank.
      int i;

      //TODO - copy from sendbuf for self rank, not recvbuf
      for(i = 0; i < g_size * g_nthreads; i++) {
        int offset = displs[i] * send_size;
        memcpy((void*)((uintptr_t)recvbuf + offset),
                (void*)((uintptr_t)comm->rbuf[0] + offset), recvcounts[i] * recv_size);
      }
  }

  barrier(&comm->barr, g_tl_tid);

  return MPI_SUCCESS;
}


//TODO - the proper thing would be to have our own internal MPI comm for colls
#define HMPI_ALLTOALL_TAG 7546347

int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
  MPI_Aint send_extent, recv_extent, lb;
  void* rbuf;
  int32_t send_size;
  int32_t recv_size;
  uint64_t size;
  MPI_Request* send_reqs;
  MPI_Request* recv_reqs;
  MPI_Datatype dt_send;
  MPI_Datatype dt_recv;

  MPI_Type_size(sendtype, &send_size);

#if HMPI_SAFE
  MPI_Type_size(recvtype, &recv_size);

  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(send_size * sendcount != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif

#ifdef DEBUG
  printf("[%i] HMPI_Alltoall(%p, %i, %p, %p, %i, %p, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
  fflush(stdout);
#endif

  //TODO how do I change this to have each thread do local copies?
  // Maybe I should even just use a bunch of sends and receives.
  //  Then we do P*T sends and receives
  // Could go in between and build a MPI datatype to allow one send/recv
  //  per other process.

  //Each thread memcpy's into a single send buffer
  //Root thread creates contiguous datatypes to span each thread, then does alltoall
  //Each thread memcpy's out of a single receive buffer

  //Can use alltoallv, or replace it with sends/receives.
  //Either way:
  // Thread 0 mallocs shared buffer?

  uint64_t comm_size = g_nthreads * g_size;
  uint64_t data_size = send_size * sendcount;

  comm->sbuf[g_tl_tid] = sendbuf;

  //Alloc a temp buffer
  if(g_tl_tid == 0) {
      comm->mpi_sbuf = memalign(4096, data_size * g_nthreads * comm_size);
      comm->mpi_rbuf = memalign(4096, data_size * g_nthreads * comm_size);

      send_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_size);
      recv_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_size);

      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have g_nthreads per process
      // though, so we multiply by that.
      MPI_Type_contiguous(sendcount * g_nthreads * g_nthreads, sendtype, &dt_send);
      MPI_Type_commit(&dt_send);

      MPI_Type_contiguous(recvcount * g_nthreads * g_nthreads, recvtype, &dt_recv);
      MPI_Type_commit(&dt_recv);

      //Post receives
      int len = data_size * g_nthreads * g_nthreads;
      for(int i = 0; i < g_size; i++) {
          if(i != g_rank) {
              MPI_Irecv((void*)((uintptr_t)comm->mpi_rbuf + (len * i)), 1,
                      dt_recv, i, HMPI_ALLTOALL_TAG, comm->mpicomm, &recv_reqs[i]);
          }
      }
      recv_reqs[g_rank] = MPI_REQUEST_NULL;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //Copy into the shared send buffer on a stride by g_nthreads
  //This way our temp buffer has all the data going to proc 0, then proc 1, etc
  uintptr_t offset = g_tl_tid * data_size;
  uintptr_t scale = data_size * g_nthreads;

  //Verified from (now missing) prints, this is correct
  //TODO - try staggering
  // Data is pushed here -- remote thread can't read it
  for(uintptr_t i = 0; i < comm_size; i++) {
      if(!HMPI_Comm_local(comm, i)) {
          //Copy to send buffer to go out over network
          memcpy((void*)((uintptr_t)(comm->mpi_sbuf) + (scale * i) + offset),
                  (void*)((uintptr_t)sendbuf + data_size * i), data_size);
      }
  }

  //Start sends to each other rank
  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(g_tl_tid == 0) {
      int len = data_size * g_nthreads * g_nthreads;
      for(int i = 1; i < g_size; i++) {
          int r = (g_rank + i) % g_size;
          if(r != g_rank) {
              MPI_Isend((void*)((uintptr_t)comm->mpi_sbuf + (len * r)), 1,
                      dt_send, r, HMPI_ALLTOALL_TAG, comm->mpicomm, &send_reqs[r]);
          }
      }

      send_reqs[g_rank] = MPI_REQUEST_NULL;
  }

  //Pull local data from other threads' send buffers.
  //For each thread, memcpy from their send buffer into my receive buffer.
  int r = g_rank * g_nthreads; //Base rank
  for(uintptr_t thr = 0; thr < g_nthreads; thr++) {
      //Note careful use of addition by r to get the right offsets
      int t = (g_tl_tid + thr) % g_nthreads;
      memcpy((void*)((uintptr_t)recvbuf + ((r + t) * data_size)),
             (void*)((uintptr_t)comm->sbuf[t] + ((r + g_tl_tid) * data_size)),
             data_size);
  }

  //Wait on sends and receives to complete
  if(g_tl_tid == 0) {
      MPI_Waitall(g_size, recv_reqs, MPI_STATUSES_IGNORE);
      MPI_Waitall(g_size, send_reqs, MPI_STATUSES_IGNORE);
      MPI_Type_free(&dt_send);
      MPI_Type_free(&dt_recv);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //Do the MPI alltoall
#if 0
  if(g_tl_tid == 0) {
      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have g_nthreads per process
      // though, so we multiply by that.
      MPI_Type_contiguous(sendcount * g_nthreads * g_nthreads, sendtype, &dt_send);
      MPI_Type_commit(&dt_send);

      MPI_Type_contiguous(recvcount * g_nthreads * g_nthreads, recvtype, &dt_recv);
      MPI_Type_commit(&dt_recv);

      //This should now be alltoallv, with my mpi rank being 0 data,
      // and 1 for all other ranks... or can i stick with alltoall?  try both
      MPI_Alltoall((void*)comm->mpi_sbuf, 1, dt_send,
              (void*)comm->mpi_rbuf, 1, dt_recv, comm->mpicomm);

      MPI_Type_free(&dt_send);
      MPI_Type_free(&dt_recv);
  }

  barrier(&comm->barr);
  barrier_wait(&comm->barr);
#endif

  //Need to do g_size memcpy's -- one block of data per MPI process.
  // We copy g_nthreads * data_size at a time.
  offset = g_tl_tid * data_size * g_nthreads;
  scale = data_size * g_nthreads * g_nthreads;
  size = g_nthreads * data_size;

  for(uint64_t i = 0; i < g_size; i++) {
      if(i != g_rank) {
          memcpy((void*)((uintptr_t)recvbuf + size * i),
                  (void*)((uintptr_t)comm->mpi_rbuf + (scale * i) + offset),
                  size);
      }
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  if(g_tl_tid == 0) {
      free((void*)comm->mpi_sbuf);
      free((void*)comm->mpi_rbuf);
      free(send_reqs);
      free(recv_reqs);
  }

  return MPI_SUCCESS;
}


#if 0
int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
  MPI_Aint send_extent, recv_extent, lb;
  //void* rbuf;
  int32_t send_size;
  int32_t recv_size;
  //uint64_t t1, t2, tmp;
  //HRT_TIMESTAMP_T t1, t2;
  //uint64_t tmp;

//  PROFILE_START(g_profile_info[g_tl_tid], alltoall);

  MPI_Type_size(sendtype, &send_size);
  MPI_Type_size(recvtype, &recv_size);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(send_size * sendcount != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

#ifdef DEBUG
  printf("[%i] HMPI_Alltoall(%p, %i, %p, %p, %i, %p, %p)\n", g_rank*g_nthreads+g_tl_tid, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
#endif

  uint64_t data_size = send_size * sendcount;

  //Construct a vector type for the send side.
  // We use this to interleave the data from each local thread.

  MPI_Address(sendbuf, (MPI_Aint*)&comm->sbuf[g_tl_tid]);
  comm->scount[g_tl_tid] = sendcount;
  comm->stype[g_tl_tid] = sendtype;

  MPI_Address(recvbuf, (MPI_Aint*)&comm->rbuf[g_tl_tid]);
  comm->rcount[g_tl_tid] = recvcount * g_nthreads;
  comm->rtype[g_tl_tid] = recvtype;

  //PROFILE_START(g_profile_info[g_tl_tid], barrier);
  barrier(&comm->barr);
  barrier_wait(&comm->barr);
  //PROFILE_STOP(g_profile_info[g_tl_tid], barrier);

  //Do the MPI alltoall
  if(g_tl_tid == 0) {
      MPI_Datatype dt_send;
      MPI_Datatype dt_recv;
      MPI_Datatype dt_tmp;
      //MPI_Datatype dt_tmp2;
      MPI_Aint lb;
      MPI_Aint extent;

      //For the send side, we create an hindexed type to stride across each
      //thread's send buffer first, then build a contiguous type to group the
      //data for the different threads of each process.
      MPI_Type_create_hindexed(g_nthreads,
              (int*)comm->scount, (MPI_Aint*)comm->sbuf, sendtype, &dt_tmp);

      MPI_Type_get_extent(dt_tmp, &lb, &extent);
      MPI_Type_create_resized(dt_tmp, lb, data_size, &dt_send);
      //MPI_Type_contiguous(g_nthreads, dt_tmp2, &dt_send);

      MPI_Type_commit(&dt_send);
      MPI_Type_free(&dt_tmp);
      //MPI_Type_free(&dt_tmp2);

      //MPI_Type_commit(&dt_send);

      //For the receive side, we build a contiguous datatype (actually, just
      //the recvtype and recvcount * numthreads) representing all the data from
      //all threads on another process.  An hindexed type is used to split the
      //data across the receive buffers of each local thread.

      //We have g_nthreads receive buffers, one from each thread.
      //Each buffer holds recvcount * g_nthreads elements per process.
      MPI_Type_create_hindexed(g_nthreads,
              (int*)comm->rcount, (MPI_Aint*)comm->rbuf, recvtype, &dt_tmp);

      MPI_Type_get_extent(dt_tmp, &lb, &extent);
      MPI_Type_create_resized(dt_tmp, lb, data_size * g_nthreads, &dt_recv);

      MPI_Type_commit(&dt_recv);
      MPI_Type_free(&dt_tmp);

      MPI_Alltoall(MPI_BOTTOM, g_nthreads, dt_send,
              MPI_BOTTOM, 1, dt_recv, comm->mpicomm);

      MPI_Type_free(&dt_send);
      MPI_Type_free(&dt_recv);
  }

//  PROFILE_START(g_profile_info[g_tl_tid], barrier);
  barrier(&comm->barr);
  barrier_wait(&comm->barr);
//  PROFILE_STOP(g_profile_info[g_tl_tid], barrier);

//  PROFILE_STOP(g_profile_info[g_tl_tid], alltoall);
  return MPI_SUCCESS;
}
#endif


int HMPI_Abort( HMPI_Comm comm, int errorcode ) {
  printf("HMPI: user code called MPI_Abort!\n");
  return MPI_Abort(comm->mpicomm, errorcode);
}


int HMPI_Alltoall_local(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
    MPI_Aint send_extent, recv_extent, lb;
    int32_t send_size;
    int32_t recv_size;
    int thr, i;
    int tid = g_tl_tid;

    MPI_Type_size(sendtype, &send_size);

#if HMPI_SAFE
    MPI_Type_size(recvtype, &recv_size);

    MPI_Type_get_extent(sendtype, &lb, &send_extent);
    MPI_Type_get_extent(recvtype, &lb, &recv_extent);
    //MPI_Type_extent(sendtype, &send_extent);
    //MPI_Type_extent(recvtype, &recv_extent);

    if(send_extent != send_size || recv_extent != recv_size) {
        printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
        MPI_Abort(comm->mpicomm, 0);
    }

    if(send_size * sendcount != recv_size * recvcount) {
        printf("different send and receive size is not supported!\n");
        MPI_Abort(comm->mpicomm, 0);
    }
#endif

  comm->sbuf[tid] = sendbuf;
  //comm->scount[g_tl_tid] = sendcount;
  //comm->stype[g_tl_tid] = sendtype;

  //comm->rbuf[g_tl_tid] = recvbuf;
  //comm->rcount[g_tl_tid] = recvcount;
  //comm->rtype[g_tl_tid] = recvtype;


  //Do the self copy
  int copy_len = send_size * sendcount;
  memcpy((void*)((uintptr_t)recvbuf + (tid * copy_len)),
         (void*)((uintptr_t)sendbuf + (tid * copy_len)), copy_len);

  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  barrier(&comm->barr, tid);

  //Push local data to each other thread's receive buffer.
  //For each thread, memcpy from my send buffer into their receive buffer.

  //TODO - try staggering
  for(thr = 1; thr < g_nthreads; thr++) {
      int t = (tid + thr) % g_nthreads;
      memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)),
             (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len)) , copy_len);
      //memcpy((void*)((uintptr_t)comm->rbuf[thr] + (g_tl_tid * copy_len)),
      //       (void*)((uintptr_t)sendbuf + (thr * copy_len)) , copy_len);
  }

  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  barrier(&comm->barr, tid);
  return MPI_SUCCESS;
}


