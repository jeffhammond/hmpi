#define _GNU_SOURCE

#ifdef MPI
#define MPI_FOO
#undef MPI
#endif
#define HMPI_INTERNAL   //Disables HMPI->MPI renaming defines
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

#ifndef CACHE_LINE
#define CACHE_LINE 64
#endif

//Pin threads using either hwloc or pthreads.

//Using hwloc causes threads to be spread across sockets first, while pthreads
//will fill one socket before pinning threads to other sockets.  The hwloc
//configuration is currently faster for apps and peak bandwidth, while pthreads
//makes small-message latency look good.

//HMPI will die if pthreads are used and more threads than cores are requested.
//If neither is defined, threads are left to the whims of the OS.

//#define PIN_WITH_HWLOC 1
#define PIN_WITH_PTHREAD 1

#ifdef PIN_WITH_PTHREAD
//#define NUM_CORES 12
//#define NUM_SOCKETS 2
#endif


//Bluegene already does the smart thing in SMP mode, no pinning needed.
#ifdef __bg__
#undef PIN_WITH_HWLOC
#undef PIN_WITH_PTHREAD
#endif

//#define COMM_NTHREADS 1    //One communicator per thread
#define COMM_SQUARED 1       //nthreads^2 communicators

#ifdef COMM_SQUARED
#define SRC_DST_COMM(src, dst) g_tcomms[((src) * g_nthreads) + (dst)]
#endif //COMM_SQUARED


#ifdef COMM_NTHREADS
//Number of tag bits to reserve for HMPI to use internally for identifying
//source ranks when sending messages via MPI.  This puts a limit on how many
//threads HMPI can support: 2^SRC_TAG_BITS - 1
#define SRC_TAG_BITS 8

#define SRC_TAG_MASK ((1 << SRC_TAG_BITS) - 1) //Value to use with ANY_SOURCE

static int SRC_TAG_ANY = MPI_ANY_TAG; //Filled in during init

#endif //COMM_NTHREADS


//Block size to use when using the accelerated sender-receiver copy.
#define BLOCK_SIZE 8192


#include <pthread.h>
#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef PIN_WITH_HWLOC
#include <hwloc.h>
#endif
#include "lock.h"


#define MALLOC(t, s) (t*)memalign(CACHE_LINE, sizeof(t) * s)

//Wrappers to GCC/ICC extensions

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

//#define PREFETCH(x)
#define PREFETCH(x) __builtin_prefetch(x)


PROFILE_DECLARE();
PROFILE_VAR(memcpy);
PROFILE_VAR(allreduce);
PROFILE_VAR(op);



#ifdef PIN_WITH_HWLOC
static hwloc_topology_t g_hwloc_topo;
#endif


static int g_ncores;                //Cores per socket
static int g_nsockets;              //Sockets per node
int g_nthreads=-1;                  //Threads per node
int g_rank=-1;                      //Underlying MPI rank for this node
int g_size=-1;                      //Underlying MPI world size
__thread int g_hmpi_rank=-1;        //HMPI rank for this thread
__thread int g_tl_tid=-1;           //HMPI node-local rank for this thread (tid)

HMPI_Comm HMPI_COMM_WORLD;

static MPI_Comm* g_tcomms;
static lock_t g_mpi_lock;      //Used to protect ANY_SRC Iprobe/Recv sequence

//Argument passthrough
static int g_argc;
static char** g_argv;
static int (*g_entry)(int argc, char** argv);


#ifdef COMM_NTHREADS
//Unexpected message queue

struct HMPI_Message {
    HMPI_Item item;

    int proc;
    int tag;

    size_t size;
    void* buf;
};

typedef struct HMPI_Message HMPI_Message;

//Pool of unused reqs to save malloc time.
static __thread HMPI_Item* g_free_msgs = NULL;

//TODO - should this be thread-local?
static __thread HMPI_Item g_msgs_head = {NULL};
static __thread HMPI_Item* g_msgs_tail = NULL;

static inline HMPI_Message* acquire_msg(void)
{
    //Malloc a new msg only if none are in the pool.
    if(g_free_msgs == NULL) {
        return MALLOC(HMPI_Message, 1);
    } else {
        HMPI_Item* item = g_free_msgs;
        g_free_msgs = item->next;
        return (HMPI_Message*)item;
    }
}


static inline void release_msg(HMPI_Message* msg)
{
    //Return a msg to the pool -- once allocated, a msg is never freed.
    HMPI_Item* item = (HMPI_Item*)msg;

    item->next = g_free_msgs;
    g_free_msgs = item;
}


static inline void add_msg(HMPI_Message* msg)
{
    HMPI_Item* item = (HMPI_Item*)msg;

    item->next = NULL;
    g_msgs_tail->next = item;
    g_msgs_tail = item;
}

static inline void remove_msg(HMPI_Item* prev, HMPI_Item* cur) {
    if(cur == g_msgs_tail) {
        g_msgs_tail = prev;
        prev->next = NULL;
    } else {
        prev->next = cur->next;
    }
}


#endif



//Each thread has a list of send and receive requests.
//The receive requests are managed privately by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them in match_recv().

static __thread HMPI_Item g_recv_reqs_head = {NULL};
static __thread HMPI_Item* g_recv_reqs_tail = NULL;


//The send request lists are based on the MCS lock, see:
// Algorithms for Scalable Synchronization on Shared-Memory Multiprocessors
// by John Mellor-Crummey and Michael Scott
//Rather than protecting with a lock, the list itself is implemented like an
//MCS spin lock.  add_send_req() is equivalent to acquiring the lock, but our
//version doesn't block if there are already nodes in the queue (or in MCS
//terms, the lock is already 'held').  match_recv() contains code to remove a
//send request from the queue, which is equivalen to releasing the lock.  Our
//version is a little different in that we try to swap in the previous req for
//the tail rather than NULL.

typedef struct HMPI_Request_list {
    HMPI_Item head;
    HMPI_Item* tail;
    mcs_lock_t lock;
    //Pad to a cache line so locks don't conflict - no benefit?
    //uint8_t pad[36];
} HMPI_Request_list;

static HMPI_Request_list* g_send_reqs = NULL;       //Senders add sends here
static __thread HMPI_Request_list* g_tl_my_send_reqs;    //Shortcut
static __thread HMPI_Request_list g_tl_send_reqs;   //Receiver-local send Q

//Pool of unused reqs to save malloc time.
//TODO - allocate in blocks for locality and more speed?
static __thread HMPI_Item* g_free_reqs = NULL;


static inline HMPI_Request acquire_req(void)
{
    //Malloc a new req only if none are in the pool.
    if(g_free_reqs == NULL) {
        return (HMPI_Request)MALLOC(HMPI_Request_info, 1);
    } else {
        HMPI_Item* item = g_free_reqs;
        g_free_reqs = item->next;
        return (HMPI_Request)item;
    }
}


static inline void release_req(HMPI_Request req)
{
    //Return a req to the pool -- once allocated, a req is never freed.
    HMPI_Item* item = (HMPI_Item*)req;

    item->next = g_free_reqs;
    g_free_reqs = item;
}


static inline void add_send_req(HMPI_Request_list* req_list,
                                HMPI_Request req) {
    //Insert req at tail.
    HMPI_Item* item = (HMPI_Item*)req;

    //item->next = NULL; //TODO - could skip this

    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&req_list->lock, &q);
    req_list->tail->next = item;
    req_list->tail = item;
    MCS_LOCK_RELEASE(&req_list->lock, &q);
}


static inline void remove_send_req(HMPI_Request_list* req_list,
                                   HMPI_Item* prev, HMPI_Item* cur)
{
    //Since we only remove from the receiver-local send Q, there is no need for
    //locking.
    if(cur->next == NULL) {
        req_list->tail = prev;
        prev->next = NULL;
    } else {
        prev->next = cur->next;
    }
}


static inline void update_send_reqs(HMPI_Request_list* local_list)
{
    //HMPI_Request_list* req_list = &g_send_reqs[g_tl_tid];
    HMPI_Request_list* req_list = g_tl_my_send_reqs;

    if(req_list->tail != &req_list->head) {
        //g_tl_send_reqs.tail->next = req_list->head.next;
        local_list->tail->next = req_list->head.next;

        mcs_qnode_t q;
        MCS_LOCK_ACQUIRE(&req_list->lock, &q);

        //g_tl_send_reqs.tail = req_list->tail;
        local_list->tail = req_list->tail;
        req_list->tail = &req_list->head;

        MCS_LOCK_RELEASE(&req_list->lock, &q);

        //g_tl_send_reqs.tail->next = NULL;
        local_list->tail->next = NULL;
    }
}



static inline void add_recv_req(HMPI_Request req) {
    HMPI_Item* item = (HMPI_Item*)req;

    //Add at tail to ensure matching occurs in order.
    item->next = NULL;
    g_recv_reqs_tail->next = item;
    g_recv_reqs_tail = item;

}


static inline void remove_recv_req(HMPI_Item* prev, HMPI_Item* cur) {
    if(cur->next == NULL) {
        g_recv_reqs_tail = prev;
        prev->next = NULL;
    } else {
        prev->next = cur->next;
    }
}


#ifdef COMM_NTHREADS
static inline HMPI_Message* match_queue(HMPI_Request recv_req) {
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Message* msg;

    int proc = recv_req->proc;
    int tag = recv_req->tag;

    for(prev = &g_msgs_head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        msg = (HMPI_Message*)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG, so don't check.
        if((msg->proc == proc || proc == MPI_ANY_SOURCE) &&
                (msg->tag == tag || tag == MPI_ANY_TAG)) {

            remove_msg(prev, cur);

            recv_req->proc = msg->proc;
            recv_req->tag = msg->tag;
            return msg;
        }
    }

    return NULL;
}
#endif //COMM_NTHREADS


//Match for receives that are *NOT* ANY_SOURCE
static inline HMPI_Request match_recv(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;
    //HMPI_Request_list* req_list = &g_tl_send_reqs;

    int proc = recv_req->proc;
    int tag = recv_req->tag;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        PREFETCH(cur->next);
        req = (HMPI_Request)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG, so don't check.
        if(req->proc == proc && (req->tag == tag || tag == MPI_ANY_TAG)) {
            remove_send_req(req_list, prev, cur);

            //recv_req->proc = req->proc; //Not necessary, no ANY_SRC
            recv_req->tag = req->tag;
            return req;
        }
    }

    return HMPI_REQUEST_NULL;
}


//Match for receives with ANY_SOURCE.
static inline HMPI_Request match_recv_any(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;
    //HMPI_Request_list* req_list = &g_tl_send_reqs;

    int tag = recv_req->tag;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        PREFETCH(cur->next);
        req = (HMPI_Request)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG, so don't check.
        if(req->tag == tag || tag == MPI_ANY_TAG) {
            remove_send_req(req_list, prev, cur);

            recv_req->proc = req->proc;
            recv_req->tag = req->tag;
            return req;
        }
    }

    return HMPI_REQUEST_NULL;
}


static inline int match_probe(int source, int tag, HMPI_Comm comm, HMPI_Request* send_req) {
    HMPI_Item* cur;
    HMPI_Request req;
    HMPI_Request_list* req_list = &g_tl_send_reqs;

    update_send_reqs(req_list);

    for(cur = req_list->head.next; cur != NULL; cur = cur->next) {
        PREFETCH(cur->next);
        req = (HMPI_Request)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG,
        // so don't check for that.
        if((req->proc == source || source == MPI_ANY_SOURCE) &&
                (req->tag == tag || tag == MPI_ANY_TAG)) {
            //We don't want to do anything other than return the send req.
            *send_req = req;
            return 1;
        }
    }

    return 0;
}


static inline void update_reqstat(HMPI_Request req, int stat) {
  req->stat = stat;
}


static inline int get_reqstat(HMPI_Request req) {
  return req->stat;
}


// this is called by pthread create and then calls the real function!
void* trampoline(void* tid) {
    int rank = (int)(uintptr_t)tid;

    {
#ifdef PIN_WITH_HWLOC
        //Spread threads evenly across cores
        //Each thread should bind itself according to its rank.
        // Compute rs = num_ranks / num_sockets
        //         c = r % rs, s = r / rs
        //         idx = s * num_cores + c
        hwloc_obj_t obj;
        hwloc_cpuset_t cpuset;

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
#endif

#ifdef PIN_WITH_PTHREAD
        //int num_sockets = NUM_SOCKETS;
        //int num_cores = NUM_CORES;
#endif //PIN_WITH_PTHREAD

#if 0
        if(num_cores < g_nthreads) {
            printf("%d ERROR requested %d threads but only %d cores\n",
                    g_rank, g_nthreads, num_cores);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }
#endif

        int core = rank % (g_ncores * g_nsockets);
        //int num_cores = g_ncores / g_nsockets; //cores per socket


        //Spread ranks evenly across sockets, grouping adjacent ranks into one
        //socket.
        int idx;
        int rms = g_nthreads % g_nsockets;
        int rs = (g_nthreads / g_nsockets) + 1;
        if(core < rms * rs) {
            idx = ((core / rs) * g_ncores) + (core % rs);
        } else {
            int rmd = core - (rms * rs);
            rs -= 1;
            idx = (rmd / rs) * g_ncores + (rmd % rs) + rms * g_ncores;
        }

        printf("Rank %d binding to index %d\n", rank, idx);

#ifdef PIN_WITH_HWLOC
        obj = hwloc_get_obj_by_type(g_hwloc_topo, HWLOC_OBJ_CORE, idx);
        if(obj == NULL) {
            printf("%d ERROR got NULL hwloc core object idx %d\n", core, idx);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }

        cpuset = hwloc_bitmap_dup(obj->cpuset);
        hwloc_bitmap_singlify(cpuset);

        hwloc_set_cpubind(g_hwloc_topo, cpuset, HWLOC_CPUBIND_THREAD);
        hwloc_set_membind(g_hwloc_topo,
                cpuset, HWLOC_MEMBIND_BIND, HWLOC_CPUBIND_THREAD);
#endif //PIN_WITH_HWLOC

#ifdef PIN_WITH_PTHREAD
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(idx, &cpuset);
        int ret =
            pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
        if(ret) {
            printf("ERROR in pthread_setaffinity_np: %s\n", strerror(ret));
            MPI_Abort(MPI_COMM_WORLD, 0);
        }
#endif //PIN_WITH_PTHREAD
    }


    //The application may modify argv/argc, so we need to make our own for the
    //app to trample on.
    char** argv = MALLOC(char*, g_argc);
    for(int i = 0; i < g_argc; i++) {
        //The + 1 takes care of the NULL byte.
        int len = strlen(g_argv[i]) + 1;
        argv[i] = MALLOC(char, len);
        strncpy(argv[i], g_argv[i], len);
    }


    // save thread-id in thread-local storage
    g_tl_tid = rank;
    g_hmpi_rank = g_rank*g_nthreads+rank;

    // Initialize send requests list and lock
    g_recv_reqs_tail = &g_recv_reqs_head;

    g_send_reqs[rank].head.next = NULL;
    g_send_reqs[rank].tail = &g_send_reqs[rank].head;
    g_tl_my_send_reqs = &g_send_reqs[rank];
    MCS_LOCK_INIT(&g_send_reqs[rank].lock);
    g_tl_send_reqs.head.next = NULL;
    g_tl_send_reqs.tail = &g_tl_send_reqs.head;

#ifdef COMM_NTHREADS
    g_msgs_tail = &g_msgs_head;
#endif

    PROFILE_INIT(g_tl_tid);

    // Don't head off to user land until all threads are ready 
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);

    // call user function
    g_entry(g_argc, argv);

    //Free up our version of argv
    for(int i = 0; i < g_argc; i++) {
        free(argv[i]);
    }

    free(argv);

    return NULL;
}


int HMPI_Init(int *argc, char ***argv, int (*start_routine)(int argc, char** argv), int nthreads, int ncores, int nsockets)
{
  pthread_t* threads;
  int provided;
  long int thr;

  //MPI_Init(argc, argv);
  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
  assert(MPI_THREAD_MULTIPLE == provided);

  printf("req size %ld\n", sizeof(HMPI_Request_info));
  fflush(stdout);

#ifdef COMM_NTHREADS
  //Make sure we can support the requested number of threads.
  if(nthreads >= (1 << SRC_TAG_BITS) - 1) {
      printf("ERROR requested %d threads, but SRC_TAG_BITS (%d) limits to %d threads\n",
              nthreads, SRC_TAG_BITS, (1 << SRC_TAG_BITS) - 2);
      MPI_Abort(MPI_COMM_WORLD, 0);
  }

  {
    int flag;

    MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_TAG_UB, &SRC_TAG_ANY, &flag);

    if(!flag) {
        printf("ERROR coulnd't get TAG_UB\n");
        MPI_Abort(MPI_COMM_WORLD, 0);
    }

    SRC_TAG_ANY = (MPI_ANY_TAG - (1 << SRC_TAG_BITS)) ^ SRC_TAG_MASK;
  }
#endif

  g_argc = *argc;
  g_argv = *argv; 
  g_entry = start_routine;
  g_nthreads = nthreads;
  g_ncores = ncores;
  g_nsockets = nsockets;
  LOCK_INIT(&g_mpi_lock, 0);

  printf("threads %d cores %d sockets %d\n", g_nthreads, g_ncores, g_nsockets);
  fflush(stdout);

  HMPI_COMM_WORLD = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
  HMPI_COMM_WORLD->mpicomm = MPI_COMM_WORLD;
  barrier_init(&HMPI_COMM_WORLD->barr, nthreads);

  HMPI_COMM_WORLD->sbuf = MALLOC(volatile void*, nthreads);
  HMPI_COMM_WORLD->rbuf = MALLOC(volatile void*, nthreads);
  HMPI_COMM_WORLD->scount = MALLOC(int, nthreads);
  HMPI_COMM_WORLD->rcount = MALLOC(int, nthreads);
  HMPI_COMM_WORLD->stype = MALLOC(MPI_Datatype, nthreads);
  HMPI_COMM_WORLD->rtype = MALLOC(MPI_Datatype, nthreads);

  threads = MALLOC(pthread_t, nthreads);

  g_send_reqs = MALLOC(HMPI_Request_list, nthreads);


#ifdef COMM_NTHREADS
  g_tcomms = MALLOC(MPI_Comm, nthreads);

  for(thr = 0; thr < nthreads; thr++) {
    //Duplicate COMM_WORLD for this thread.
    MPI_Comm_dup(MPI_COMM_WORLD, &g_tcomms[thr]);
  }
#endif

#ifdef COMM_SQUARED
  g_tcomms = MALLOC(MPI_Comm, nthreads * nthreads);

  for(int i = 0; i < nthreads; i++) {
      for(int j = 0; j < nthreads; j++) {
        MPI_Comm_dup(MPI_COMM_WORLD, &SRC_DST_COMM(i, j));
      }
  }
#endif


  MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &g_size);

  //MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

  //Ranks set their own affinity in trampoline.
#ifdef PIN_WITH_HWLOC
  hwloc_topology_init(&g_hwloc_topo);
  hwloc_topology_load(g_hwloc_topo);
#endif

#if 0
#ifdef PIN_WITH_PTHREAD
  {
    threads[0] = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    int ret = pthread_setaffinity_np(threads[0], sizeof(cpuset), &cpuset);
    if(ret) {
        printf("ERROR in pthread_setaffinity_np: %s\n", strerror(ret));
        MPI_Abort(MPI_COMM_WORLD, 0);
    }
  }
#endif
#endif

  //Bluegene/P has a 1-thread/core limit, so this thread will run as rank 0.
  for(thr=1; thr < nthreads; thr++) {
    //Create the thread
    int ret = pthread_create(&threads[thr], NULL, trampoline, (void *)thr);
    if(ret) {
        printf("ERROR in pthread_create: %s\n", strerror(ret));
        MPI_Abort(MPI_COMM_WORLD, 0);
    }

#if 0
#ifdef PIN_WITH_PTHREAD
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thr, &cpuset);

    ret = pthread_setaffinity_np(threads[thr], sizeof(cpuset), &cpuset);
    if(ret) {
        printf("ERROR in pthread_setaffinity_np: %s\n", strerror(ret));
        MPI_Abort(MPI_COMM_WORLD, 0);
    }
#endif
#endif
  }

  trampoline((void*)0);

  for(thr=1; thr<nthreads; thr++) {
    pthread_join(threads[thr], NULL);
  }

  free(g_send_reqs);
  free(threads);
  free(g_tcomms);
  return 0;
}


int HMPI_Comm_rank(HMPI_Comm comm, int *rank) {
  
#ifdef HMPI_SAFE 
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif

  *rank = g_hmpi_rank;
  return 0;
}


int HMPI_Comm_size ( HMPI_Comm comm, int *size ) {
#ifdef HMPI_SAFE 
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
    
  *size = g_size*g_nthreads;
  return 0;
}


int HMPI_Finalize() {

  HMPI_Barrier(HMPI_COMM_WORLD);

  PROFILE_SHOW_REDUCE(allreduce);
  PROFILE_SHOW_REDUCE(op);
  PROFILE_SHOW_REDUCE(memcpy);

  barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);

  //Free the local request pool
  for(HMPI_Item* cur = g_free_reqs; cur != NULL;) {
      HMPI_Item* next = cur->next;
      free(cur);
      cur = next;
  }

#ifdef COMM_NTHREADS
  for(HMPI_Item* cur = g_free_msgs; cur != NULL;) {
      HMPI_Item* next = cur->next;
      free(cur);
      cur = next;
  }
#endif

  if(g_tl_tid == 0) {
    MPI_Finalize();
  }

  return 0;
}


//We assume req->type == HMPI_SEND and req->stat == 0 (uncompleted send)
static inline int HMPI_Progress_send(HMPI_Request send_req)
{
    if(get_reqstat(send_req) == HMPI_REQ_COMPLETE) {
        return HMPI_REQ_COMPLETE;
    }

    //Write blocks on this send req if receiver has matched it.
    //If mesage is short, receiver won't bother clearing the match lock, and
    // instead just does the copy and marks completion.
    if(LOCK_TRY(&send_req->match)) {
        HMPI_Request recv_req = (HMPI_Request)send_req->u.local.match_req;
        uintptr_t rbuf = (uintptr_t)recv_req->buf;

        size_t send_size = send_req->size;
        size_t recv_size = recv_req->size;
        size_t size = (send_size < recv_size ? send_size : recv_size);

        uintptr_t sbuf = (uintptr_t)send_req->buf;
        volatile ssize_t* offsetptr = &send_req->u.local.offset;
        ssize_t offset;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(offsetptr, BLOCK_SIZE)) < size) {
            size_t left = size - offset;
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }

        //Signal that the sender is done copying.
        //Possible for the receiver to still be copying here.
        LOCK_CLEAR(&send_req->match);

        //Receiver will set completion soon, wait rather than running off.
        while(get_reqstat(send_req) != HMPI_REQ_COMPLETE);
        return HMPI_REQ_COMPLETE;
    }

    return HMPI_REQ_ACTIVE;
}


//For req->type == HMPI_RECV
static inline void HMPI_Complete_recv(HMPI_Request recv_req, HMPI_Request send_req)
{
    size_t send_size = send_req->size;
    size_t size = recv_req->size;

    if(send_size < size) {
        //Adjust receive count
        recv_req->size = send_size;
        size = send_size;
    }


#if 0
    if(unlikely(send_size > size)) {
        printf("%d ERROR recv message from %d of size %ld truncated to %ld\n", g_hmpi_rank, send_req->proc, send_size, size);
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 5);
    }
#endif

    if(size < BLOCK_SIZE * 2) {
        memcpy((void*)recv_req->buf, send_req->buf, size);
    } else {
        //The setting of send_req->match_req signals to sender that they can
        // start doing copying as well, if they are testing the req.

        //recv_req->match_req = send_req; //TODO - keep this here?
        send_req->u.local.match_req = recv_req;
        send_req->u.local.offset = 0;
        LOCK_CLEAR(&send_req->match);

        uintptr_t rbuf = (uintptr_t)recv_req->buf;
        uintptr_t sbuf = (uintptr_t)send_req->buf;
        volatile ssize_t* offsetptr = &send_req->u.local.offset;
        ssize_t offset = 0;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(offsetptr, BLOCK_SIZE)) < size) {
            size_t left = size - offset;

            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }

        //Wait if the sender is copying.
        LOCK_SET(&send_req->match);
    }

    //Mark send and receive requests done
    update_reqstat(send_req, HMPI_REQ_COMPLETE);
    update_reqstat(recv_req, HMPI_REQ_COMPLETE);

#ifdef DEBUG
    printf("[%d] completed local-level RECV buf %p size %lu source %d tag %d\n",
            g_hmpi_rank, recv_req->buf, recv_req->size, recv_req->proc, recv_req->tag);
    printf("[%d] completed local-level SEND buf %p size %lu dest %d tag %d\n",
            send_req->proc, send_req->buf, send_req->size, g_hmpi_rank, send_req->tag);
    fflush(stdout);
#endif
}


//For req->type == MPI_SEND || req->type == MPI_RECV
static inline int HMPI_Progress_mpi(HMPI_Request req)
{
    int flag;
    MPI_Status status;

#if 0
    int err;
    if((err = MPI_Test(&req->req, &flag, &status))) {
        char str[4096];
        int len;

        MPI_Error_string(err, str, &len);

        printf("%d MPI_Test ERROR %s\n", g_hmpi_rank, str);
        printf("%d req type %d proc %d tag %d size %ld buf %p\n", g_hmpi_rank,
                req->type, req->proc, req->tag, req->size, req->buf);
        fflush(stdout);

        assert(0);
        MPI_Abort(MPI_COMM_WORLD, 0);
    }
#endif

#ifdef COMM_NTHREADS
    //Have to check message queue for matches first.
    HMPI_Message* msg = match_queue(req);
    if(msg != NULL) {
        //Matched a queued message!
        //Copy the data over.
#ifdef HMPI_SAFE
        if(unlikely(msg->size > req->size)) {
            printf("[HMPI recv %d] message from %d of size %ld truncated to %ld\n", g_hmpi_rank, req->proc, msg->size, req->size);

        }
#endif

        req->size = msg->size;

        memcpy(req->buf, msg->buf, msg->size);

        free(msg->buf);
        release_msg(msg);

        update_reqstat(req, HMPI_REQ_COMPLETE);
        return 1;
    }

#endif //COMM_NTHREADS

    MPI_Test(&req->u.remote.req, &flag, MPI_STATUS_IGNORE);

    if(flag) {
        //Update status
        int count;
        MPI_Get_count(&status, req->datatype, &count);

        int type_size;
        MPI_Type_size(req->datatype, &type_size);

        //ANY_SOURCE isn't possible here, so proc is already correct.
        req->size = count * type_size;
#ifdef COMM_NTHREADS
        //TODO - deal with ANY_TAG properly
        req->tag = status.MPI_TAG >> SRC_TAG_BITS;
#endif

#ifdef COMM_SQUARED
        req->tag = status.MPI_TAG;
#endif

        update_reqstat(req, HMPI_REQ_COMPLETE);
    }

#ifdef DEBUG
    if(flag) {
        if(req->type == MPI_SEND) {
            printf("[%d] completed MPI-level SEND %d proc %d tag %d\n", g_hmpi_rank, req->type, req->proc, req->tag);
        } else {
            printf("[%d] completed MPI-level RECV %d proc %d tag %d\n", g_hmpi_rank, req->type, req->proc, req->tag);
        }
        fflush(stdout);
    }
#endif

    return flag;
}


static inline void HMPI_Progress() {
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;
    HMPI_Request_list* req_list = &g_tl_send_reqs;

    update_send_reqs(req_list);

    //Progress receive requests.
    //We remove items from the list, but they are still valid; nothing in this
    //function will free or modify a req.  So, it's safe to do cur = cur->next.
    //Note the careful updating of prev; we need to leave it alone on iterations
    //where cur is matched successfully and only update it otherwise.
    // This prevents the recv_reqs list from getting corrupted due to a bad
    // prev pointer.
    for(prev = &g_recv_reqs_head, cur = prev->next;
            cur != NULL; cur = cur->next) {
        PREFETCH(cur->next);
        req = (HMPI_Request)cur;

        if(likely(req->type == HMPI_RECV)) {
            HMPI_Request send_req = match_recv(req_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            }
        } else { //req->type == HMPI_RECV_ANY_SOURCE
#ifdef COMM_NTHREADS
            //Have to check message queue for matches first.
            HMPI_Message* msg = match_queue(req);
            if(msg != NULL) {
                //Matched a queued message!
                //Copy the data over.
#ifdef HMPI_SAFE
                if(unlikely(msg->size > req->size)) {
                    printf("[HMPI recv %d] message from %d of size %ld truncated to %ld\n", g_hmpi_rank, req->proc, msg->size, req->size);

                }
#endif

                req->size = msg->size;

                memcpy(req->buf, msg->buf, msg->size);

                free(msg->buf);
                release_msg(msg);

                remove_recv_req(prev, cur);
                update_reqstat(req, HMPI_REQ_COMPLETE);
                continue; //Whenever we remove a req, dont update prev
            }
#endif //COMM_NTHREADS

            HMPI_Request send_req = match_recv_any(req_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            } else if(g_size > 1) {
                printf("%d offnode recv!\n", g_hmpi_rank);
#ifdef COMM_NTHREADS
                // check if we can get something via the MPI library
                //For ANY_SOURCE, we have to use ANY_TAG to get at the tag and
                //match on it.  If not a match, we have a message sitting at the
                //front of the queue.  Allocate a buffer and post a receive,
                //then save that message on an unexpected Q.

                //So, probe with ANY_SRC, ANY_TAG on the right comm.
                //Check the tag -- if tag matches, YAY.
                //If not, add to unexpected Q.

                int flag=0;
                MPI_Status status;
                int tag = req->tag;

                LOCK_SET(&g_mpi_lock);
                MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG,
                        g_tcomms[tid], &flag, &status);

                int mpi_tag = status.MPI_TAG >> SRC_TAG_BITS;
                if(!flag) {
                    LOCK_CLEAR(&g_mpi_lock);
                } else if(mpi_tag != tag && tag != SRC_TAG_ANY) {
                    //Got a message but it doesn't match -- Q it.
                    HMPI_Message* msg = acquire_msg();

                    //TODO - limited to 2gb unexpected msgs
                    int count;
                    MPI_Get_count(&status, MPI_BYTE, &count);

                    //TODO - something smarter than malloc
                    msg->buf = MALLOC(char, count);

                    MPI_Recv(msg->buf, count, MPI_BYTE, status.MPI_SOURCE,
                            status.MPI_TAG, g_tcomms[tid], &status);

                    LOCK_CLEAR(&g_mpi_lock);

                    //Done outside the lock to make critical section shorter.
                    msg->proc = status.MPI_SOURCE * g_nthreads +
                            (tag & SRC_TAG_MASK);
                    msg->tag = mpi_tag;
                    msg->size = count;

                    add_msg(msg);
                } else {
                    //Got a matching message!
                    int count;
                    MPI_Get_count(&status, req->datatype, &count);

                    MPI_Recv(req->buf, count, req->datatype,
                          status.MPI_SOURCE, status.MPI_TAG,
                          g_tcomms[tid], &status);
                    LOCK_CLEAR(&g_mpi_lock);

                    int type_size;
                    MPI_Type_size(req->datatype, &type_size);

                    req->size = count * type_size;
                    req->proc = status.MPI_SOURCE * g_nthreads +
                            (tag & SRC_TAG_MASK);
                    //TODO - deal with ANY_TAG properly
                    req->tag = mpi_tag;

                    update_reqstat(req, HMPI_REQ_COMPLETE);
                    remove_recv_req(prev, cur);
                    continue; //Whenever we remove a req, dont update prev
                }

#endif

#ifdef COMM_SQUARED
                //Poll every comm we receive on, and grab the first message
                //that matches.
                //Locking across the iprobe+recv isn't necessary since we're
                //the only thread receiving on these comms.
                int flag = 0;
                MPI_Status status;

                for(int dst = 0; dst < g_nthreads; dst++) {
                    for(int src = 0; src < g_nthreads; src++) {
                        MPI_Iprobe(MPI_ANY_SOURCE, req->tag,
                                SRC_DST_COMM(src, dst), &flag, &status);

                        if(flag) {
                            int count;
                            MPI_Get_count(&status, req->datatype, &count);

                            MPI_Recv(req->buf, count, req->datatype,
                                    status.MPI_SOURCE, status.MPI_TAG,
                                    SRC_DST_COMM(src, dst), &status);

                            int type_size;
                            MPI_Type_size(req->datatype, &type_size);

                            req->size = count * type_size;
                            req->proc = status.MPI_SOURCE * g_nthreads + src;
                            req->tag = status.MPI_TAG;

                            update_reqstat(req, HMPI_REQ_COMPLETE);
                            remove_recv_req(prev, cur);

                            //Yeah, goto is bad, but I want to break out of
                            //both dst/src for loops quickly here.
                            //Also careful to skip updating prev here.
                            goto main_loop;
                        }
                    }
                }
#endif // COMM_SQUARED
            }
        }

        //Update prev -- we only do this if cur wasn't matched.
        prev = cur;

#ifdef COMM_SQUARED
main_loop:
        continue; //Needed to avoid weird compiler error with the label
#endif
    }
}



int HMPI_Test(HMPI_Request *request, int *flag, HMPI_Status *status)
{
    HMPI_Request req = *request;

    if(unlikely(req == HMPI_REQUEST_NULL)) {
        if(status != HMPI_STATUS_IGNORE) {
            //Make Get_count return 0 count
            status->size = 0;
        }

        *flag = 1;
        return MPI_SUCCESS;
    } else if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
        int f;

        HMPI_Progress();

        if(req->type == HMPI_SEND) {
            f = HMPI_Progress_send(req);
        } else if(req->type == HMPI_RECV) {
            f = get_reqstat(req);
        } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
            f = HMPI_Progress_mpi(req);
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            f = get_reqstat(req);
        }

        if(f != HMPI_REQ_COMPLETE) {
            *flag = 0;
            return MPI_SUCCESS;
        }
    }

    if(status != HMPI_STATUS_IGNORE) {
        status->size = req->size;
        status->MPI_SOURCE = req->proc;
        status->MPI_TAG = req->tag;
        status->MPI_ERROR = MPI_SUCCESS;
    }

    release_req(req);
    *request = HMPI_REQUEST_NULL;
    *flag = 1;
    return MPI_SUCCESS;
}


int HMPI_Testall(int count, HMPI_Request *requests, int* flag, HMPI_Status *statuses)
{
    *flag = 1;

    //Return as soon as any one request isn't complete.
    //TODO - poll each request anyway, to try and progress?
    for(int i = 0; i < count && *flag; i++) {
        if(requests[i] == HMPI_REQUEST_NULL) {
            continue;
        } else if(statuses == HMPI_STATUSES_IGNORE) {
            HMPI_Test(&requests[i], flag, HMPI_STATUS_IGNORE);
        } else {
            HMPI_Test(&requests[i], flag, &statuses[i]);
        }

        if(!(*flag)) {
            return MPI_SUCCESS;
        }
    }

    return MPI_SUCCESS;
}


static inline void HMPI_Wait_recv(HMPI_Request req)
{
//    do {
//        HMPI_Progress();
//    } while(get_reqstat(req) != HMPI_REQ_COMPLETE);

    HMPI_Progress();
    if(req->stat == HMPI_REQ_COMPLETE) {
        return;
    }

    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request_list* req_list = &g_tl_send_reqs;

    for(prev = &g_recv_reqs_head, cur = prev->next;
            cur != NULL; cur = cur->next) {
        if(cur == (HMPI_Item*)req) {
            break;
        }
    }

    do {
        update_send_reqs(req_list);

        HMPI_Request send_req = match_recv(req_list, req);
        if(send_req != HMPI_REQUEST_NULL) {
            HMPI_Complete_recv(req, send_req);

            remove_recv_req(prev, cur);
            return;
        }
    } while(1);
}


int HMPI_Wait(HMPI_Request *request, HMPI_Status *status) {
#ifdef DEBUG
    printf("[%i] HMPI_Wait(%x, %x) type: %i\n", g_hmpi_rank, req, status, (*request)->type);
    fflush(stdout);
#endif

    HMPI_Request req = *request;

    if(unlikely(req == HMPI_REQUEST_NULL)) {
        if(status != HMPI_STATUS_IGNORE) {
            //Make Get_count return 0 count
            status->size = 0;
        }

        return MPI_SUCCESS;
    }

    if(req->type == HMPI_SEND) {
        do {
            HMPI_Progress();
        } while(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE);
    } else if(req->type == HMPI_RECV) {
        do {
            HMPI_Progress();
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
        //HMPI_Wait_recv(req);
    } else if(req->type == MPI_RECV || req->type == MPI_SEND) {
        do {
            HMPI_Progress();
        } while(HMPI_Progress_mpi(req) != HMPI_REQ_COMPLETE);
    } else { //HMPI_RECV_ANY_SOURCE
        do {
            HMPI_Progress();
            //sched_yield();
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
    }

    //Req is complete at this point
    if(status != HMPI_STATUS_IGNORE) {
        status->size = req->size;
        status->MPI_SOURCE = req->proc;
        status->MPI_TAG = req->tag;
        status->MPI_ERROR = MPI_SUCCESS;
    }

    release_req(req);
    *request = HMPI_REQUEST_NULL;
    return MPI_SUCCESS;
}


int HMPI_Waitall(int count, HMPI_Request *requests, HMPI_Status *statuses)
{
    int done;

#ifdef DEBUG
    printf("[%i] HMPI_Waitall(%d, %p, %p)\n", g_hmpi_rank, count, requests, statuses);
    fflush(stdout);
#endif

    do {
        HMPI_Progress();
        done = 0;

        for(int i = 0; i < count; i++) {
            HMPI_Request req = requests[i];

            if(req == HMPI_REQUEST_NULL) {
                done += 1;
                continue;
            }

            if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
                if(req->type == HMPI_SEND) {
                    if(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE) {
                        continue;
                    }
                } else if(req->type == HMPI_RECV) {
                    continue;
                } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
                    if(!HMPI_Progress_mpi(req)) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            //req is complete but status not handled
            if(statuses != HMPI_STATUSES_IGNORE) {
                HMPI_Status* st = &statuses[i];
                st->size = req->size;
                st->MPI_SOURCE = req->proc;
                st->MPI_TAG = req->tag;
                st->MPI_ERROR = MPI_SUCCESS;
            }

            release_req(req);
            requests[i] = HMPI_REQUEST_NULL;
            done += 1;
        }
    } while(done < count);

  return MPI_SUCCESS;
}


int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status)
{
    int done;
    int iters = 0;

    //Call Progress once, then check each req.
    do {
        done = 1;

        HMPI_Progress();

        for(int i = 0; i < count; i++) {
            HMPI_Request req = requests[i];
            if(req == HMPI_REQUEST_NULL) {
                continue;
            }

            //Not done! at least one req was not NULL.
            done = 0;

            //Check completion of the req.
            if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
                if(req->type == HMPI_SEND) {
                    if(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE) {
                        continue;
                    }
                } else if(req->type == HMPI_RECV) {
                    continue;
                } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
                    if(!HMPI_Progress_mpi(req)) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            //req is complete, handle status.
            if(status != HMPI_STATUS_IGNORE) {
                status->size = req->size;
                status->MPI_SOURCE = req->proc;
                status->MPI_TAG = req->tag;
                status->MPI_ERROR = MPI_SUCCESS;
            }

            release_req(req);
            requests[i] = HMPI_REQUEST_NULL;
            *index = i;
            return MPI_SUCCESS;
        }
        iters++;
    } while(done == 0);

    //All requests were NULL.
    *index = MPI_UNDEFINED;
    if(status != HMPI_STATUS_IGNORE) {
        status->size = 0;
    }
    return MPI_SUCCESS;
}


int HMPI_Get_count(HMPI_Status* status, MPI_Datatype datatype, int* count)
{
    int type_size;

    MPI_Type_size(datatype, &type_size);

    if(unlikely(type_size == 0)) {
        *count = 0;
    } else if(unlikely(status->size % type_size != 0)) {
        *count = MPI_UNDEFINED;
    } else {
        *count = status->size / (size_t)type_size;
    }

    return MPI_SUCCESS;
}


int HMPI_Iprobe(int source, int tag, HMPI_Comm comm, int* flag, HMPI_Status* status)
{
    //Try to match using source/tag/comm.
    //If match, set flag and fill in status as we would for a recv
    // Also have to set length!
    //If no match, clear flag and we're done.
    HMPI_Request send_req = NULL;

    //Progress here prevents deadlocks.
    HMPI_Progress();

    //Probe HMPI (on-node) layer
    *flag = match_probe(source, tag, comm, &send_req);
    if(*flag) {
        if(status != HMPI_STATUS_IGNORE) {
            status->size = send_req->size;
            status->MPI_SOURCE = send_req->proc;
            status->MPI_TAG = send_req->tag;
            status->MPI_ERROR = MPI_SUCCESS;
        }
    } else if(g_size > 1) {
        //Probe MPI (off-node) layer only if more than one MPI rank
        MPI_Status st;
        MPI_Iprobe(source, tag, g_tcomms[g_tl_tid], flag, &st);

        if(*flag && status != HMPI_STATUS_IGNORE) {
            int count;
            MPI_Get_count(&st, MPI_BYTE, &count);
            status->size = count;
            status->MPI_SOURCE = st.MPI_SOURCE;
            status->MPI_TAG = st.MPI_TAG;
            status->MPI_ERROR = st.MPI_ERROR;
        }
    }

    return MPI_SUCCESS;
}


int HMPI_Probe(int source, int tag, HMPI_Comm comm, HMPI_Status* status)
{
    int flag;

    do {
        HMPI_Iprobe(source, tag, comm, &flag, status);
    } while(flag == 0);

    return MPI_SUCCESS;
}


int HMPI_Isend(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *request) {
  
#ifdef DEBUG
    printf("[%i] HMPI_Isend(%p, %i, %p, %i, %i, %p, %p) (proc null: %i)\n", g_hmpi_rank, buf, count, (void*)datatype, dest, tag, comm, req);
    fflush(stdout);
#endif

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

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

    int nthreads = g_nthreads;
    int hmpi_rank = g_hmpi_rank;

    int type_size;
    MPI_Type_size(datatype, &type_size);

    update_reqstat(req, HMPI_REQ_ACTIVE);

    req->proc = hmpi_rank; // my local rank
    req->tag = tag;
    req->size = count * type_size;
    req->buf = buf;
    req->datatype = datatype;

    if(unlikely(dest == MPI_PROC_NULL)) { 
        req->type = HMPI_SEND;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        return MPI_SUCCESS;
    }

    int target_mpi_rank = dest / nthreads;
    //if(target_mpi_rank == (hmpi_rank / nthreads)) {
    if(target_mpi_rank == g_rank) {
        req->type = HMPI_SEND;
        //req->u.local.match_req = NULL; //Not neeeded
        //req->u.local.offset = 0;
        LOCK_INIT(&req->match, 1);

        int target_mpi_thread = dest % nthreads;
        add_send_req(&g_send_reqs[target_mpi_thread], req);
    } else {
        req->type = MPI_SEND;

        int target_thread = dest % nthreads;
        //printf("%d MPI send to rank %d (%d:%d) tag %d count %d size %d\n", hmpi_rank, dest, target_mpi_rank, target_thread, tag, count, req->size);
        //fflush(stdout);

#ifdef COMM_NTHREADS
        int mpi_tag = (tag << SRC_TAG_BITS) | g_tl_tid;
        MPI_Isend(buf, count, datatype, target_mpi_rank, mpi_tag, g_tcomms[target_thread], &req->req);
#endif

#ifdef COMM_SQUARED
        MPI_Isend(buf, count, datatype, target_mpi_rank, tag, SRC_DST_COMM(g_tl_tid, target_thread), &req->u.remote.req);
#endif
    }

    return MPI_SUCCESS;
}


int HMPI_Send(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm) {
  HMPI_Request req;
  HMPI_Isend(buf, count, datatype, dest, tag, comm, &req);
  HMPI_Wait(&req, HMPI_STATUS_IGNORE);
  return MPI_SUCCESS;
}


int HMPI_Irecv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *request) {

#ifdef DEBUG
  printf("[%i] HMPI_Irecv(%p, %i, %p, %i, %i, %p, %p) (proc null: %i)\n", g_hmpi_rank, buf, count, (void*)datatype, source, tag, comm, req, MPI_PROC_NULL);
  fflush(stdout);
#endif

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

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

  int type_size;
  MPI_Type_size(datatype, &type_size);

  update_reqstat(req, HMPI_REQ_ACTIVE);

  req->proc = source;
  req->tag = tag;
  req->size = count * type_size;
  req->buf = buf;
  req->datatype = datatype;

    if(unlikely(source == MPI_PROC_NULL)) { 
        req->type = HMPI_RECV;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        return MPI_SUCCESS;
    }

  int source_mpi_rank = source / g_nthreads;
  if(unlikely(source == MPI_ANY_SOURCE)) {
    // test both layers and pick first 
    req->type = HMPI_RECV_ANY_SOURCE;
    req->proc = MPI_ANY_SOURCE;

    //Post an MPI level receive; it can be cancelled later.
    //for nthreads comms, This could kind of work -- we have to use ANY_TAG and queue messages..
    //for nthreads^2 comms, this won't work -- we dont know which comm to post on!

    add_recv_req(req);
  } else if(source_mpi_rank == g_rank) { //Recv on-node, but not ANY_SOURCE
    // recv from other thread in my process
    req->type = HMPI_RECV;

    add_recv_req(req);
  } else { //Recv off-node, but not ANY_SOURCE
    //printf("%d MPI recv buf %p count %d src %d (%d) tag %d req %p\n", g_hmpi_rank, buf, count, source, source_mpi_rank, tag, req);
    //fflush(stdout);

#ifdef COMM_NTHREADS
    int mpi_tag;
 
   //Note that the original tag is always stored as-is on the req.
   //This, if MPI_ANY_TAG is used, we have it indicated there.
   mpi_tag = (tag << SRC_TAG_BITS) | (source % g_nthreads);

    MPI_Irecv(buf, count, datatype, source_mpi_rank, mpi_tag, g_tcomms[g_tl_tid], &req->req);
#endif

#ifdef COMM_SQUARED
    MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, SRC_DST_COMM(source % g_nthreads, g_tl_tid), &req->u.remote.req);
#endif

    req->type = MPI_RECV;
  }

  return MPI_SUCCESS;
}


int HMPI_Recv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status) {
  HMPI_Request req;
  HMPI_Irecv(buf, count, datatype, source, tag, comm, &req);
  HMPI_Wait(&req, status);
  return MPI_SUCCESS;
}

