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

#include "profile2.h"


//Pin threads to cores using pthreads by default.
//No pinning needed on BG P/Q
#ifndef __bg__
#define PIN_WITH_PTHREAD 1
#endif

#define COMM_NTHREADS 1    //One communicator per thread
//#define COMM_SQUARED 1       //nthreads^2 communicators

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
#ifdef __bg__
#define BLOCK_SIZE 65536
#else
#define BLOCK_SIZE 8192
#endif


#include <pthread.h>
#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "lock.h"

//#define MALLOC(t, s) (t*)__builtin_assume_aligned(memalign(64, sizeof(t) * s), 64)
#define MALLOC(t, s) (t*)memalign(64, sizeof(t) * s)

//Wrappers to GCC/ICC extensions

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)



#ifdef FULL_PROFILE
PROFILE_DECLARE();
#define FULL_PROFILE_INIT(v) PROFILE_INIT(v)
#define FULL_PROFILE_VAR(v) PROFILE_VAR(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_RESET(v) PROFILE_RESET(v)
#define FULL_PROFILE_SHOW(v) PROFILE_SHOW(v)
#else
#define FULL_PROFILE_INIT(v)
#define FULL_PROFILE_VAR(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_RESET(v)
#define FULL_PROFILE_SHOW(v)
#endif

FULL_PROFILE_VAR(MPI_Other);
FULL_PROFILE_VAR(MPI_Isend);
FULL_PROFILE_VAR(MPI_Irecv);
FULL_PROFILE_VAR(MPI_Test);
FULL_PROFILE_VAR(MPI_Testall);
FULL_PROFILE_VAR(MPI_Wait);
FULL_PROFILE_VAR(MPI_Waitall);
FULL_PROFILE_VAR(MPI_Waitany);
FULL_PROFILE_VAR(MPI_Iprobe);

FULL_PROFILE_VAR(MPI_Barrier);
FULL_PROFILE_VAR(MPI_Reduce);
FULL_PROFILE_VAR(MPI_Allreduce);
FULL_PROFILE_VAR(MPI_Scan);
FULL_PROFILE_VAR(MPI_Bcast);
FULL_PROFILE_VAR(MPI_Scatter);
FULL_PROFILE_VAR(MPI_Gather);
FULL_PROFILE_VAR(MPI_Gatherv);
FULL_PROFILE_VAR(MPI_Allgather);
FULL_PROFILE_VAR(MPI_Allgatherv);
FULL_PROFILE_VAR(MPI_Alltoall);


#ifdef ENABLE_OPI
FULL_PROFILE_VAR(OPI_Alloc);
FULL_PROFILE_VAR(OPI_Free);
FULL_PROFILE_VAR(OPI_Give);
FULL_PROFILE_VAR(OPI_Take);

void OPI_Init(void);
void OPI_Finalize(void);
#endif


static int g_ncores;                //Cores per socket
static int g_nsockets;              //Sockets per node
int g_nthreads=-1;                  //Threads per node
int g_rank=-1;                      //Underlying MPI rank for this node
int g_size=-1;                      //Underlying MPI world size
__thread int g_hmpi_rank=-1;        //HMPI rank for this thread
__thread int g_tl_tid=-1;           //HMPI node-local rank for this thread (tid)

typedef struct local_info_t {
    int g_ncores;                //Cores per socket
    int g_nsockets;              //Sockets per node
    int g_nthreads;                  //Threads per node
    int g_rank;                      //Underlying MPI rank for this node
    int g_size;                      //Underlying MPI world size
    int g_hmpi_rank;        //HMPI rank for this thread
    int g_tl_tid;           //HMPI node-local rank for this thread (tid)
} local_info_t;

static __thread local_info_t* local_info;

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
    HMPI_Item* item = g_free_msgs;

    //Malloc a new msg only if none are in the pool.
    if(item == NULL) {
        HMPI_Message* ptr = MALLOC(HMPI_Message, 1);
        return ptr;
    } else {
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


typedef struct HMPI_Request_list {
    HMPI_Item head;
    HMPI_Item* tail;

#ifdef USE_MCS 
    mcs_lock_t lock;
#else
    lock_t lock;
#endif 
} HMPI_Request_list;

static HMPI_Request_list* g_send_reqs = NULL;       //Senders add sends here
static __thread HMPI_Request_list* g_tl_my_send_reqs;    //Shortcut
static __thread HMPI_Request_list g_tl_send_reqs;   //Receiver-local send Q

//Pool of unused reqs to save malloc time.
//TODO - allocate in blocks for locality and more speed?
static __thread HMPI_Item* g_free_reqs = NULL;


//TODO - Maybe send reqs should be allocated on the receiver.  How?
static inline HMPI_Request acquire_req(void)
{
    HMPI_Item* item = g_free_reqs;

    //Malloc a new req only if none are in the pool.
    if(item == NULL) {
        HMPI_Request req = (HMPI_Request)MALLOC(HMPI_Request_info, 1);
        return req;
    } else {
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

#ifndef USE_MCS
    LOCK_SET(&req_list->lock);
    //FENCE();
#else
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&req_list->lock, &q);
#endif

    req_list->tail->next = item;
    req_list->tail = item;

#ifndef USE_MCS
    //FENCE();
    LOCK_CLEAR(&req_list->lock);
#else
    MCS_LOCK_RELEASE(&req_list->lock, &q);
#endif
}


static inline void remove_send_req(HMPI_Request_list* req_list,
                                   HMPI_Item* prev, const HMPI_Item* cur)
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


static inline void update_send_reqs(HMPI_Request_list* local_list, HMPI_Request_list* shared_list)
{
    HMPI_Item* tail;

    //FENCE();
    if(shared_list->tail != &shared_list->head) {
        //g_tl_send_reqs.tail->next = req_list->head.next;
        //This is safe, the branch ensures at least one node.
        //Senders only add at the tail, so head.next won't
        //change out from under us.

    //FENCE();
        //__lwsync();
        //__fence();
        //TODO - possible BG hang here? read could come before branch check
        local_list->tail->next = shared_list->head.next;

#ifndef USE_MCS
        LOCK_SET(&shared_list->lock);
    //FENCE();
#else
        mcs_qnode_t q;
        MCS_LOCK_ACQUIRE(&shared_list->lock, &q);
#endif

        //g_tl_send_reqs.tail = req_list->tail;
        //local_list->tail = req_list->tail;
        tail = shared_list->tail;
        shared_list->tail = &shared_list->head;

#ifndef USE_MCS
    //FENCE();
        LOCK_CLEAR(&shared_list->lock);
#else
        MCS_LOCK_RELEASE(&shared_list->lock, &q);
#endif

        local_list->tail = tail;
        tail->next = NULL;
    }
}



static inline void add_recv_req(HMPI_Request req) {
    HMPI_Item* item = (HMPI_Item*)req;

    //Add at tail to ensure matching occurs in order.
    item->next = NULL;
    g_recv_reqs_tail->next = item;
    g_recv_reqs_tail = item;

}


static inline void remove_recv_req(HMPI_Item* prev, const HMPI_Item* cur) {
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

    int proc = recv_req->proc;
    int tag = recv_req->tag;

    //So searching takes a long time..
    //Maybe it is taking a while for updates to the send Q to show up at the
    // receiver?  Is there a way to sync/flush/notify?
    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

#ifdef ENABLE_OPI
        if(req->type == HMPI_SEND &&
                req->proc == proc && (req->tag == tag || tag == MPI_ANY_TAG)) {
#else
        if(req->proc == proc && (req->tag == tag || tag == MPI_ANY_TAG)) {
#endif
            remove_send_req(req_list, prev, cur);

            //recv_req->proc = req->proc; //Not necessary, no ANY_SRC
            recv_req->tag = req->tag;
            //printf("%d matched recv req %d proc %d tag %d to send req %p\n",
            //        g_hmpi_rank, recv_req, proc, tag, req);
            //fflush(stdout);
            return req;
        }

    }

    return HMPI_REQUEST_NULL;
}


//Match for takes
static inline HMPI_Request match_take(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    int proc = recv_req->proc;
    int tag = recv_req->tag;

    //So searching takes a long time..
    //Maybe it is taking a while for updates to the send Q to show up at the
    // receiver?  Is there a way to sync/flush/notify?
    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

#ifdef ENABLE_OPI
        if(req->type == OPI_GIVE &&
                req->proc == proc && (req->tag == tag || tag == MPI_ANY_TAG)) {
#else
        if(req->proc == proc && (req->tag == tag || tag == MPI_ANY_TAG)) {
#endif
            remove_send_req(req_list, prev, cur);

            //recv_req->proc = req->proc; //Not necessary, no ANY_SRC
            recv_req->tag = req->tag;
            //printf("%d matched recv req %d proc %d tag %d to send req %p\n",
            //        g_hmpi_rank, recv_req, proc, tag, req);
            //fflush(stdout);
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

    int tag = recv_req->tag;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

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

    update_send_reqs(req_list, g_tl_my_send_reqs);

    for(cur = req_list->head.next; cur != NULL; cur = cur->next) {
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
#ifdef __bg__
    __lwsync();
#endif
    req->stat = stat;
}


#if 0
static inline int get_reqstat(const HMPI_Request req) {
  return req->stat;
}
#endif

#define get_reqstat(req) req->stat


int __attribute__((weak)) tmain(int argc, char** argv);

//Weak version of main so applications don't have to keep redefining it.
int __attribute__((weak)) main(int argc, char** argv)
{
    if(argc < 4) {
        printf("ERROR must specify number of threads, cores, and sockets:\n\n\t%s <args> <numthreads> <cores_per_socket> <numsockets>\n\n", argv[0]);
        return -1;
    }

    //TODO - may not be portable to some MPIs?
    int threads = atoi(argv[argc - 3]);
    int cores = atoi(argv[argc - 2]);
    int sockets = atoi(argv[argc - 1]);
    argc -= 3;

    return HMPI_Init(&argc, &argv, tmain, threads, cores, sockets);
}


// this is called by pthread create and then calls the real function!
void* trampoline(void* tid) {
    int rank = (int)(uintptr_t)tid;

#ifdef PIN_WITH_PTHREAD
    {
        int core = rank % (g_ncores * g_nsockets);
        int idx;

        if(g_nthreads <= 2) {
            //Cheat like MPI for microbenchmarks
            idx = core;
        } else {
            //Spread ranks evenly across sockets, grouping adjacent ranks into one
            //socket.
            int rms = g_nthreads % g_nsockets;
            int rs = (g_nthreads / g_nsockets) + 1;
            if(core < rms * rs) {
                idx = ((core / rs) * g_ncores) + (core % rs);
            } else {
                int rmd = core - (rms * rs);
                rs -= 1;
                idx = (rmd / rs) * g_ncores + (rmd % rs) + rms * g_ncores;
            }
        }

        //printf("Rank %d binding to index %d\n", rank, idx);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(idx, &cpuset);
        int ret =
            pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
        if(ret) {
            printf("%d ERROR in pthread_setaffinity_np: %s\n", rank, strerror(ret));
            MPI_Abort(MPI_COMM_WORLD, 0);
        }
    }
#endif //PIN_WITH_PTHREAD


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
#ifndef USE_MCS
    LOCK_INIT(&g_send_reqs[rank].lock, 0);
#else
    MCS_LOCK_INIT(&g_send_reqs[rank].lock);
#endif
    g_tl_send_reqs.head.next = NULL;
    g_tl_send_reqs.tail = &g_tl_send_reqs.head;


#ifdef COMM_NTHREADS
    g_msgs_tail = &g_msgs_head;
#endif

    //Copy global vars to local mem
    local_info = MALLOC(local_info_t, 1);
    local_info->g_ncores = g_ncores;
    local_info->g_nsockets = g_nsockets;
    local_info->g_nthreads = g_nthreads;
    local_info->g_rank = g_rank;
    local_info->g_size = g_size;
    local_info->g_hmpi_rank = g_hmpi_rank;
    local_info->g_tl_tid = g_tl_tid;

    FULL_PROFILE_INIT(g_tl_tid);

    // Don't head off to user land until all threads are ready 

//#ifdef USE_L2_BARRIER
//#warning "L2 barrier"
   // L2_barrier(&HMPI_COMM_WORLD->barr, g_nthreads);
//#else
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);
//#endif

#ifdef ENABLE_OPI
    OPI_Init();
#endif

    FULL_PROFILE_RESET(MPI_Other);
    FULL_PROFILE_START(MPI_Other);

    // call user function
    g_entry(g_argc, argv);

    //Free up our version of argv
    for(int i = 0; i < g_argc; i++) {
        free(argv[i]);
    }

    free(argv);

#ifdef ENABLE_OPI
    OPI_Finalize();
#endif
    return NULL;
}


int HMPI_Init(int *argc, char ***argv, int (*start_routine)(int argc, char** argv), int nthreads, int ncores, int nsockets)
{
  pthread_t* threads;
  int provided;
  long int thr;

  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
  assert(MPI_THREAD_MULTIPLE == provided);

#ifdef COMM_NTHREADS
  //Make sure we can support the requested number of threads.
  if(nthreads >= (1 << SRC_TAG_BITS) - 1) {
      printf("ERROR requested %d threads, but SRC_TAG_BITS (%d) limits to %d threads\n",
              nthreads, SRC_TAG_BITS, (1 << SRC_TAG_BITS) - 2);
      MPI_Abort(MPI_COMM_WORLD, 0);
  }

  {
    int flag;
    uintptr_t val;

    MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_TAG_UB, &val, &flag);
    SRC_TAG_ANY = val;

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

    //TODO - consider using this in HMPI
#if 0
    PAPI_hw_info_t* info = PAPI_get_hardware_info();

    printf("PAPI ncpu %d nnodes %d totalcpus %d\n", info->ncpu, info->nnodes, info->totalcpus);
#endif

  HMPI_COMM_WORLD = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
  HMPI_COMM_WORLD->mpicomm = MPI_COMM_WORLD;
#ifdef USE_L2_BARRIER
  L2_barrier_init(&HMPI_COMM_WORLD->barr, nthreads);
#else
  barrier_init(&HMPI_COMM_WORLD->barr, nthreads);
#endif

  HMPI_COMM_WORLD->sbuf = MALLOC(volatile void*, nthreads);
  HMPI_COMM_WORLD->rbuf = MALLOC(volatile void*, nthreads);
  HMPI_COMM_WORLD->scount = MALLOC(int, nthreads);
  HMPI_COMM_WORLD->rcount = MALLOC(int, nthreads);
  HMPI_COMM_WORLD->stype = MALLOC(MPI_Datatype, nthreads);
  HMPI_COMM_WORLD->rtype = MALLOC(MPI_Datatype, nthreads);
  HMPI_COMM_WORLD->coll = NULL;

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

  //Bluegene P/Q has a 1-thread/core limit, so this thread will run as rank 0.
  for(thr=1; thr < nthreads; thr++) {
    //Create the thread
    int ret = pthread_create(&threads[thr], NULL, trampoline, (void *)thr);
    if(ret) {
        printf("ERROR in pthread_create: %s\n", strerror(ret));
        MPI_Abort(MPI_COMM_WORLD, 0);
    }
  }

  trampoline((void*)0);

  for(thr=1; thr<nthreads; thr++) {
    pthread_join(threads[thr], NULL);
  }

  free(g_send_reqs);
  free(threads);
  free(g_tcomms);
  MPI_Finalize();
  return 0;
}


int HMPI_Finalize()
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Other);
    FULL_PROFILE_SHOW(MPI_Isend);
    FULL_PROFILE_SHOW(MPI_Irecv);
    FULL_PROFILE_SHOW(MPI_Test);
    FULL_PROFILE_SHOW(MPI_Testall);
    FULL_PROFILE_SHOW(MPI_Wait);
    FULL_PROFILE_SHOW(MPI_Waitall);
    FULL_PROFILE_SHOW(MPI_Waitany);
    FULL_PROFILE_SHOW(MPI_Iprobe);

    FULL_PROFILE_SHOW(MPI_Barrier);
    FULL_PROFILE_SHOW(MPI_Reduce);
    FULL_PROFILE_SHOW(MPI_Allreduce);
    FULL_PROFILE_SHOW(MPI_Scan);
    FULL_PROFILE_SHOW(MPI_Bcast);
    FULL_PROFILE_SHOW(MPI_Scatter);
    FULL_PROFILE_SHOW(MPI_Gather);
    FULL_PROFILE_SHOW(MPI_Gatherv);
    FULL_PROFILE_SHOW(MPI_Allgather);
    FULL_PROFILE_SHOW(MPI_Allgatherv);
    FULL_PROFILE_SHOW(MPI_Alltoall);

    FULL_PROFILE_SHOW(MPI_Other);

    HMPI_Barrier(HMPI_COMM_WORLD);

#if 0
#ifdef USE_L2_BARRIER
    L2_barrier(&HMPI_COMM_WORLD->barr, g_nthreads);
#else
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);
#endif
#endif

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

    //Seems to prevent a segfault in MPI_Finalize()
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);
    return 0;
}


//We assume req->type == HMPI_SEND and req->stat == 0 (uncompleted send)
static inline int HMPI_Progress_send(const HMPI_Request send_req)
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
        while((offset = FETCH_ADD64(offsetptr, BLOCK_SIZE)) < size) {
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
        while((offset = FETCH_ADD64(offsetptr, BLOCK_SIZE)) < size) {
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
    printf("%d completed local-level RECV buf %p size %lu source %d tag %d\n",
            g_hmpi_rank, recv_req->buf, recv_req->size, recv_req->proc, recv_req->tag);
    printf("%d completed local-level SEND buf %p size %lu dest %d tag %d\n",
            send_req->proc, send_req->buf, send_req->size, g_hmpi_rank, send_req->tag);
    fflush(stdout);
#endif
}


#ifdef ENABLE_OPI
//For req->type == OPI_TAKE
static inline void HMPI_Complete_take(HMPI_Request recv_req, HMPI_Request send_req)
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

#if 0
    if(size < 256) {
        //Size is too small - just memcpy the buffer instead of doing OP.
        //But, I have to alloc and free.. blah
        OPI_Alloc(recv_req->buf, size);
        memcpy(*((void**)recv_req->buf), send_req->buf, size);
        OPI_Free(&send_req->buf);
    } else {
#endif
        //Easy OP
        *((void**)recv_req->buf) = send_req->buf;
    //}

    //Mark send and receive requests done
    update_reqstat(send_req, HMPI_REQ_COMPLETE);
    update_reqstat(recv_req, HMPI_REQ_COMPLETE);

#ifdef DEBUG
    printf("%d completed local-level TAKE buf %p size %lu source %d tag %d\n",
            g_hmpi_rank, recv_req->buf, recv_req->size, recv_req->proc, recv_req->tag);
    printf("%d completed local-level GIVE buf %p size %lu dest %d tag %d\n",
            send_req->proc, send_req->buf, send_req->size, g_hmpi_rank, send_req->tag);
    fflush(stdout);
#endif
}
#endif


//For req->type == MPI_SEND || req->type == MPI_RECV
static inline int HMPI_Progress_mpi(HMPI_Request req)
{
    int flag;
    MPI_Status status;

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
        HMPI_Type_size(req->datatype, &type_size);

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


//Progress local receive requests.
static inline void HMPI_Progress(HMPI_Request_list* local_list, HMPI_Request_list* shared_list) {
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    //With a separate Q, this will need to be duplicated
    update_send_reqs(local_list, shared_list);

    //Progress receive requests.
    //We remove items from the list, but they are still valid; nothing in this
    //function will free or modify a req.  So, it's safe to do cur = cur->next.
    //Note the careful updating of prev; we need to leave it alone on iterations
    //where cur is matched successfully and only update it otherwise.
    // This prevents the recv_reqs list from getting corrupted due to a bad
    // prev pointer.
    for(prev = &g_recv_reqs_head, cur = prev->next;
            cur != NULL; cur = cur->next) {
        req = (HMPI_Request)cur;

        if(likely(req->type == HMPI_RECV)) {
            HMPI_Request send_req = match_recv(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            }
#ifdef ENABLE_OPI
        } else if(req->type == OPI_TAKE) {
            HMPI_Request send_req = match_take(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_take(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            }
#endif
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

            HMPI_Request send_req = match_recv_any(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            } else if(g_size > 1) {
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
                        g_tcomms[g_tl_tid], &flag, &status);

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
                            status.MPI_TAG, g_tcomms[g_tl_tid], &status);

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
                          g_tcomms[g_tl_tid], &status);
                    LOCK_CLEAR(&g_mpi_lock);

                    int type_size;
                    HMPI_Type_size(req->datatype, &type_size);

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
                            HMPI_Type_size(req->datatype, &type_size);

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
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Test);
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

        HMPI_Progress(&g_tl_send_reqs, g_tl_my_send_reqs);

        if(req->type == HMPI_SEND) {
            f = HMPI_Progress_send(req);
        //OPI GIVE/TAKE will need an entry here; just check its stat
#ifdef ENABLE_OPI
        } else if(req->type == HMPI_RECV || req->type == OPI_GIVE || req->type == OPI_TAKE) {
#else
        } else if(req->type == HMPI_RECV) {
#endif
            f = get_reqstat(req);
        } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
            f = HMPI_Progress_mpi(req);
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            f = get_reqstat(req);
        }

        if(f != HMPI_REQ_COMPLETE) {
            *flag = 0;
            FULL_PROFILE_STOP(MPI_Test);
            FULL_PROFILE_START(MPI_Other);
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

    FULL_PROFILE_STOP(MPI_Test);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Testall(int count, HMPI_Request *requests, int* flag, HMPI_Status *statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Testall);
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
            FULL_PROFILE_STOP(MPI_Testall);
            FULL_PROFILE_START(MPI_Other);
            return MPI_SUCCESS;
        }
    }

    FULL_PROFILE_STOP(MPI_Testall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Wait(HMPI_Request *request, HMPI_Status *status) {
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Wait);

    HMPI_Request req = *request;

#ifdef DEBUG
    printf("%i HMPI_Wait(%x, %x) type: %i\n", g_hmpi_rank, req, status, req->type);
    fflush(stdout);
#endif

    if(unlikely(req == HMPI_REQUEST_NULL)) {
        if(status != HMPI_STATUS_IGNORE) {
            //Make Get_count return 0 count
            status->size = 0;
        }

        FULL_PROFILE_STOP(MPI_Wait);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;

    if(req->type == HMPI_SEND) {
        do {
            HMPI_Progress(local_list, shared_list);
        } while(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE);
        //OPI GIVE/TAKE will need an entry here; just check its stat
#ifdef ENABLE_OPI
    } else if(req->type == HMPI_RECV || req->type == OPI_GIVE || req->type == OPI_TAKE) {
#else
    } else if(req->type == HMPI_RECV) {
#endif
        do {
            HMPI_Progress(local_list, shared_list);
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
    } else if(req->type == MPI_RECV || req->type == MPI_SEND) {
        do {
            HMPI_Progress(local_list, shared_list);
        } while(HMPI_Progress_mpi(req) != HMPI_REQ_COMPLETE);
    } else { //HMPI_RECV_ANY_SOURCE
        do {
            HMPI_Progress(local_list, shared_list);
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

    FULL_PROFILE_STOP(MPI_Wait);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Waitall(int count, HMPI_Request *requests, HMPI_Status *statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Waitall);
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;
    int done;

#ifdef DEBUG
    printf("[%i] HMPI_Waitall(%d, %p, %p)\n", g_hmpi_rank, count, requests, statuses);
    fflush(stdout);
#endif

    do {
        HMPI_Progress(local_list, shared_list);
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
        //OPI GIVE/TAKE will need an entry here; just continue
#ifdef ENABLE_OPI
                } else if(req->type == HMPI_RECV || req->type == OPI_GIVE || req->type == OPI_TAKE) {
#else
                } else if(req->type == HMPI_RECV) {
#endif
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

    FULL_PROFILE_STOP(MPI_Waitall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Waitany);
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;
    int done;

    //Call Progress once, then check each req.
    do {
        done = 1;

        //What if I have progress return whether a completion was made?
        //Then I can skip the poll loop unless something finishes.
        HMPI_Progress(local_list, shared_list);

        //TODO - is there a way to speed this up?
        // Maybe keep my own list of non-null entries that i can reorder to
        // skip null requests..
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
                //OPI GIVE/TAKE will need an entry here; just continue
#ifdef ENABLE_OPI
                } else if(req->type == HMPI_RECV || req->type == OPI_GIVE || req->type == OPI_TAKE) {
#else
                } else if(req->type == HMPI_RECV) {
#endif
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

            FULL_PROFILE_STOP(MPI_Waitany);
            FULL_PROFILE_START(MPI_Other);
            return MPI_SUCCESS;
        }
    } while(done == 0);

    //All requests were NULL.
    *index = MPI_UNDEFINED;
    if(status != HMPI_STATUS_IGNORE) {
        status->size = 0;
    }

    FULL_PROFILE_STOP(MPI_Waitany);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Get_count(HMPI_Status* status, MPI_Datatype datatype, int* count)
{
    int type_size;

    HMPI_Type_size(datatype, &type_size);

    if(unlikely(type_size == 0)) {
        *count = 0;
    } else if(unlikely(status->size % type_size != 0)) {
        *count = MPI_UNDEFINED;
    } else {
        *count = status->size / (size_t)type_size;
    }

    return MPI_SUCCESS;
}


int HMPI_Type_size(MPI_Datatype datatype, int* size)
{
    switch(datatype) {
        case MPI_INT:
            *size = sizeof(int);
            return MPI_SUCCESS;
        case MPI_DOUBLE:
            *size = sizeof(double);
            return MPI_SUCCESS;
        case MPI_FLOAT:
            *size = sizeof(float);
            return MPI_SUCCESS;
        case MPI_CHAR:
            *size = sizeof(char);
            return MPI_SUCCESS;
        default:
            return MPI_Type_size(datatype, size);
    }
}



int HMPI_Iprobe(int source, int tag, HMPI_Comm comm, int* flag, HMPI_Status* status)
{
    //Try to match using source/tag/comm.
    //If match, set flag and fill in status as we would for a recv
    // Also have to set length!
    //If no match, clear flag and we're done.
    HMPI_Request send_req = NULL;

    //Progress here prevents deadlocks.
    HMPI_Progress(&g_tl_send_reqs, g_tl_my_send_reqs);

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
                assert(0);
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


int HMPI_Isend(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Isend);
#ifdef DEBUG
    printf("[%i] HMPI_Isend(%p, %i, %p, %i, %i, %p, %p) (proc null: %i)\n", g_hmpi_rank, buf, count, (void*)datatype, dest, tag, comm, req);
    fflush(stdout);
#endif

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

#if 0
  int size;
  HMPI_Type_size(datatype, &size);
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

    if(unlikely(dest == MPI_PROC_NULL)) { 
        req->type = HMPI_SEND;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        FULL_PROFILE_STOP(MPI_Isend);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

    int nthreads = g_nthreads;
    int hmpi_rank = g_hmpi_rank;

    int type_size;
    HMPI_Type_size(datatype, &type_size);
    uint64_t size = (uint64_t)count * (uint64_t)type_size;

    update_reqstat(req, HMPI_REQ_ACTIVE);

    req->proc = hmpi_rank; // my local rank
    req->tag = tag;
    req->size = size;
    req->buf = buf;
    req->datatype = datatype;

    int target_mpi_rank = dest / nthreads;
    if(target_mpi_rank == (hmpi_rank / nthreads)) {
        req->type = HMPI_SEND;
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
        MPI_Isend(buf, count, datatype, target_mpi_rank, mpi_tag, g_tcomms[target_thread], &req->u.remote.req);
#endif

#ifdef COMM_SQUARED
        MPI_Isend(buf, count, datatype, target_mpi_rank, tag, SRC_DST_COMM(g_tl_tid, target_thread), &req->u.remote.req);
#endif
    }

    FULL_PROFILE_STOP(MPI_Isend);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Send(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm) {
  HMPI_Request req;
  HMPI_Isend(buf, count, datatype, dest, tag, comm, &req);
  HMPI_Wait(&req, HMPI_STATUS_IGNORE);
  return MPI_SUCCESS;
}


int HMPI_Irecv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *request) {
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Irecv);

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
  HMPI_Type_size(datatype, &type_size);

  //update_reqstat(req, HMPI_REQ_ACTIVE);
  req->stat = HMPI_REQ_ACTIVE;

  req->proc = source;
  req->tag = tag;
  req->size = count * type_size;
  req->buf = buf;
  req->datatype = datatype;

    if(unlikely(source == MPI_PROC_NULL)) { 
        req->type = HMPI_RECV;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        FULL_PROFILE_STOP(MPI_Irecv);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

  int source_mpi_rank = source / g_nthreads;
  if(unlikely(source == MPI_ANY_SOURCE)) {
    req->type = HMPI_RECV_ANY_SOURCE;

    add_recv_req(req);
  } else if(source_mpi_rank == g_rank) { //Recv on-node, but not ANY_SOURCE
    // recv from other thread in my process
    req->type = HMPI_RECV;

    add_recv_req(req);
  } else { //Recv off-node, but not ANY_SOURCE
    //printf("%d MPI recv buf %p count %d src %d (%d) tag %d req %p\n", g_hmpi_rank, buf, count, source, source_mpi_rank, tag, req);

#ifdef COMM_NTHREADS
    int mpi_tag;
 
    //Note that the original tag is always stored as-is on the req.
    //This, if MPI_ANY_TAG is used, we have it indicated there.
    mpi_tag = (tag << SRC_TAG_BITS) | (source % g_nthreads);

    MPI_Irecv(buf, count, datatype, source_mpi_rank, mpi_tag, g_tcomms[g_tl_tid], &req->u.remote.req);
#endif

#ifdef COMM_SQUARED
    MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, SRC_DST_COMM(source % g_nthreads, g_tl_tid), &req->u.remote.req);
#endif

    req->type = MPI_RECV;
  }

    FULL_PROFILE_STOP(MPI_Irecv);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}


int HMPI_Recv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status) {
  HMPI_Request req;

  HMPI_Irecv(buf, count, datatype, source, tag, comm, &req);
  HMPI_Wait(&req, status);
  return MPI_SUCCESS;
}


#ifdef ENABLE_OPI
int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request* request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Give);

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

    if(unlikely(dest == MPI_PROC_NULL)) { 
        req->type = OPI_GIVE;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        FULL_PROFILE_STOP(OPI_Give);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

    int nthreads = g_nthreads;
    int hmpi_rank = g_hmpi_rank;

    int type_size;
    HMPI_Type_size(datatype, &type_size);
    uint64_t size = (uint64_t)count * (uint64_t)type_size;

    update_reqstat(req, HMPI_REQ_ACTIVE);

    req->proc = hmpi_rank; // my local rank
    req->tag = tag;
    req->size = size;
    req->buf = *ptr; //For OP Give, we store the pointer being shared directly.
    req->datatype = datatype;

    int target_mpi_rank = dest / nthreads;
    if(target_mpi_rank == (hmpi_rank / nthreads)) {
        req->type = OPI_GIVE;

        int target_mpi_thread = dest % nthreads;
        add_send_req(&g_send_reqs[target_mpi_thread], req);
    } else {
        //Off-node not supported right now
        //This is easy, just convert to a send.
        //Remember to OPI_Free when finished..
        req->type = MPI_SEND;
        assert(0);
    }

    //*ptr = NULL;
    FULL_PROFILE_STOP(OPI_Give);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request* request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Take);

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

    int type_size;
    HMPI_Type_size(datatype, &type_size);

    update_reqstat(req, HMPI_REQ_ACTIVE);
    req->stat = HMPI_REQ_ACTIVE;

    req->proc = source;
    req->tag = tag;
    req->size = count * type_size;
    req->buf = ptr;
    req->datatype = datatype;

    if(unlikely(source == MPI_PROC_NULL)) { 
        req->type = OPI_TAKE;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        FULL_PROFILE_STOP(MPI_Irecv);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

    int source_mpi_rank = source / g_nthreads;
    if(unlikely(source == MPI_ANY_SOURCE)) {
        //Take ANY_SOURCE not supported right now
        assert(0);
        // test both layers and pick first 
        req->type = OPI_TAKE_ANY_SOURCE;

        add_recv_req(req);
    } else if(source_mpi_rank == g_rank) { //Recv on-node, but not ANY_SOURCE
        // recv from other thread in my process
        req->type = OPI_TAKE;

        add_recv_req(req);
    } else { //Recv off-node, but not ANY_SOURCE
        //Need to OPI_Alloc a buffer
        assert(0);
    }

    FULL_PROFILE_STOP(OPI_Take);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}
#endif //ENABLE_OPI

