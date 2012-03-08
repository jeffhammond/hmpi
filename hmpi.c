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

#include <pthread.h>
#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifndef __bg__
#include <hwloc.h>
#endif
//#include "pipelinecopy.h"
#include "lock.h"

#define BLOCK_SIZE 8192

//Wrappers to GCC/ICC extensions

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

PROFILE_DECLARE();
PROFILE_VAR(memcpy);
PROFILE_VAR(allreduce);
PROFILE_VAR(op);


#ifndef __bg__
static hwloc_topology_t g_hwloc_topo;
#endif


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

//Each thread has a list of send and receive requests.
//The receive requests are managed only by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them.

static __thread HMPI_Item g_recv_reqs_head = {NULL, NULL};
static __thread HMPI_Item g_recv_reqs_tail = {NULL, NULL};
//static HMPI_Item** g_recv_reqs = NULL;

typedef struct HMPI_Request_list {
    HMPI_Item head;
    HMPI_Item* tail;
    lock_t lock;
} HMPI_Request_list;

static HMPI_Request_list* g_send_reqs = NULL;
static __thread HMPI_Item* g_tl_send_reqs_head;

static __thread HMPI_Item* g_free_reqs = NULL;


static inline HMPI_Request acquire_req(void)
{
    if(g_free_reqs == NULL) {
        return (HMPI_Request)malloc(sizeof(HMPI_Request_info));
    } else {
        HMPI_Item* item = g_free_reqs;
        g_free_reqs = item->next;
        return (HMPI_Request)item;
    }
}


static inline void release_req(HMPI_Request req)
{
    HMPI_Item* item = (HMPI_Item*)req;

    item->next = g_free_reqs;
    g_free_reqs = item;
}


//There is no corresponding remove -- done in match_recv()
static inline void add_send_req(HMPI_Request req, int tid) {
    //Insert req at tail.
    HMPI_Request_list* req_list = &g_send_reqs[tid];
    HMPI_Item* item = (HMPI_Item*)req;

    item->next = NULL;

    LOCK_SET(&req_list->lock);
    req_list->tail->next = item;
    req_list->tail = item;
    LOCK_CLEAR(&req_list->lock);

#if 0
    HMPI_Item* next;
    do {
        next = item->next = req_list->head.next;
    } while (!CAS_PTR_BOOL(&req_list->head.next, next, item));
#endif
}


static inline void add_recv_req(HMPI_Request req) {
    HMPI_Item* item = (HMPI_Item*)req;
    //int tid = g_tl_tid;

    item->next = &g_recv_reqs_tail;
    item->prev = g_recv_reqs_tail.prev;

    g_recv_reqs_tail.prev->next = item;
    g_recv_reqs_tail.prev = item;

#if 0
    item->next = g_recv_reqs;
    item->prev = NULL;

    if(item->next != NULL) {
        item->next->prev = item;
    }

    g_recv_reqs = item;
#endif
}


static inline void remove_recv_req(HMPI_Request req) {
    HMPI_Item* item = (HMPI_Item*)req;
    //int tid = g_tl_tid;

    item->next->prev = item->prev;
    item->prev->next = item->next;

#if 0
    if(item->prev == NULL) {
        //Head of list
        g_recv_reqs = item->next;
    } else {
        item->prev->next = item->next;
    }

    if(item->next != NULL) {
        item->next->prev = item->prev;
    }
#endif
}


static inline int match_recv(HMPI_Request recv_req, HMPI_Request* send_req) {
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    int proc = recv_req->proc;
    int tag = recv_req->tag;

    //for(prev = NULL, cur = req_list->head.next;
    for(prev = g_tl_send_reqs_head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG, so don't check.
        if((req->proc == proc || proc == MPI_ANY_SOURCE) &&
                (req->tag == tag || tag == MPI_ANY_TAG)) {

            if(cur->next == NULL) {
                HMPI_Request_list* req_list = &g_send_reqs[g_tl_tid];

                //cur is tail, lock to update tail.
                prev->next = NULL;
                LOCK_SET(&req_list->lock);

                //Check again, may have had another insert since checking.
                if(cur->next == NULL) {
                    //Still tail.
                    req_list->tail = prev;
                    LOCK_CLEAR(&req_list->lock);
                } else {
                    LOCK_CLEAR(&req_list->lock);
                    prev->next = cur->next;
                }
            } else {
                //Not at tail; just remove.
                prev->next = cur->next;
            }
#if 0
            if(cur == req_list->head.next) {
                //cur is head, CAS to update.
                if(!CAS_PTR_BOOL(&req_list->head.next, cur, cur->next)) {
                    //No longer the head, just update.
                    for(prev = req_list->head.next;
                            prev->next != cur; prev = prev->next);
                    prev->next = cur->next;
                }
            } else {
                prev->next = cur->next;
            }
#endif


            recv_req->proc = req->proc;
            recv_req->tag = req->tag;
            *send_req = req;
            return 1;
        }
    }

    return 0;
}


static inline int match_probe(int source, int tag, HMPI_Comm comm, HMPI_Request* send_req) {
    //HMPI_Request_list* req_list = &g_send_reqs[g_tl_tid];
    HMPI_Item* cur;
    HMPI_Request req;

    for(cur = g_tl_send_reqs_head->next; cur != NULL; cur = cur->next) {
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

#ifndef __bg__
    {
        //Spread threads evenly across cores
        //Each thread should bind itself according to its rank.
        // Compute rs = num_ranks / num_sockets
        //         c = r % rs, s = r / rs
        //         idx = s * num_cores + c
        hwloc_obj_t obj;
        hwloc_cpuset_t cpuset;
        //hwloc_cpuset_t* cpuset = &g_hwloc_cpuset[rank];

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

#if 0
        if(num_cores < g_nthreads) {
            printf("%d ERROR requested %d threads but only %d cores\n",
                    g_rank, g_nthreads, num_cores);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }
#endif

        int core = rank % num_cores;
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
        if(core < rms * rs) {
            idx = ((core / rs) * num_cores) + (core % rs);
        } else {
            int rmd = core - (rms * rs);
            rs -= 1;
            idx = (rmd / rs) * num_cores + (rmd % rs) + rms * num_cores;
        }

        //printf("Rank %d binding to index %d\n", rank, idx);

        obj = hwloc_get_obj_by_type(g_hwloc_topo, HWLOC_OBJ_CORE, idx);
        if(obj == NULL) {
            printf("%d ERROR got NULL hwloc core object idx %d\n", core, idx);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }

        cpuset = hwloc_bitmap_dup(obj->cpuset);
        hwloc_bitmap_singlify(cpuset);

        hwloc_set_cpubind(g_hwloc_topo, cpuset, HWLOC_CPUBIND_THREAD);
        hwloc_set_membind(g_hwloc_topo, cpuset, HWLOC_MEMBIND_BIND, HWLOC_CPUBIND_THREAD);
    }
#endif //__bg__


    // save thread-id in thread-local storage
    g_tl_tid = rank;
    g_hmpi_rank = g_rank*g_nthreads+rank;

    // Initialize send requests list and lock
    //g_recv_reqs = NULL;
    g_recv_reqs_head.next = &g_recv_reqs_tail;
    g_recv_reqs_tail.prev = &g_recv_reqs_head;

    g_send_reqs[rank].head.next = NULL;
    g_send_reqs[rank].tail = &g_send_reqs[rank].head;
    g_tl_send_reqs_head = &g_send_reqs[rank].head;
    LOCK_INIT(&g_send_reqs[rank].lock, 0);
    PROFILE_INIT(g_tl_tid);

    //printf("%d:%d entered trampoline\n", g_rank, g_tl_tid); fflush(stdout);

    // Don't head off to user land until all threads are ready 
    barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);

    //printf("%d:%d g_entry now\n", g_rank, g_tl_tid); fflush(stdout);
    // call user function
    g_entry(g_argc, g_argv);
    return NULL;
}


int HMPI_Init(int *argc, char ***argv, int nthreads, int (*start_routine)(int argc, char** argv))
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

  g_send_reqs = (HMPI_Request_list*)malloc(sizeof(HMPI_Request_list) * nthreads);

  g_tcomms = (MPI_Comm*)malloc(sizeof(MPI_Comm) * nthreads);

  MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &g_size);


  //Ranks set their own affinity in trampoline.
#ifndef __bg__
  hwloc_topology_init(&g_hwloc_topo);
  hwloc_topology_load(g_hwloc_topo);
#endif


  //Do per-thread initialization that must be done before threads are started.
  for(thr = 0; thr < nthreads; thr++) {
    //Duplicate COMM_WORLD for this thread.
    MPI_Comm_dup(MPI_COMM_WORLD, &g_tcomms[thr]);
  }

  //Bluegene has a 1-thread/core limit, so this thread will run as rank 0.
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
  return 0;
}


int HMPI_Comm_rank(HMPI_Comm comm, int *rank) {
  
  //printf("[%i] HMPI_Comm_rank()\n", g_rank*g_nthreads+g_tl_tid);
#ifdef HMPI_SAFE 
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

  if(g_tl_tid == 0) {
    MPI_Finalize();
  }

  return 0;
}


// global progress function
//static inline int HMPI_Progress_request(HMPI_Request req);
//static inline int HMPI_Progress_recv(HMPI_Request recv_req);


//We assume req->type == HMPI_SEND and req->stat == 0 (uncompleted send)
static inline int HMPI_Progress_send(HMPI_Request send_req) {

    //Poll local receives in the recv reqs list.
    // We do this to prevent deadlock when local threads are exchange messages
    // with each other.  Normally no work is done on a send request, but if
    // the app blocks waiting for it to complete, a neighbor thread could also
    // block on its send and neither of their receives are ever completed.

    //TODO - this visits most rescent receives first.  maybe iterate backwards
    // to progress oldest receives first?

    //Write blocks on this send req if receiver has matched it.
    //If mesage is short, receiver won't bother clearing the match lock, and
    // instead just does the copy and marks completion.

    if(LOCK_TRY(&send_req->match)) {
        HMPI_Request recv_req = (HMPI_Request)send_req->match_req;
        uintptr_t rbuf = (uintptr_t)recv_req->buf;

        size_t send_size = send_req->size;
        size_t recv_size = recv_req->size;
        size_t size = (send_size < recv_size ? send_size : recv_size);

        uintptr_t sbuf = (uintptr_t)send_req->buf;
        volatile ssize_t* offsetptr = &send_req->offset;
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
        while(get_reqstat(send_req) != 1);
        return 1;
    }

    return get_reqstat(send_req);
}


//For req->type == HMPI_RECV
static inline int HMPI_Progress_recv(HMPI_Request recv_req) {
    //Try to match from the local send reqs list
    HMPI_Request send_req = NULL;

    if(!match_recv(recv_req, &send_req)) {
        return 0;
    }

    //Remove the recv from the pending recv list
    remove_recv_req(recv_req);

#ifdef DEBUG
    printf("[%i] [recv] found send from %i (%p) for buf %p in uq (tag: %i, size: %ld, status: %d)\n",
            g_hmpi_rank, send_req->proc, send_req->buf, recv_req->buf, send_req->tag, send_req->size, get_reqstat(send_req));
    fflush(stdout);
#endif

    size_t send_size = send_req->size;
    size_t size = recv_req->size;

    if(send_size < size) {
        //Adjust receive count
        recv_req->size = send_size;
        size = send_size;
    }

#ifdef HMPI_SAFE
    if(unlikely(send_size > recv_size)) {
        printf("[recv] message of size %i truncated to %i (doesn't fit in matching receive buffer)!\n", sendsize, recv_req->size);
    }
#endif

#if 0
    printf("[%i] memcpy %p -> %p (%i)\n",
            g_hmpi_rank, send_req->buf, recv_req->buf, size);
    fflush(stdout);
#endif


    if(size < BLOCK_SIZE * 2) {
    //if(size < 128) {
    //if(size < 1024 * 1024) {
        memcpy((void*)recv_req->buf, send_req->buf, size);
    } else {
        //The setting of send_req->match_req signals to sender that they can
        // start doing copying as well, if they are testing the req.

        //recv_req->match_req = send_req; //TODO - keep this here?
        send_req->match_req = recv_req;
        LOCK_CLEAR(&send_req->match);

        uintptr_t rbuf = (uintptr_t)recv_req->buf;
        uintptr_t sbuf = (uintptr_t)send_req->buf;
        volatile ssize_t* offsetptr = &send_req->offset;
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
    printf("[%d] completed local-level RECV buf %p count %d source %d tag %d\n",
            g_hmpi_rank, recv_req->buf, recv_req->count, recv_req->proc, recv_req->tag);
    printf("[%d] completed local-level SEND buf %p count %d dest %d tag %d\n",
            send_req->proc, send_req->buf, send_req->count, g_hmpi_rank, send_req->tag);
    fflush(stdout);
#endif
    return 1;
}


//For req->type == MPI_SEND || req->type == MPI_RECV
static inline int HMPI_Progress_mpi(HMPI_Request req)
{
    int flag;

    MPI_Test(&req->req, &flag, MPI_STATUS_IGNORE);

    update_reqstat(req, flag);

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
    //TODO - this visits most recent receives first.  maybe iterate backwards
    // to progress oldest receives first?
    HMPI_Item* cur;
    HMPI_Item* next;
    HMPI_Item* tail = &g_recv_reqs_tail;
    HMPI_Request req;

    //Progress receive requests.
    for(cur = g_recv_reqs_head.next; cur != tail; cur = next) {
        req = (HMPI_Request)cur;
        next = cur->next;

//        if(get_reqstat(req) == HMPI_REQ_COMPLETE) {
//            printf("%d req %p type %d proc %d complete but is on recv_reqs list\n", g_hmpi_rank, req, req->type, req->proc);
//        }

        if(likely(req->type == HMPI_RECV)) {
            HMPI_Progress_recv(req);
#if 0
        //Currently this never happens; MPI recvs aren't added to g_recv_reqs.
        } else if(cur->type == MPI_RECV) {
            int flag;
            MPI_Test(&cur->req, &flag, MPI_STATUS_IGNORE);
            update_reqstat(cur, flag);
            remove_recv_req(cur);
#endif
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            if(HMPI_Progress_recv(req)) {
                //Local match & completion
                continue;
            } else if(g_size > 1) {
                // check if we can get something via the MPI library
                int flag=0;
                MPI_Status status;

                LOCK_SET(&g_mpi_lock);
                MPI_Iprobe(MPI_ANY_SOURCE, req->tag,
                        g_tcomms[g_tl_tid], &flag, &status);
                if(flag) {
                    int count;
                    MPI_Get_count(&status, req->datatype, &count);

                    MPI_Recv((void*)req->buf, count, req->datatype,
                          status.MPI_SOURCE, req->tag,
                          g_tcomms[g_tl_tid], &status);
                    LOCK_CLEAR(&g_mpi_lock);

                    remove_recv_req(req);

                    int type_size;
                    MPI_Type_size(req->datatype, &type_size);

                    req->size = count * type_size;
                    req->proc = status.MPI_SOURCE;
                    req->tag = status.MPI_TAG;
                    update_reqstat(req, HMPI_REQ_COMPLETE);
#ifdef DEBUG
                    printf("[%d] completed MPI-level RECV ANY_SRC buf %p count %d source %d tag %d\n", g_hmpi_rank, req->buf, req->count, status.MPI_SOURCE, req->tag);
                    fflush(stdout);
#endif
                } else {
                    LOCK_CLEAR(&g_mpi_lock);
                }
            }
        }
    }
}


#if 0
static inline int HMPI_Progress_request(HMPI_Request req)
{
#if 0
  if(req->stat == HMPI_REQ_COMPLETE) {
      //This is possible but unlikely -- another thread can complete the req
      //between the check outside this function, and then here.
      printf("%d A progressing complete req %p\n", g_hmpi_rank, req);
  }
#endif
  HMPI_Progress();

#if 0
  if(req->stat == HMPI_REQ_COMPLETE) {
      //This happens more often, since progress can finish req if it is a recv
      // or a send to self.  Or another thread can race and complete.
      printf("%d B progressing complete req %p type %d\n", g_hmpi_rank, req, req->type);
  }
#endif

  if(req->type == HMPI_SEND) {
      return HMPI_Progress_send(req);
  } else if(req->type == HMPI_RECV) {
      return get_reqstat(req);
  } else if(req->type == MPI_SEND || req->type == MPI_RECV) {
    int flag;

    MPI_Test(&req->req, &flag, MPI_STATUS_IGNORE);

    update_reqstat(req, flag);
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
  } else /*if(req->type == HMPI_RECV_ANY_SOURCE)*/ {
      return get_reqstat(req);
  } //HMPI_RECV_ANY_SOURCE

  return 0;
}
#endif


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

        if(!f) {
            *flag = 0;
            return MPI_SUCCESS;
        }

        //if(!HMPI_Progress_request(req)) {
        //    *flag = 0;
        //    return MPI_SUCCESS;
        //}
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
            //sched_yield();
        } while(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE);
    } else if(req->type == HMPI_RECV) {
        do {
            HMPI_Progress();
            //sched_yield();
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
    } else if(req->type == MPI_RECV || req->type == MPI_SEND) {
        do {
            HMPI_Progress();
            //sched_yield();
        } while(HMPI_Progress_mpi(req) != HMPI_REQ_COMPLETE);
        //Shortcut for now -- put progress for MPI types in its own fn
        //while(HMPI_Progress_request(req) != HMPI_REQ_COMPLETE);
    } else { //HMPI_RECV_ANY_SOURCE
        do {
            HMPI_Progress();
            //sched_yield();
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
    }

    //while(!HMPI_Progress_request(req));

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
//    int flag;
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
                    if(!HMPI_Progress_send(req)) {
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

#if 0    
            if(statuses == HMPI_STATUSES_IGNORE) {
                HMPI_Test(&requests[i], &flag, HMPI_STATUS_IGNORE);
            } else {
                HMPI_Test(&requests[i], &flag, &statuses[i]);
            }

            if(flag) {
                done += 1;
            }
#endif
        }
    } while(done < count);

  return MPI_SUCCESS;
}


int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status)
{
    int flag;
    int done;

    //Done is used to stop waiting if all requests are NULL.
    for(done = 1; done == 0; done = 1) {
        for(int i = 0; i < count; i++) {
            if(requests[i] == HMPI_REQUEST_NULL) {
                continue;
            }

            HMPI_Test(&requests[i], &flag, status);
            if(flag) {
                *index = i;
                return MPI_SUCCESS;
            }
            done = 0;
        }
    }

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
    printf("[%i] HMPI_Isend(%p, %i, %p, %i, %i, %p, %p) (proc null: %i)\n", g_hmpi_rank, buf, count, (void*)datatype, dest, tag, comm, req, MPI_PROC_NULL);
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

    int type_size;
    MPI_Type_size(datatype, &type_size);

    update_reqstat(req, HMPI_REQ_ACTIVE);

    req->proc = g_hmpi_rank; // my local rank
    req->tag = tag;
    req->size = count * type_size;
    req->datatype = datatype;
    req->buf = buf;

    if(unlikely(dest == MPI_PROC_NULL)) { 
        req->type = HMPI_SEND;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        return MPI_SUCCESS;
    }

    int target_mpi_rank = dest / g_nthreads;
    if(target_mpi_rank == g_rank) {
        req->type = HMPI_SEND;
        req->match_req = NULL;
        req->offset = 0;
        LOCK_INIT(&req->match, 1);

        int target_mpi_thread = dest % g_nthreads;
        add_send_req(req, target_mpi_thread);

//        printf("[%i] LOCAL sending to thread %i at rank %i tag %d  %p %p\n", g_hmpi_rank, target_mpi_thread, target_mpi_rank, tag, req, g_send_reqs[target_mpi_thread]);
//        fflush(stdout);
    } else {
        req->type = MPI_SEND;

        int target_thread = dest % g_nthreads;
        //printf("%i MPI send to rank %d (%d) tag %d\n", g_hmpi_rank, dest, target_mpi_rank, tag);
        //fflush(stdout);
        MPI_Isend(buf, count, datatype, target_mpi_rank, tag, g_tcomms[target_thread], &req->req);
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
  req->datatype = datatype;
  req->buf = buf;

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

    add_recv_req(req);
    HMPI_Progress_recv(req);
  } else if(source_mpi_rank == g_rank) { //Recv on-node, but not ANY_SOURCE
    // recv from other thread in my process
    req->type = HMPI_RECV;

    add_recv_req(req);
    HMPI_Progress_recv(req);
  } else { //Recv off-node, but not ANY_SOURCE
    //printf("%d MPI recv buf %p count %d src %d (%d) tag %d req %p\n", g_hmpi_rank, buf, count, source, source_mpi_rank, tag, req);
    //fflush(stdout);

    MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, g_tcomms[g_tl_tid], &req->req);

    req->type = MPI_RECV;
    //add_recv_req(req);
  }

  return MPI_SUCCESS;
}


int HMPI_Recv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status) {
  HMPI_Request req;
  HMPI_Irecv(buf, count, datatype, source, tag, comm, &req);
  HMPI_Wait(&req, status);
  return MPI_SUCCESS;
}


//
// Collectives
//

#if 0
int HMPI_Barrier(HMPI_Comm comm) {
#ifdef DEBUG
    printf("in HMPI_Barrier\n"); fflush(stdout);
#endif

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    if(g_size > 1) {
        // all root-threads perform MPI_Barrier 
        if(g_tl_tid == 0) {
            MPI_Barrier(comm->mpicomm);
        }

        barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
    }

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
    //MPI_Aint extent, lb;
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
    //MPI_Aint extent, lb;
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

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    //One rank posts a recv if mpi rank > 0
    if(g_tl_tid == 0 && g_rank > 0) {
        comm->rbuf[0] = memalign(4096, size * count);
        MPI_Irecv((void*)comm->rbuf[0], count, datatype,
                g_rank - 1, HMPI_SCAN_TAG, MPI_COMM_WORLD, &req);
    }

    //Each rank reduces local ranks below it
    //Copy my own receive buffer first
    memcpy(recvbuf, sendbuf, size * count);

    //Intentionally skip reducing self due to copy above
    for(i = 0; i < g_tl_tid; i++) {
        NBC_Operation(recvbuf,
                recvbuf, (void*)comm->sbuf[i], op, datatype, count);
    }

    //Wait on recv; all ranks reduce if mpi rank > 0
    if(g_tl_tid == 0 && g_rank > 0) {
        MPI_Wait(&req, MPI_STATUS_IGNORE);
    }

    barrier(&comm->barr, g_tl_tid);

    if(g_rank > 0) {
        NBC_Operation(recvbuf,
                recvbuf, (void*)comm->rbuf[0], op, datatype, count);

        barrier(&comm->barr, g_tl_tid);

        if(g_tl_tid == 0) {
            free((void*)comm->rbuf[0]);
        }
    }

    //Last rank sends result to next mpi rank if < size - 1
    if(g_tl_tid == g_nthreads - 1 && g_rank < g_size - 1) {
        MPI_Send(recvbuf, count, datatype,
                g_rank + 1, HMPI_SCAN_TAG, MPI_COMM_WORLD);
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


#define HMPI_GATHERV_TAG 76361347
int HMPI_Gatherv(void* sendbuf, int sendcnt, MPI_Datatype sendtype, void* recvbuf, int* recvcnts, int* displs, MPI_Datatype recvtype, int root, HMPI_Comm comm)
{
    //each sender can have a different send count.
    //recvcnts[i] must be equal to rank i's sendcnt.
    //only sendbuf, sendcnt, sendtype, root, comm meaningful on senders

    //How do I want to do this?
    //Challenge is that only the root knows the displacements.
    //Could simply have each HMPI rank send to the root.
    //Except for local ranks -- root can copy those.
    // Actually, root can post its displ list locally, and locals copy.
    //To reduce messages, each node builds a dtype covering its senders.
    // Root then builds a dtype for each remote node.
    // Results in one message from each node.
    // May not be any better due to dtype overhead and less overlap.

    //Root posts displs to local threads, who can then start copying.
    //Root receives from all non-local ranks, waits for completion.
    //Root waits for local threads to finish their copy.

    //Do it like this -- root on each node builds a dtype
    //Then just call MPI_Gatherv

    //Everybody posts their send info
    int tid = g_tl_tid;
    comm->sbuf[tid] = sendbuf;
    comm->scount[tid] = sendcnt;
    comm->stype[tid] = sendtype;
    comm->rbuf[tid] = recvbuf;
    comm->mpi_rbuf = displs;

    barrier_cb(&comm->barr, tid, barrier_iprobe);

    //One rank on each node builds the dtypes and does the MPI_Gatherv
    if(tid == root % g_nthreads) {
        MPI_Datatype dtsend;

        //I have g_nthreads blocks, with their own buf, cnt, type.
        MPI_Type_create_struct(g_nthreads, (int*)comm->scount, (MPI_Aint*)comm->sbuf, (MPI_Datatype*)comm->stype, &dtsend);
        MPI_Type_commit(&dtsend);

        if(root == g_hmpi_rank) {
            //I am root; create recv dtype.
            //It'll have an entry for each other node.
            //Blah.. this is a lot of dtypes..
            //Even if I do just send/recvs, I have to create all of these..
            //Build one datatype for every other node.
            int rank = g_rank;
            MPI_Datatype* dtrecvs = (MPI_Datatype*)alloca(sizeof(MPI_Datatype) * g_size);
            MPI_Request* reqs = (MPI_Request*)alloca(sizeof(MPI_Request) * g_size);


            for(int i = 0; i < g_size; i++) {
                if(rank == g_rank) {
                    //We do local copies in the root rank's node.
                    dtrecvs[i] = MPI_DATATYPE_NULL;
                    reqs[i] = MPI_REQUEST_NULL;
                    continue;
                }

                MPI_Type_indexed(g_nthreads,
                        &recvcnts[i * g_nthreads], &displs[i * g_nthreads],
                        recvtype, &dtrecvs[i]);

                MPI_Type_commit(&dtrecvs[i]);

                MPI_Irecv((void*)((uintptr_t)recvbuf + displs[i * g_nthreads]),
                            1, dtrecvs[i],
                            i, HMPI_GATHERV_TAG, comm->mpicomm, &reqs[i]);
            }

            //Is it possible to use MPI_Gatherv at all?
            //I have to flatten a list of per-rank displs into per-node,
            //with one base dtype.  Might be possible with extent ugliness
            //On the other hand, sends/recvs allow me to make a dtype for
            //each sender.  More simple..

            MPI_Waitall(g_size, reqs, MPI_STATUSES_IGNORE);
            for(int i = 0; i < g_size; i++) {
                MPI_Type_free(&dtrecvs[i]);
            }
        } else {
            //MPI_Gatherv(MPI_BOTTOM, 1, dtsend, NULL, NULL, MPI_DATATYPE_NULL,
            //        root, comm->mpicomm);
            MPI_Send(MPI_BOTTOM, 1, dtsend, root / g_nthreads, HMPI_GATHERV_TAG, comm->mpicomm);
        }

        MPI_Type_free(&dtsend);
    } else if(HMPI_Comm_local(comm, root)) {
        //Meanwhile, all local non-root ranks do memcpys.
        int root_tid;
        HMPI_Comm_thread(comm, root, &root_tid);
        int* displs = (int*)comm->mpi_rbuf;
        void* buf = (void*)comm->rbuf[root_tid];
        int size;

        MPI_Type_size(sendtype, &size);

        //Copy my data from sendbuf to the root.
        memcpy((void*)((uintptr_t)buf + displs[g_hmpi_rank]),
                sendbuf, size * sendcnt);
    }

    barrier(&comm->barr, tid);
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

#ifdef HMPI_SAFE
  if(send_extent != send_size || recv_extent != recv_size) {
    printf("gather non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->mpicomm, 0);
  }

  if(size != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
 
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
  //void* rbuf;
  int32_t send_size;
  uint64_t size;
  MPI_Request* send_reqs = NULL;
  MPI_Request* recv_reqs = NULL;
  MPI_Datatype dt_send;
  MPI_Datatype dt_recv;

  MPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
  int32_t recv_size;
  MPI_Aint send_extent, recv_extent, lb;

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
    int32_t send_size;
    int thr;
    int tid = g_tl_tid;

    MPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
    MPI_Aint send_extent, recv_extent, lb;
    int32_t recv_size;

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

  barrier(&comm->barr, tid);

  //Push local data to each other thread's receive buffer.
  //For each thread, memcpy from my send buffer into their receive buffer.

  for(thr = 1; thr < g_nthreads; thr++) {
      int t = (tid + thr) % g_nthreads;
      memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)),
             (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len)) , copy_len);
      //memcpy((void*)((uintptr_t)comm->rbuf[thr] + (g_tl_tid * copy_len)),
      //       (void*)((uintptr_t)sendbuf + (thr * copy_len)) , copy_len);
  }

  barrier(&comm->barr, tid);
  return MPI_SUCCESS;
}

#endif

