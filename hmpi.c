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
} HMPI_Request_list;

static HMPI_Request_list* g_send_reqs = NULL;
static __thread HMPI_Item* g_tl_send_reqs_head;

//Pool of unused reqs to save malloc time.
static __thread HMPI_Item* g_free_reqs = NULL;

static inline HMPI_Request acquire_req(void)
{
    //Malloc a new req only if none are in the pool.
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
    //Return a req to the pool -- once allocated, a req is never freed.
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

    HMPI_Item* pred =
        (HMPI_Item*)fetch_and_store((void**)&req_list->tail, item);

    if(pred != NULL) {
        //Another req is ahead of us in the queue.
        pred->next = item;

        //Skip waiting on locked to clear -- no blocking here.
    } else {
        //Set the head -- err, this is a race isnt it?
        //Actually this should never happen, since tail is initialized to head.
        printf("ERROR hit bad case in add_send_req\n");
        MPI_Abort(MPI_COMM_WORLD, 0);
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
    if(cur == g_recv_reqs_tail) {
        g_recv_reqs_tail = prev;
        prev->next = NULL;
    } else {
        prev->next = cur->next;
    }
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
                //Looks like this is the tail -- CAS prev in.
                HMPI_Request_list* req_list = &g_send_reqs[g_tl_tid];
                prev->next = NULL;

                //STORE_FENCE(); //TODO - necessary? does CAS imply fence?

                HMPI_Item* ptr = CAS_PTR_VAL(&req_list->tail, cur, prev);
                if(ptr != cur) {
                    //A new req was added between the branch and the CAS.
                    //Wait for cur->next to go non-NULL -- shouldnt take long.
                    HMPI_Item* volatile* vol_next = &cur->next;
                    HMPI_Item* next;

                    while((next = *vol_next) == NULL);
                    prev->next = next;
                }
            } else {
                //Not the tail; just remove.
                prev->next = cur->next;
            }

            recv_req->proc = req->proc;
            recv_req->tag = req->tag;
            *send_req = req;
            return 1;
        }
    }

    return 0;
}


static inline int match_probe(int source, int tag, HMPI_Comm comm, HMPI_Request* send_req) {
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
    g_recv_reqs_tail = &g_recv_reqs_head;

    g_send_reqs[rank].head.next = NULL;
    g_send_reqs[rank].tail = &g_send_reqs[rank].head;
    g_tl_send_reqs_head = &g_send_reqs[rank].head;

    PROFILE_INIT(g_tl_tid);

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

  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
  assert(MPI_THREAD_MULTIPLE == provided);

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
        while(get_reqstat(send_req) != HMPI_REQ_COMPLETE);
        return HMPI_REQ_COMPLETE;
    }

    return HMPI_REQ_ACTIVE;
}


//For req->type == HMPI_RECV
static inline int HMPI_Progress_recv(HMPI_Request recv_req)
{
    //Try to match from the local send reqs list
    HMPI_Request send_req = NULL;

    if(!match_recv(recv_req, &send_req)) {
        return 0;
    }

    //Remove the recv from the pending recv list
    //remove_recv_req(recv_req);

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
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    //Progress receive requests.
    //We remove items from the list, but they are still valid; nothing in this
    //function will free or overwrite a req.
    //So, it's safe to do cur = cur->next.
    for(prev = &g_recv_reqs_head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

//        if(get_reqstat(req) == HMPI_REQ_COMPLETE) {
//            printf("%d req %p type %d proc %d complete but is on recv_reqs list\n", g_hmpi_rank, req, req->type, req->proc);
//        }

        if(likely(req->type == HMPI_RECV)) {
            //TODO - this branch could be eliminated by passing prev in.
            if(HMPI_Progress_recv(req) == 1) {
                remove_recv_req(prev, cur);
            }
#if 0
        //Currently this never happens; MPI recvs aren't added to g_recv_reqs.
        } else if(cur->type == MPI_RECV) {
            int flag;
            MPI_Test(&cur->req, &flag, MPI_STATUS_IGNORE);
            update_reqstat(cur, flag);
            remove_recv_req(cur);
#endif
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            if(HMPI_Progress_recv(req) == 1) {
                //Local match & completion
                remove_recv_req(prev, cur);
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

                    remove_recv_req(prev, cur);

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
    //HMPI_Progress_recv(req);
  } else if(source_mpi_rank == g_rank) { //Recv on-node, but not ANY_SOURCE
    // recv from other thread in my process
    req->type = HMPI_RECV;

    add_recv_req(req);
    //HMPI_Progress_recv(req);
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

