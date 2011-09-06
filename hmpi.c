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
#include "profile2.h"
#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
//#include "pipelinecopy.h"

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


//PROFILE_DECLARE();
//PROFILE_VAR(mpi);


//Use the pipelined messaging approach, first just for alltoall.
//Allocate a table of buffers, g_nthreads^2.  makes synchronization easy.
//Start just by using send_msg/recv_msg
//Next try a more nonblocking approach, progressing each write until
// it cant anymore, then checking the next transfer.


//buffer_t** g_buffers = NULL;

#if 0
#define SHORT_BUF_SIZE 64

typedef struct short_req_t {
    HMPI_Request req;
    uint8_t buf[SHORT_BUF_SIZE];
} short_req_t;

//Will this help?  We fall back to two copies, which might be OK for small.
//Reduces synchronization -- send can complete before recv is posted.
static short_req_t g_short_req;
#endif

int g_nthreads=-1;
int g_rank=-1;
static __thread int g_hmpi_rank=-1;
static int g_size=-1;

HMPI_Comm HMPI_COMM_WORLD;

static MPI_Comm* g_tcomms;

static __thread int g_tl_tid=-1;

static void (*g_entry)();

//Each thread has a list of send and receive requests.
//The receive requests are managed only by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them.

static __thread HMPI_Request* g_recv_reqs = NULL;

typedef struct send_req_info_t {
    HMPI_Request* reqs;
    pthread_mutex_t mut;
    char padding[64 - (sizeof(void*) + sizeof(pthread_mutex_t))];
} send_req_info_t;

//static HMPI_Request** g_send_reqs = NULL;
//static pthread_mutex_t* g_send_muts = NULL;
static send_req_info_t* g_send_reqs = NULL;


static inline void add_recv_req(HMPI_Request *req) {
  req->next = g_recv_reqs;
  req->prev = NULL;
  if(req->next != NULL) {
    req->next->prev = req;
  }

  g_recv_reqs = req;
}


static inline void add_send_req(HMPI_Request *req, int tid) {
  send_req_info_t* reqinfo = &g_send_reqs[tid];

  //Set req->next = head
  //CAS head with req
  //if result is not head, repeat
  HMPI_Request* next;
  do {
      next = req->next = reqinfo->reqs;
  } while(!CAS_PTR_BOOL(&reqinfo->reqs, next, req));

#if 0
  send_req_info_t* reqinfo = &g_send_reqs[tid];

  pthread_mutex_lock(&reqinfo->mut);
  req->next = reqinfo->reqs;

  //if(req->next != NULL) {
  //  req->next->prev = req;
  //}

  reqinfo->reqs = req;
  pthread_mutex_unlock(&reqinfo->mut);
#endif
}


static inline void remove_recv_req(HMPI_Request *req) {
  if(req->prev == NULL) {
      //Head of list
      g_recv_reqs = req->next;
  } else {
      req->prev->next = req->next;
  }

  if(req->next != NULL) {
      req->next->prev = req->prev;
  }

}


//TODO AWF - never used; match_recv removes reqs directly
#if 0
static inline void remove_send_req(HMPI_Request *req) {
  send_req_info_t* reqinfo = &g_send_reqs[g_tl_tid];
  pthread_mutex_lock(&reqinfo->mut);

  if(req->prev == NULL) {
      //Head of list
      reqinfo->reqs = req->next;
  } else {
      req->prev->next = req->next;
  }

  if(req->next != NULL) {
      req->next->prev = req->prev;
  }

  pthread_mutex_unlock(&reqinfo->mut);

#if 0
  pthread_mutex_lock(&g_send_muts[g_tl_tid]);
  if(req->prev == NULL) {
      //Head of list
      g_send_reqs[g_tl_tid] = req->next;
  } else {
      req->prev->next = req->next;
  }

  if(req->next != NULL) {
      req->next->prev = req->prev;
  }

  pthread_mutex_unlock(&g_send_muts[g_tl_tid]);
#endif
}
#endif

static inline int match_recv(HMPI_Request* recv_req, HMPI_Request** send_req) {
    //Iterate over g_send_reqs[g_tl_tid]
    HMPI_Request* cur;
    HMPI_Request* prev;

    //pthread_mutex_lock(&g_send_reqs[g_tl_tid].mut);
    //for(cur = g_send_reqs[g_tl_tid]; cur != NULL; cur = cur->next) {
    for(prev = NULL, cur = g_send_reqs[g_tl_tid].reqs;
            cur != NULL; prev = cur, cur = cur->next) {
        //The send request can't have ANY_SOURCE or ANY_TAG
        if(cur->proc == recv_req->proc ||
                recv_req->proc == MPI_ANY_SOURCE) {
            if(cur->tag == recv_req->tag || recv_req->tag == MPI_ANY_TAG) {
                //If this element is the head, CAS head with cur->next
                if(prev == NULL) {
                    //Head of list -- CAS to remove
                    if(!CAS_PTR_BOOL(&g_send_reqs[g_tl_tid].reqs, cur, cur->next)) {
                        //Element is no longer the head.. find its prev then
                        // remove it.
                        for(prev = g_send_reqs[g_tl_tid].reqs;
                                prev->next != cur; prev = prev->next);
                        prev->next = cur->next;
                    }
                } else {
                    //Not at head of list, just remove.
                    prev->next = cur->next;
                }


#if 0
                //if(cur->prev == NULL) {
                if(prev == NULL) {
                    //Head of list
                    g_send_reqs[g_tl_tid].reqs = cur->next;
                } else {
                    //cur->prev->next = cur->next;
                    prev->next = cur->next;
                }

                //if(cur->next != NULL) {
                //    cur->next->prev = cur->prev;
                //}
                
                pthread_mutex_unlock(&g_send_reqs[g_tl_tid].mut);
#endif
                *send_req = cur;
                return 1;
            }
        }
    }

    //pthread_mutex_unlock(&g_send_reqs[g_tl_tid].mut);
    return 0;
}


static inline void update_reqstat(HMPI_Request *req, int stat) {
  //pthread_mutex_lock(&req->statlock);
  req->stat = stat;
  //OPA_swap_int(&req->stat, stat);
  //pthread_mutex_unlock(&req->statlock);
}


static inline int get_reqstat(HMPI_Request *req) {
  int stat;
  //pthread_mutex_lock(&req->statlock);
  stat = req->stat;
  //return OPA_load_int(&req->stat);
  //pthread_mutex_unlock(&req->statlock);
  return stat;
}


// this is called by pthread create and then calls the real function!
void* trampoline(void* tid) {
  // save thread-id in thread-local storage
  g_tl_tid = (int)(unsigned long)tid;
  g_hmpi_rank = g_rank*g_nthreads+(int)(unsigned long)tid;

//  printf("%d:%d entered trampoline\n", g_rank, g_tl_tid); fflush(stdout);

  //g_buffers[(unsigned long)tid] = (buffer_t*)memalign(4096, sizeof(buffer_t) * g_nthreads);
  //memset(g_buffers[(unsigned long)tid], 0, sizeof(buffer_t) * g_nthreads);


  // barrier to avoid race in tid ...
  barrier(&HMPI_COMM_WORLD->barr, g_tl_tid);
  //while(!barrier_test(&HMPI_COMM_WORLD->barr));

  //printf("%d:%d g_entry now\n", g_rank, g_tl_tid); fflush(stdout);
  // call user function
  g_entry();

  return NULL;
}


int HMPI_Init(int *argc, char ***argv, int nthreads, void (*start_routine)())
{
  pthread_t* threads;
  int provided;
  long int thr;

//  printf("before MPI_Init\n"); fflush(stdout);
  MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
  assert(MPI_THREAD_MULTIPLE == provided);
#ifdef DEBUG
  printf("after MPI_Init\n"); fflush(stdout);
#endif

  //PROFILE_INIT();
  
  g_nthreads = nthreads;
  g_entry = start_routine;

  //g_buffers = (buffer_t**)malloc(sizeof(buffer_t*) * nthreads * nthreads);

  HMPI_COMM_WORLD = (HMPI_Comm_info*)malloc(sizeof(HMPI_Comm_info));
  HMPI_COMM_WORLD->mpicomm = MPI_COMM_WORLD;
  barrier_init(&HMPI_COMM_WORLD->barr, nthreads);

  //HMPI_COMM_WORLD->sinfo = (HMPI_Data_info*)malloc(sizeof(HMPI_Data_info) * nthreads);
  //HMPI_COMM_WORLD->rinfo = (HMPI_Data_info*)malloc(sizeof(HMPI_Data_info) * nthreads);
  HMPI_COMM_WORLD->sbuf = (volatile void**)malloc(sizeof(void*) * nthreads);
  HMPI_COMM_WORLD->rbuf = (volatile void**)malloc(sizeof(void*) * nthreads);
  HMPI_COMM_WORLD->scount = (volatile int*)malloc(sizeof(int) * nthreads);
  HMPI_COMM_WORLD->rcount = (volatile int*)malloc(sizeof(int) * nthreads);
  HMPI_COMM_WORLD->stype = (volatile MPI_Datatype*)malloc(sizeof(MPI_Datatype) * nthreads);
  HMPI_COMM_WORLD->rtype = (volatile MPI_Datatype*)malloc(sizeof(MPI_Datatype) * nthreads);


  threads = (pthread_t*)malloc(sizeof(pthread_t) * nthreads);

  //g_send_reqs = (HMPI_Request**)malloc(sizeof(HMPI_Request*) * nthreads);
  //g_send_muts = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * nthreads);
  g_send_reqs = (send_req_info_t*)malloc(sizeof(send_req_info_t) * nthreads);

  //g_tcomms.resize(nthreads);
  g_tcomms = (MPI_Comm*)malloc(sizeof(MPI_Comm) * nthreads);

#ifdef _PROFILE
    //g_profile_info = (profile_info_t*)calloc(sizeof(struct profile_info_t), nthreads);
#endif

  MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &g_size);

 
  //Do per-thread initialization that must be complete for all threads before
  // actually starting the threads. 
  for(thr=0; thr < nthreads; thr++) {
    // create one world communicator for each thread 
    MPI_Comm_dup(MPI_COMM_WORLD, &g_tcomms[thr]);

    // Initialize send requests list and lock
    g_send_reqs[thr].reqs = NULL;
    //pthread_mutex_init(&g_send_reqs[thr].mut, NULL);
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

  pthread_attr_t attr;
  for(thr=0; thr < nthreads; thr++) {
    //Create the thread
    pthread_attr_init(&attr);
    int rc = pthread_create(&threads[thr], &attr, trampoline, (void *)thr);

    //Set affinity -- pin each thread to one core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thr, &cpuset);

    rc = pthread_setaffinity_np(threads[thr], sizeof(cpu_set_t), &cpuset);
    //rc = pthread_setaffinity_np(threads[thr], sizeof(cpu_set_t), &cpuset[thr / 6]);
    if(rc) {
      printf("%d:%ld pthread_setaffinity_np error %s\n", g_rank, thr, strerror(rc));
      MPI_Abort(MPI_COMM_WORLD, 0);
    }
  }


  for(thr=0; thr<nthreads; thr++) {
    pthread_join(threads[thr], NULL);
  }

  free(g_send_reqs);
  //free(g_send_muts);
  free(threads);
  free(g_tcomms);
  return 0;
}


int HMPI_Comm_rank(HMPI_Comm comm, int *rank) {
  
  //printf("[%i] HMPI_Comm_rank()\n", g_rank*g_nthreads+g_tl_tid);
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
    
  //*rank = g_nthreads*g_rank+g_tl_tid;
  *rank = g_hmpi_rank;
  return 0;
}


int HMPI_Comm_size ( HMPI_Comm comm, int *size ) {
  
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
    
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
  //barrier_wait(&comm->barr);
}


int HMPI_Finalize() {

  HMPI_Barrier(HMPI_COMM_WORLD);

  int r;
  HMPI_Comm_rank(HMPI_COMM_WORLD, &r);
  //PROFILE_SHOW_REDUCE(mpi);
  //PROFILE_SHOW_REDUCE(copy, r);
  //PROFILE_SHOW_REDUCE(send, r);
  //PROFILE_SHOW_REDUCE(add_send_req, r);
  //PROFILE_SHOW_REDUCE(barrier, r);
  //PROFILE_SHOW_REDUCE(alltoall, r);

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
static inline int HMPI_Progress_send(HMPI_Request *req) {

    //Write blocks on this send req if receiver has matched it.
    //If mesage is short, receiver won't bother setting recv_buf, and instead
    // just does the copy and moves on.
    //uintptr_t rbuf = (uintptr_t)req->recv_buf;
    //if((void*)rbuf != NULL) {
    if(req->offset != 0) {
        uintptr_t rbuf = (uintptr_t)req->recv_buf;
        //We know the receiver has arrived, so work on copying.
        size_t size = req->size;
        uintptr_t sbuf = (uintptr_t)req->send_buf;
        size_t offset;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(&req->offset, BLOCK_SIZE)) < size) {
            req->sender_done = 0;
            int left = size - offset;
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }

        //STORE_FENCE();
        req->sender_done = 1;
        //Possible for the receiver to still be copying here.
        //But, not always.  How do I handle completion?
        //TODO - for now just return whatever stat is; receiver marks done.
    } else {
        //Poll local receives in the recv reqs list.
        // We do this to prevent deadlock when local threads are exchange messages
        // with each other.  Normally no work is done on a send request, but if
        // the app blocks waiting for it to complete, a neighbor thread could also
        // block on its send and neither of their receives are ever completed.

        //TODO - this visits most rescent receives first.  maybe iterate backwards
        // to progress oldest receives first?
        HMPI_Request* cur;

        for(cur = g_recv_reqs; cur != NULL; cur = cur->next) {
            HMPI_Progress_recv(cur);
        }

    }

    return get_reqstat(req);
}


static inline int HMPI_Progress_recv(HMPI_Request *recv_req) {
    //Try to match from the local send reqs list
    HMPI_Request* send_req;

    if(!match_recv(recv_req, &send_req)) {
        return 0;
    }

#ifdef DEBUG
    printf("[%i] [recv] found send from %i (%x) for buf %x in uq (tag: %i, size: %i, status: %x)\n",
            g_hmpi_rank, send_req->proc, send_req->buf, recv_req->buf, send_req->tag, send_req->size, get_reqstat(send_req));
#endif

    int sendsize = send_req->size;
    if(unlikely(sendsize != recv_req->size)) {
        //printf("[recv] message of size %i truncated to %i (doesn't fit in matching receive buffer)!\n", sendsize, recv_req->size);
        sendsize = recv_req->size;
    }

    // copy message to destination
    //printf("[%i] memcpy %p -> %p (%i)\n",
    //        g_hmpi_rank, send_req->buf, recv_req->buf, sendsize);
    //fflush(stdout);

    //printf("%d memcpy %p %p\n", g_tl_tid, recv_req->buf, send_req->buf);
    //fflush(stdout);
    if(sendsize < 32) {
        memcpy((void*)recv_req->recv_buf, send_req->send_buf, sendsize);
    } else {
        //The setting of send_req->recv_buf signals to sender that they can
        // start doing copying as well, if they are testing the req.
        uintptr_t rbuf = (uintptr_t)(send_req->recv_buf = recv_req->recv_buf);
        uintptr_t sbuf = (uintptr_t)send_req->send_buf;
        size_t offset = 0;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD(&send_req->offset, BLOCK_SIZE)) < sendsize) {
            int left = sendsize - offset;

            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < BLOCK_SIZE ? left : BLOCK_SIZE));
        }

        //Wait if the sender is copying.
        while(send_req->sender_done == 0);
//        LOAD_FENCE();
    }

    //Mark send and receive requests done
    update_reqstat(send_req, 1);
    update_reqstat(recv_req, 1);

    //Remove the recv from the request list
    remove_recv_req(recv_req);
    return 1;
}


static inline int HMPI_Progress_request(HMPI_Request *req) {
  //printf("%d progress type %d\n", g_hmpi_rank, req->type);
  //fflush(stdout);

  if(req->type == HMPI_SEND) {
      return HMPI_Progress_send(req);
  } else if(req->type == HMPI_RECV) {
      return HMPI_Progress_recv(req);

#if 0
    //TODO - Try to match from the local send reqs list


    // check if in UQ (needs to be done because the checks are not atomic)
    ruqelem_t *ret;

    //if(match(req, &uq[g_tl_tid], &ret)) {
    if(match(req, g_tl_tid, &ret)) {
#ifdef DEBUG
      printf("[%i] [recv] found send from %i (%x) for buf %x in uq (tag: %i, size: %i, status: %x)\n",
              g_rank*g_nthreads+g_tl_tid, ret->proc, ret->buf, req->buf, ret->tag, ret->size, get_reqstat(ret));
#endif

      int sendsize = ret->size;
      if(sendsize != req->size) {
        printf("[recv] message of size %i truncated to %i (doesn't fit in matching receive buffer)! %d\n", sendsize, req->size, uq[g_tl_tid].size());
        sendsize = req->size;
      }
      // copy message to destination
      //printf("[%i] memcpy %x -> %x (%i)\n",
      //        g_rank*g_nthreads+g_tl_tid, ret->buf, req->buf, sendsize);
      memcpy(req->buf, ret->buf, sendsize);
      //for(int k=0; k<sendsize; ++k) *((char*)req->buf+k)=*((char*)ret->buf+k);

      // mark send as done too
      update_reqstat(ret, 1);
      // mark request as done
      update_reqstat(req, 1);

      return 1;
    }
#endif
  } // HMPI_RECV
  else if(req->type == MPI_SEND || req->type == MPI_RECV) {
    int flag;

    //HMPI_Progress();

    //printf("%d testi req %d\n", g_hmpi_rank, req->type);
    PROFILE_START(mpi);
    MPI_Test(&req->req, &flag, req->status);
    PROFILE_STOP(mpi);

    update_reqstat(req, flag);
//    if(flag && req->type == MPI_RECV) {
//        remove_recv_req(req);
//    }
    return flag;
  }

  else if(req->type == HMPI_RECV_ANY_SOURCE) {
    if(HMPI_Progress_recv(req)) {
        return 1;
    }

#if 0
    // check if we received something from local thread
    ruqelem_t *ret;
    //if(match(req, &uq[g_tl_tid], &ret)) {
    if(match(req, g_tl_tid, &ret)) {
#ifdef DEBUG
      printf("[%i] [recv] found send from %i into %x in uq (tag: %i, size: %i, status: %x)\n", g_rank*g_nthreads+g_tl_tid, ret->proc, ret->buf, ret->tag, ret->size, get_reqstat(ret));
#endif
      int sendsize = ret->size;
      if(sendsize > req->size) {
        printf("[recv] message of size %i truncated to %i(doesn't fit in matching receive buffer)!\n", sendsize, req->size);
        sendsize = req->size;
      }

      // copy message to destination
      memcpy(req->buf, ret->buf, sendsize);

      // mark send as done too
      update_reqstat(ret, 1);
      // mark request as done
      update_reqstat(req, 1);

      return 1;
    } 
#endif

    // check if we can get something via the MPI library
    int flag=0;
    PROFILE_START(mpi);
    MPI_Iprobe(MPI_ANY_SOURCE, req->tag, req->comm, &flag, req->status);
    PROFILE_STOP(mpi);
    if(flag) {
      PROFILE_START(mpi);
      MPI_Recv((void*)req->recv_buf, req->size, req->datatype, req->status->MPI_SOURCE, req->tag, req->comm, req->status);
      PROFILE_STOP(mpi);
      remove_recv_req(req);
      return 1;
    }
    
  } //HMPI_RECV_ANY_SOURCE
  return 0;
}


static inline void HMPI_Progress() {
#if 0
    for(std::list<HMPI_Request*>::iterator iter=g_recv_reqs[g_tl_tid].begin();
          iter != g_recv_reqs[g_tl_tid].end(); ++iter) {
        HMPI_Progress_request(*iter);
    }
#endif
    //TODO - this visits most rescent receives first.  maybe iterate backwards
    // to progress oldest receives first?
    HMPI_Request* cur;
    for(cur = g_recv_reqs; cur != NULL; cur = cur->next) {
        HMPI_Progress_recv(cur);
    }

#if 0
  //if(g_allrequests[g_tl_tid].empty()) {
  //    return;
  //}

  //printf("progress req list size %d\n", g_allrequests[g_tl_tid].size());
  //fflush(stdout);
  for(std::list<HMPI_Request*>::iterator iter=g_allrequests[g_tl_tid].begin();
          iter != g_allrequests[g_tl_tid].end(); ++iter) {
    if(get_reqstat(*iter) != 1) {
        HMPI_Progress_request(*iter);
    }
#if 0
    if(HMPI_Progress_request(*iter)) {
        std::list<HMPI_Request*>::iterator old = iter;
        ++iter;
        g_allrequests[g_tl_tid].erase(old);
    } else {
        ++iter;
    }
#endif
  }
#endif
}


int HMPI_Test(HMPI_Request *req, int *flag, MPI_Status *status)
{
  if(get_reqstat(req) == 0) {
      //req->status = status;
      *flag = HMPI_Progress_request(req);
  } else {
    *flag = 1;
  }
  return MPI_SUCCESS;

#if 0
  //HMPI_Progress();
  if(get_reqstat(req) != 1) HMPI_Progress_request(req);

  if(get_reqstat(req) == 1) {
    //printf("req complete\n");
    update_reqstat(req, 0);
    remove_req(req);
    if(status!=MPI_STATUS_IGNORE) *status=req->status;
    *flag=1; 
  } else {
    *flag=0;
  }

  return MPI_SUCCESS;
#endif
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
    update_reqstat(req, 1);
    return MPI_SUCCESS;
  }

  req->status = MPI_STATUS_IGNORE;
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

  update_reqstat(req, 0);
  
  int target_mpi_rank = dest / g_nthreads;
  if(target_mpi_rank == g_rank) {
    int size;
    MPI_Type_size(datatype, &size);

    // send to other thread in my process
    req->type = HMPI_SEND;
    req->proc = g_hmpi_rank; // my local rank
    req->tag = tag;
    req->size = size*count;
    req->send_buf = buf;
    req->recv_buf = NULL;
    //req->offset = BLOCK_SIZE;
    req->offset = 0;
    req->sender_done = 1;

    int target_mpi_thread = dest % g_nthreads;

    //printf("[%i] LOCAL sending to thread %i at rank %i\n", g_nthreads*g_rank+g_tl_tid, target_mpi_thread, target_mpi_rank);
    add_send_req(req, target_mpi_thread);
  } else {
    //int target_mpi_thread = dest % g_nthreads;
    //printf("[%i] MPI sending to thread %i at rank %i\n", g_nthreads*g_rank+g_tl_tid, target_mpi_thread, target_mpi_rank);
    int size;
    MPI_Type_size(datatype, &size);
    req->type = MPI_SEND;
    req->proc = g_hmpi_rank; // my local rank
    req->tag = tag;
    req->size = size*count;
    req->send_buf = buf;

    MPI_Isend(buf, count, datatype, target_mpi_rank, tag, g_tcomms[g_tl_tid], &req->req);
  }

//  add_req(req);
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
    update_reqstat(req, 1);
    return MPI_SUCCESS;
  }

  req->status = MPI_STATUS_IGNORE;
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

  update_reqstat(req, 0);
  
  int source_mpi_rank = source / g_nthreads;
  if(source_mpi_rank == g_rank) {
    int size;
    MPI_Type_size(datatype, &size);

    // recv from other thread in my process
    req->type = HMPI_RECV;
    req->proc = source;
    req->tag = tag;
    req->size = size*count;
    //req->send_buf = NULL;
    req->recv_buf = buf;
    //req->offset = 0;

    //int tests=0;
    //while(HMPI_Progress_request(req) != 1 && ++tests<10);

    add_recv_req(req);
  } else if(source != MPI_ANY_SOURCE) {
    int source_mpi_thread = source % g_nthreads;
    //printf("%d buf %p count %d src %d tag %d req %p\n", g_rank*g_nthreads+g_tl_tid, buf, count, source, tag, req);

    int size;
    MPI_Type_size(datatype, &size);
    req->proc = source;
    req->tag = tag;
    req->size = size*count;
    req->recv_buf = buf;

    PROFILE_START(mpi);
    MPI_Irecv(buf, count, datatype, source_mpi_rank, tag, g_tcomms[source_mpi_thread], &req->req);
    PROFILE_STOP(mpi);

    req->type = MPI_RECV;
  } else if(source == MPI_ANY_SOURCE) {
    int size;
    MPI_Type_size(datatype, &size);

    // test both layers and pick first 
    req->type = HMPI_RECV_ANY_SOURCE;
    req->proc = source;
    req->tag = tag;
    req->size = size*count;
    req->recv_buf = buf;

    req->comm = g_tcomms[g_tl_tid]; // not 100% sure -- this doesn't catch all messages -- Probe would need to loop over all thread comms and lock :-(
    req->datatype = datatype;

    add_recv_req(req);
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

int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm) {

  MPI_Aint extent, lb;
  int size;
  int i;

  MPI_Type_size(datatype, &size);
  MPI_Type_get_extent(datatype, &lb, &extent);
  //MPI_Type_extent(datatype, &extent);

  if(extent != size) {
    printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
    fflush(stdout);
    MPI_Abort(comm->mpicomm, 0);
  }


#ifdef DEBUG
  printf("[%i %i] HMPI_Allreduce(%p, %p, %i, %p, %p, %p)\n", g_hmpi_rank, g_tl_tid, sendbuf, recvbuf,  count, datatype, op, comm);
  fflush(stdout);
#endif

  void* localbuf = NULL;

  if(g_tl_tid == 0) {
    localbuf = memalign(4096, size * count);
    memcpy(localbuf, sendbuf, size * count);
    //comm->rootsbuf = localbuf;
    //comm->rootrbuf = recvbuf;
    comm->sbuf[0] = localbuf;
    comm->rbuf[0] = recvbuf;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  for(i=1; i<g_nthreads; ++i) {
     if(g_tl_tid == i) {
         NBC_Operation((void*)comm->sbuf[0], (void*)comm->sbuf[0], sendbuf, op, datatype, count);
     }

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
    //barrier_wait(&comm->barr);
  }


  if(g_tl_tid == 0) {
    //printf("%d before mpi allreduce\n", g_rank); fflush(stdout);
    MPI_Allreduce((void*)comm->sbuf[0], (void*)comm->rbuf[0], count, datatype, op, comm->mpicomm);
    //printf("%d after mpi allreduce\n", g_rank); fflush(stdout);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //printf("%d doing allreduce copy\n", g_tl_tid); fflush(stdout);
  if(g_tl_tid != 0) memcpy(recvbuf, (void*)comm->rbuf[0], count*size);

  // protect from early leave (rootrbuf)
  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  //printf("%d done with allreduce\n", g_tl_tid); fflush(stdout);
  if(g_tl_tid == 0) {
    free(localbuf);
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
      comm->sbuf[0] = buffer;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(g_tl_tid == 0) {
    MPI_Bcast((void*)comm->sbuf[0], count, datatype, root, comm->mpicomm);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  //barrier_wait(&comm->barr);

  if(root % g_nthreads != g_tl_tid) {
    memcpy(buffer, (void*)comm->sbuf[0], count*size);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
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
  //barrier_wait(&comm->barr);

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
    MPI_Type_size(recvtype, &recv_size);

#if HMPI_SAFE
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
         (void*)((uintptr_t)sendbuf + (tid * copy_len)) , copy_len);

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


#if 0
int HMPI_Alltoall_local2(void* sendbuf, void* recvbuf, size_t copy_len, HMPI_Comm comm)
{
    int thr, i;
    int tid = g_tl_tid;

  comm->sbuf[tid] = sendbuf;
  comm->rbuf[tid] = recvbuf;


  //Do the self copy
  //int copy_len = len;
  memcpy((void*)((uintptr_t)recvbuf + (tid * copy_len)),
         (void*)((uintptr_t)sendbuf + (tid * copy_len)) , copy_len);

  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  barrier(&comm->barr, tid);

  //Push local data to each other thread's receive buffer.
  //For each thread, memcpy from my send buffer into their receive buffer.

  //TODO - try staggering
  for(thr = 1; thr < g_nthreads; thr++) {
      int t = (tid + thr) % g_nthreads;
      memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)),
             (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len)) , copy_len);
      //memcpy((void*)((uintptr_t)comm->rbuf[t] + (tid * copy_len)),
      //       (void*)((uintptr_t)sendbuf + (t * copy_len)) , copy_len);
  }


//for alltoall, i can set bufs, barrier, all threads copy, barrier
//easy checks -- if < 16k recver copies the whole thing
//if more, lower rank copies lower half, upper rank copies upper half

//do two for loops.. one up to tid, one from tid onward
#if 0
#define COPY_LIMIT 16384

  //int copy_len = len;
  int half_len = copy_len >> 1;

  //Do the self copy
  memcpy((void*)((uintptr_t)recvbuf + (tid * copy_len)),
         (void*)((uintptr_t)sendbuf + (tid * copy_len)), copy_len);


  if(copy_len < COPY_LIMIT) {
      for(thr = 1; thr < g_nthreads; thr++) {
          int t = (tid + thr) % g_nthreads;
          memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)),
                 (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len)) , copy_len);
          //memcpy((void*)((uintptr_t)comm->rbuf[t] + (tid * copy_len)),
          //       (void*)((uintptr_t)sendbuf + (t * copy_len)) , copy_len);
      }
  } else {
      half_len = copy_len >> 1;

      //Double-copy path

      for(int t = tid + 1; t < g_nthreads; t++) {
          memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)),
                 (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len)), half_len);
      //}

      //for(int t = tid + 1; t < g_nthreads; t++) {
          memcpy((void*)((uintptr_t)comm->rbuf[t] + (tid * copy_len)),
                 (void*)((uintptr_t)sendbuf + (t * copy_len)), half_len);
      }

      for(int t = 0; t < tid; t++) {
          //Also need to memcpy the other way!
          memcpy((void*)((uintptr_t)recvbuf + (t * copy_len)+half_len),
                 (void*)((uintptr_t)comm->sbuf[t] + (tid * copy_len) + half_len),
                 copy_len - half_len);
      //}

      //for(int t = 0; t < tid; t++) {
          memcpy((void*)((uintptr_t)comm->rbuf[t] + (tid * copy_len)+half_len),
                 (void*)((uintptr_t)sendbuf + (t * copy_len) + half_len),
                 copy_len - half_len);
      }


  }
#endif

  //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
  barrier(&comm->barr, tid);
  return MPI_SUCCESS;
}
#endif


//Can I sync smarter?
//Two threads join up and swap data -- combine the sync for the two transfers.
//each thread sets send/recv buffer, when those are set, we know to go

int HMPI_Alltoall_local2(void* sendbuf, void* recvbuf, size_t copy_len, HMPI_Comm comm)
{
    int comm_size = g_size * g_nthreads;
    uintptr_t sbuf = (uintptr_t)sendbuf;
    uintptr_t rbuf = (uintptr_t)recvbuf;
    int tid = g_tl_tid;
    int rank = g_nthreads*g_rank + tid;

    //Dumb alltoall -- send to everybody, then receive.
    //HMPI_Request* sreqs = (HMPI_Request*)malloc(sizeof(MPI_Request) * comm_size);
    HMPI_Request* sreqs = (HMPI_Request*)alloca(sizeof(MPI_Request) * comm_size);

    //Do the self copy
    memcpy((void*)(rbuf + (tid * copy_len)),
           (void*)(sbuf + (tid * copy_len)), copy_len);

    //Post sends
    for(int i = 1; i < g_size; i++) {
        int r = (rank + i) % g_size;
        HMPI_Isend((void*)(sbuf + copy_len * r), copy_len, MPI_BYTE, r, 4317194,
                HMPI_COMM_WORLD, &sreqs[i]);
    }

    //Do blocking receives
    for(int i = 1; i < g_size; i++) {
        int r = (rank + g_size - i) % g_size;
        HMPI_Recv((void*)(rbuf + copy_len * r), copy_len, MPI_BYTE, r, 4317194,
                HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
        HMPI_Wait(&sreqs[i], MPI_STATUS_IGNORE);
    }

    //Complete sends
    //for(int i = 1; i < g_size; i++) {
        //HMPI_Wait(&sreqs[i], MPI_STATUS_IGNORE);
   // }

    //free(sreqs);
}



#if 0
int HMPI_Alltoall_local2(void* sendbuf, void* recvbuf, size_t copy_len, HMPI_Comm comm)
{
    int thr, i;
    int tid = g_tl_tid;

    //Do the self copy
    memcpy((void*)((uintptr_t)recvbuf + (tid * copy_len)),
           (void*)((uintptr_t)sendbuf + (tid * copy_len)), copy_len);

    //Need to avoid deadlocking here.
    //Have a priority -- lower tid sends first, then higher tid

    for(int t = 1; t < g_nthreads; t++) {
        //Send to tid - t, recv from tid + t
        int send_tid = (tid + g_nthreads - t) % g_nthreads;
        int recv_tid = (tid + t) % g_nthreads;

        int send_len = copy_len;
        int recv_len = copy_len;
        char* sendptr = (char*)((uintptr_t)sendbuf + (send_tid * copy_len));
        char* recvptr = (char*)((uintptr_t)recvbuf + (recv_tid * copy_len));
        //printf("%d send tid %d recv tid %d\n", tid, send_tid, recv_tid);
        //fflush(stdout);

        //Index is [sender][receiver]
        buffer_t* send_buf = &g_buffers[tid][recv_tid];
        buffer_t* recv_buf = &g_buffers[send_tid][tid];

        //printf("%d sendbuf %p head %llu tail %llu\n", tid, send_buf, send_buf->head, send_buf->tail);
        //printf("%d recvbuf %p head %llu tail %llu\n", tid, recv_buf, recv_buf->head, recv_buf->tail);
        //fflush(stdout);

        while(send_len > 0 || recv_len > 0) {
            int head = send_buf->head;

            // Send until we have to wait for a free block
            while(send_len > 0 && head - send_buf->tail < NUM_BLOCKS) {
                int len = (send_len < BLOCK_SIZE ? send_len : BLOCK_SIZE);

                memcpy(&send_buf->data[(head % NUM_BLOCKS) * BLOCK_SIZE],
                        sendptr, len);

                //TODO - maybe use sfence and non-atomic add
                head = __sync_fetch_and_add(&send_buf->head, 1) + 1;

                send_len -= len;
                sendptr += len;
            }


            int tail = recv_buf->tail;

            // Recv until we have to wait for a free block
            while(recv_len > 0 && tail < recv_buf->head) {
                int len = (recv_len < BLOCK_SIZE ? recv_len : BLOCK_SIZE);

                memcpy(recvptr,
                       &recv_buf->data[(tail % NUM_BLOCKS) * BLOCK_SIZE], len);

                //TODO - maybe use sfence and non-atomic add
                tail = __sync_fetch_and_add(&recv_buf->tail, 1) + 1;

                recv_len -= len;
                recvptr += len;
            }
        }
    }

    return MPI_SUCCESS;
}
#endif


