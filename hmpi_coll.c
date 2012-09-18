#define _GNU_SOURCE

//Deal with annoying MPICH/MVAPICH
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

//#include <sched.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "lock.h"
#ifdef ENABLE_PSM
#include "libpsm.h"
#endif


#ifdef FULL_PROFILE
#define FULL_PROFILE_VAR(v) PROFILE_VAR(v)
#define FULL_PROFILE_EXTERN(v) PROFILE_EXTERN(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v) PROFILE_SHOW_REDUCE(v)
#else
#define FULL_PROFILE_VAR(v)
#define FULL_PROFILE_EXTERN(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v)
#endif

FULL_PROFILE_EXTERN(MPI_Other);
FULL_PROFILE_EXTERN(MPI_Barrier);
FULL_PROFILE_EXTERN(MPI_Reduce);
FULL_PROFILE_EXTERN(MPI_Allreduce);
FULL_PROFILE_EXTERN(MPI_Scan);
FULL_PROFILE_EXTERN(MPI_Bcast);
FULL_PROFILE_EXTERN(MPI_Scatter);
FULL_PROFILE_EXTERN(MPI_Gather);
FULL_PROFILE_EXTERN(MPI_Gatherv);
FULL_PROFILE_EXTERN(MPI_Allgather);
FULL_PROFILE_EXTERN(MPI_Allgatherv);
FULL_PROFILE_EXTERN(MPI_Alltoall);


extern __thread int g_tl_tid;       //HMPI node-local rank for this thread (tid)


//Callback used in barriers to cause MPI library progress while waiting
#ifdef ENABLE_PSM
//Just call PSM poll directly, no need for a wrapper.
#define barrier_iprobe poll
#elif defined(ENABLE_MPI)
static void barrier_iprobe(void)
{
    int flag;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
}
#else
//No off-node support, nothing to poll.
static void barrier_iprobe(void)
{
}
#endif


//
// Collectives
//


// AWF new function - barrier only among local threads
void HMPI_Barrier_local(HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Barrier);

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    FULL_PROFILE_STOP(MPI_Barrier);
    FULL_PROFILE_START(MPI_Other);
}


int HMPI_Barrier(HMPI_Comm comm) {
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Barrier);

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    if(g_size > 1) {
        // all root-threads perform MPI_Barrier 
        if(g_tl_tid == 0) {
            MPI_Barrier(comm->mpicomm);
        }

        barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
    }

    FULL_PROFILE_STOP(MPI_Barrier);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


// declaration
//extern "C" {
int NBC_Operation(void *buf3, void *buf1, void *buf2, MPI_Op op, MPI_Datatype type, int count);
//}

int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Reduce);
    int size;
    int i;

    HMPI_Type_size(datatype, &size);

#ifdef DEBUG
    if(g_tl_tid == 0) {
    printf("[%i %i] HMPI_Reduce(%p, %p, %i, %p, %p, %d, %p)\n", g_hmpi_rank, g_tl_tid, sendbuf, recvbuf, count, datatype, op, root, comm);
    fflush(stdout);
    }
#endif

    if(g_tl_tid == root % g_nthreads) {
        //The true root uses its recv buf; others alloc a temp buf.
        void* localbuf;
        if(g_hmpi_rank == root) {
            localbuf = recvbuf;
        } else {
            localbuf = memalign(4096, size * count);
        }

        //TODO eliminate this memcpy by folding into a reduce call?
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

    FULL_PROFILE_STOP(MPI_Reduce);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

#ifndef __bg__
int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Allreduce);
    int size;

    MPI_Type_size(datatype, &size);

    //Do an MCS-style insertion into the coll list.
    hmpi_coll_t coll;

    coll.buf = sendbuf;
    coll.next = NULL;

    hmpi_coll_t* pred = (hmpi_coll_t*)FETCH_STORE((void**)&comm->coll, &coll);

    //Was there a predecessor?  If not, we're the root.
    if(pred == NULL) {
        //printf("%d is root recvbuf %p\n", g_hmpi_rank, recvbuf); fflush(stdout);
        //Wait for successors to show up; reduce each one as it arrives.
        int nthreads = g_nthreads;

        comm->rbuf[0] = recvbuf;

        //TODO eliminate this memcpy by folding into a reduce call?
        memcpy(recvbuf, sendbuf, size * count);

        hmpi_coll_t* volatile * vol_next;
        hmpi_coll_t* cur = &coll;

        for(int i = 1; i < nthreads; i++) {
            //AWF - the way this type is declared is CRITICAL for correctness!!!
            vol_next = (hmpi_coll_t* volatile *)&cur->next;

            while((cur = *vol_next) == NULL);

            NBC_Operation(recvbuf, recvbuf,
                    (void*)cur->buf, op, datatype, count);
        }

        if(g_size > 1) {
            MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->mpicomm);
        }

        //Signal everyone else that they can copy out.
        //TODO - can something smarter be done?
        barrier(&comm->barr, g_tl_tid);

        //Clear the list to NULL for the next allreduce
        comm->coll = NULL;
        //STORE_FENCE();
    } else {
        //volatile int* locked = &coll.locked;

        //STORE_FENCE();

        //Make sure the root can get to us.
        pred->next = &coll;
        STORE_FENCE();

        //Wait for the root to do its thing.
        barrier(&comm->barr, g_tl_tid);
        //printf("%d is child src recv buf %p\n", g_hmpi_rank, comm->rbuf[0]); fflush(stdout);

        memcpy(recvbuf, (void*)comm->rbuf[0], count*size);
    }

    barrier(&comm->barr, g_tl_tid);

    FULL_PROFILE_STOP(MPI_Allreduce);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

#else
int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Allreduce);
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

    //Set my buffer and flag.
    //go around the ranks starting above me:
    // wait for their flag
    // reduce into my result

    // Do the MPI allreduce, then each rank can copy out.
    if(g_tl_tid == 0) {
        comm->rbuf[0] = recvbuf;

        barrier(&comm->barr, g_tl_tid);
        //barrier_cb(&comm->barr, 0, barrier_iprobe);

        //TODO eliminate this memcpy by folding into a reduce call?
        memcpy(recvbuf, sendbuf, size * count);

        for(i=1; i<g_nthreads; ++i) {
            NBC_Operation(recvbuf, recvbuf,
                    (void*)comm->sbuf[i], op, datatype, count);
        }

        //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
        barrier(&comm->barr, 0);
    } else {
        //Put up our send buffer for the root thread to reduce from.
        comm->sbuf[g_tl_tid] = sendbuf;
        //barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);
        barrier(&comm->barr, g_tl_tid);

        //Wait for the root rank to do its thing.
        barrier(&comm->barr, g_tl_tid);

        //Copy reduced data to our own buffer.
        memcpy(recvbuf, (void*)comm->rbuf[0], count*size);
    }

    //Potential optimization -- 0 can't leave until all threads arrive.. all
    //others can go
    barrier(&comm->barr, g_tl_tid);
    FULL_PROFILE_STOP(MPI_Allreduce);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

#endif

#define HMPI_SCAN_TAG 7546348

int HMPI_Scan(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Scan);
    //MPI_Aint extent, lb;
    MPI_Request req;
    int size;
    int i;

    HMPI_Type_size(datatype, &size);
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

    //Thread 0 has no more reduce work to do.
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
   
    FULL_PROFILE_STOP(MPI_Scan);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, HMPI_Comm comm) {
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Bcast);
    //MPI_Aint extent, lb;
    int size;

    HMPI_Type_size(datatype, &size);
    //MPI_Type_get_extent(datatype, &lb, &extent);
    //MPI_Type_extent(datatype, &extent);

#if 0
#ifdef HMPI_SAFE
    if(extent != size) {
        printf("bcast non-contiguous derived datatypes are not supported yet!\n");
        MPI_Abort(comm->mpicomm, 0);
    }
#endif
#endif
  
#ifdef DEBUG
    printf("[%i] HMPI_Bcast(%x, %i, %x, %i, %x)\n", g_rank*g_nthreads+g_tl_tid, buffer, count, datatype, root, comm);
#endif

    int local_root = root % g_nthreads;

    //Root sets the send buffer
    if(local_root == g_tl_tid) {
        comm->sbuf[0] = buffer;
    }

    barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

    //Only do an MPI-level bcast and barrier if more the one node.
    if(g_size > 1) {
        if(g_tl_tid == local_root) {
            MPI_Bcast((void*)comm->sbuf[0],
                    count, datatype, root, comm->mpicomm);
        }

        barrier(&comm->barr, g_tl_tid);
    }

    //All ranks other than the root copy the recv buffer.
    if(local_root != g_tl_tid) {
        memcpy(buffer, (void*)comm->sbuf[0], count*size);
    }

    barrier(&comm->barr, g_tl_tid);
    FULL_PROFILE_STOP(MPI_Bcast);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


// TODO - scatter and gather may not work right for count > 1

int HMPI_Scatter(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm) {
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Scatter);
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  HMPI_Type_size(sendtype, &send_size);
  HMPI_Type_size(recvtype, &recv_size);
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
    //Grab a temp recv buf and make it available to other threads.
    comm->rbuf[0] = memalign(4096, size * g_nthreads);

    if(root / g_nthreads != g_rank) {
        //root is not on this node, set the send type to something
        comm->sbuf[0] = NULL;
        comm->scount[0] = recvcount;
        comm->stype[0] = recvtype;
    }
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  //Just do a scatter!
  if(g_tl_tid == 0) {
    MPI_Scatter((void*)comm->sbuf[0], (int)comm->scount[0] * g_nthreads,
            (MPI_Datatype)comm->stype[0], (void*)comm->rbuf[0],
            recvcount * g_nthreads, recvtype, root / g_nthreads, comm->mpicomm);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  //Each thread copies out of the root buffer
  if(recvbuf == MPI_IN_PLACE) {
    memcpy(sendbuf, (void*)((uintptr_t)comm->rbuf[0] + size * g_tl_tid), size);
  } else {
    memcpy(recvbuf, (void*)((uintptr_t)comm->rbuf[0] + size * g_tl_tid), size);
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  if(g_tl_tid == 0) {
      free((void*)comm->rbuf[0]);
  }

    FULL_PROFILE_STOP(MPI_Scatter);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}


// TODO - scatter and gather may not work right for count > 1

int HMPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Gather);
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  HMPI_Type_size(sendtype, &send_size);
  HMPI_Type_size(recvtype, &recv_size);
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

  //Each thread copies into the send buffer
  memcpy((void*)((uintptr_t)comm->sbuf[0] + size * g_tl_tid), sendbuf, size);

  barrier(&comm->barr, g_tl_tid);

  if(g_tl_tid == 0) {
    MPI_Gather((void*)comm->sbuf[0], sendcount * g_nthreads,
            sendtype, (void*)comm->rbuf[0],
            recvcount * g_nthreads, recvtype, root / g_nthreads, comm->mpicomm);
    free((void*)comm->sbuf[0]);
  }

  barrier(&comm->barr, g_tl_tid);
    FULL_PROFILE_STOP(MPI_Gather);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}


#define HMPI_GATHERV_TAG 76361347
int HMPI_Gatherv(void* sendbuf, int sendcnt, MPI_Datatype sendtype, void* recvbuf, int* recvcnts, int* displs, MPI_Datatype recvtype, int root, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Gatherv);
    //Each sender can have a different send count.
    //recvcnts[i] must be equal to rank i's sendcnt.
    //Only sendbuf, sendcnt, sendtype, root, comm meaningful on senders

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

    //Everybody posts their send info
    int tid = g_tl_tid;
    comm->sbuf[tid] = sendbuf;
    comm->scount[tid] = sendcnt;
    comm->stype[tid] = sendtype;
    comm->rbuf[tid] = recvbuf;
    comm->mpi_rbuf = displs;

    barrier_cb(&comm->barr, tid, barrier_iprobe);

    //One rank on each node builds the dtypes and does the MPI-level gatherv
    //using sends/receives.
    if(tid == root % g_nthreads) {
        //One root rank on each node.
        if(root == g_hmpi_rank) {
            //I am root; create recv dtype.
            int rank = g_rank;
            MPI_Datatype* dtrecvs = (MPI_Datatype*)alloca(sizeof(MPI_Datatype) * g_size);
            MPI_Request* reqs = (MPI_Request*)alloca(sizeof(MPI_Request) * g_size);

            for(int i = 0; i < g_size; i++) {
                if(i == rank) {
                    //We do local copies in the root rank's node.
                    dtrecvs[i] = MPI_DATATYPE_NULL;
                    reqs[i] = MPI_REQUEST_NULL;
                    continue;
                }

                //Build a dtype to receive from this node -- combine the
                //displacements we expect from each rank in that node into one
                //dtype.
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
            MPI_Datatype dtsend;

            //I have g_nthreads blocks, with their own buf, cnt, type.
            MPI_Type_create_struct(g_nthreads, (int*)comm->scount, (MPI_Aint*)comm->sbuf, (MPI_Datatype*)comm->stype, &dtsend);
            MPI_Type_commit(&dtsend);

            MPI_Send(MPI_BOTTOM, 1, dtsend, root / g_nthreads, HMPI_GATHERV_TAG, comm->mpicomm);

            //MPI_Gatherv(MPI_BOTTOM, 1, dtsend, NULL, NULL, MPI_DATATYPE_NULL,
            //        root, comm->mpicomm);
            MPI_Type_free(&dtsend);
        }

    } else if(HMPI_Comm_local(comm, root)) {
        //Meanwhile, all local non-root ranks do memcpys.
        int root_tid;
        HMPI_Comm_thread(comm, root, &root_tid);

        int* displs = (int*)comm->mpi_rbuf;
        void* buf = (void*)comm->rbuf[root_tid];
        int size;

        HMPI_Type_size(sendtype, &size);

        //Copy data from my sendbuf to the root.
        memcpy((void*)((uintptr_t)buf + displs[g_hmpi_rank]),
                sendbuf, size * sendcnt);
    }

    barrier(&comm->barr, tid);
    FULL_PROFILE_STOP(MPI_Gatherv);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Allgather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Allgather);
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  HMPI_Type_size(sendtype, &send_size);
  HMPI_Type_size(recvtype, &recv_size);

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
      //Use this node's spot in tid 0's recvbuf, as the send buffer.
      comm->sbuf[0] =
          (void*)((uintptr_t)recvbuf + (size * g_nthreads * g_rank));

      comm->rbuf[0] = recvbuf;
      //comm->rcount[0] = recvcount;
      //comm->rtype[0] = recvtype;
  }

  barrier_cb(&comm->barr, g_tl_tid, barrier_iprobe);

  //Each thread copies into the send buffer
  memcpy((void*)((uintptr_t)comm->sbuf[0] + size * g_tl_tid), sendbuf, size);

  barrier(&comm->barr, g_tl_tid);

  if(g_size > 1) {
    //Do the MPI-level inter-node gather.
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

    FULL_PROFILE_STOP(MPI_Allgather);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}


int HMPI_Allgatherv(void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int* recvcounts, int *displs, MPI_Datatype recvtype, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Allgatherv);
  MPI_Aint send_extent, recv_extent, lb;
  int send_size;
  int recv_size;
  int size;

  HMPI_Type_size(sendtype, &send_size);
  HMPI_Type_size(recvtype, &recv_size);

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
      //Use this node's spot in tid 0's recvbuf, as the send buffer.
      //Have to use the displacements to get the right spot.
      comm->sbuf[0] = recvbuf;
          //(void*)((uintptr_t)recvbuf + (size * displs[g_hmpi_rank]));

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
        MPI_Datatype dtype;
        MPI_Datatype* basetype;
        int i;

        //Do a series of bcasts, rotating the root to each node.
        //Have to build a dtype for each node to cover its unique displs.
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
      //Ugh, have to do one memcpy per rank.
      int i;

      //TODO - copy from sendbuf for self rank, not recvbuf
      // Wouldn't have to wait, and maybe beter locality.
      for(i = 0; i < g_size * g_nthreads; i++) {
        int offset = displs[i] * send_size;
        memcpy((void*)((uintptr_t)recvbuf + offset),
                (void*)((uintptr_t)comm->rbuf[0] + offset), recvcounts[i] * recv_size);
      }
  }

  barrier(&comm->barr, g_tl_tid);

    FULL_PROFILE_STOP(MPI_Allgatherv);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}


//TODO - the proper thing would be to have our own internal MPI comm for colls
#define HMPI_ALLTOALL_TAG 7546347

#ifdef ENABLE_MPI
int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Alltoall);
  //void* rbuf;
  int32_t send_size;
  uint64_t size;
  MPI_Request* send_reqs = NULL;
  MPI_Request* recv_reqs = NULL;
  MPI_Datatype dt_send;
  MPI_Datatype dt_recv;

  HMPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
  int32_t recv_size;
  MPI_Aint send_extent, recv_extent, lb;

  HMPI_Type_size(recvtype, &recv_size);

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

  uint64_t comm_size = g_nthreads * g_size;
  uint64_t data_size = send_size * sendcount;

  comm->sbuf[g_tl_tid] = sendbuf;

  if(g_tl_tid == 0) {
      //Alloc temp send/recv buffers
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

      //Post receives from each other rank, except self.
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

    FULL_PROFILE_STOP(MPI_Alltoall);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}

#elif defined(ENABLE_PSM)
#warning "PSM alltoall"

int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Alltoall);
    int32_t send_size;
    uint64_t size;
  int rank = g_rank;
  int nthreads = g_nthreads;
  int tid = g_tl_tid;
  libpsm_req_t* send_reqs = NULL;
  libpsm_req_t* recv_reqs = NULL;

  HMPI_Type_size(sendtype, &send_size);

  uint64_t comm_size = nthreads * g_size;
  uint64_t data_size = send_size * sendcount;

  comm->sbuf[tid] = sendbuf;

  if(tid == 0) {
      //Alloc temp send/recv buffers
      comm->mpi_sbuf = memalign(4096, data_size * nthreads * comm_size);
      comm->mpi_rbuf = memalign(4096, data_size * nthreads * comm_size);

      send_reqs = (libpsm_req_t*)alloca(sizeof(libpsm_req_t) * g_size);
      recv_reqs = (libpsm_req_t*)alloca(sizeof(libpsm_req_t) * g_size);

      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have nthreads per process
      // though, so we multiply by that.

      //Post receives from each other rank, except self.
      int len = data_size * nthreads * nthreads;
      for(int i = 0; i < g_size; i++) {
          if(i != rank) {
              post_recv((void*)((uintptr_t)comm->mpi_rbuf + (len * i)),
                      len,
                      BUILD_TAG(i * nthreads, 0, HMPI_ALLTOALL_TAG),
                      TAGSEL_P2P, i, &recv_reqs[i]);
          }
      }
      recv_reqs[rank] = NULL;
  }

  barrier_cb(&comm->barr, tid, barrier_iprobe);

  //Copy into the shared send buffer on a stride by nthreads
  //This way our temp buffer has all the data going to proc 0, then proc 1, etc
  uintptr_t offset = tid * data_size;
  uintptr_t scale = data_size * nthreads;

  //Verified from (now missing) prints, this is correct
  // Data is pushed here -- remote thread can't read it
  for(uintptr_t i = 0; i < comm_size; i++) {
      if(!HMPI_Comm_local(comm, i)) {
          //Copy to send buffer to go out over network
          memcpy((void*)((uintptr_t)(comm->mpi_sbuf) + (scale * i) + offset),
                  (void*)((uintptr_t)sendbuf + data_size * i), data_size);
      }
  }

  //Start sends to each other rank
  barrier_cb(&comm->barr, tid, barrier_iprobe);

  if(tid == 0) {
      int len = data_size * nthreads * nthreads;
      for(int i = 1; i < g_size; i++) {
          int r = (rank + i) % g_size;
          if(r != rank) {
              //MPI_Isend((void*)((uintptr_t)comm->mpi_sbuf + (len * r)), 1,
              //        dt_send, r, HMPI_ALLTOALL_TAG, comm->mpicomm, &send_reqs[r]);
              post_send((void*)((uintptr_t)comm->mpi_sbuf + (len * r)),
                      data_size * nthreads * nthreads,
                      BUILD_TAG(g_hmpi_rank, 0, HMPI_ALLTOALL_TAG),
                      r, &send_reqs[r]);
          }
      }

      send_reqs[rank] = NULL;
  }

  //Pull local data from other threads' send buffers.
  //For each thread, memcpy from their send buffer into my receive buffer.
  int r = rank * nthreads; //Base rank
  for(uintptr_t thr = 0; thr < nthreads; thr++) {
      //Note careful use of addition by r to get the right offsets
      int t = (tid + thr) % nthreads;
      memcpy((void*)((uintptr_t)recvbuf + ((r + t) * data_size)),
             (void*)((uintptr_t)comm->sbuf[t] + ((r + tid) * data_size)),
             data_size);
  }

  //Wait on sends and receives to complete
  if(tid == 0) {
    int not_done;
    do {
        not_done = 0;
        poll();

        for(int i = 0; i < g_size; i++) {
            if(recv_reqs[i] != NULL) {
                not_done |= !test(&recv_reqs[i], NULL);
            }

            if(send_reqs[i] != NULL) {
                not_done |= !test(&send_reqs[i], NULL);
            }
        }
    } while(not_done);
  }

  barrier_cb(&comm->barr, tid, barrier_iprobe);

  //Need to do g_size memcpy's -- one block of data per MPI process.
  // We copy nthreads * data_size at a time.
  offset = tid * data_size * nthreads;
  scale = data_size * nthreads * nthreads;
  size = nthreads * data_size;

  for(uint64_t i = 0; i < g_size; i++) {
      if(i != rank) {
          memcpy((void*)((uintptr_t)recvbuf + size * i),
                  (void*)((uintptr_t)comm->mpi_rbuf + (scale * i) + offset),
                  size);
      }
  }

  barrier_cb(&comm->barr, tid, barrier_iprobe);

  if(tid == 0) {
      free((void*)comm->mpi_sbuf);
      free((void*)comm->mpi_rbuf);
  }

    FULL_PROFILE_STOP(MPI_Alltoall);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}

#if 0
//Write a new alltoall that memcpy's into one buffer then sends to all nodes.
// Use an MCS entry like I did in allreduce to skip the initial barrier.
int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Alltoall);
    int32_t send_size;
    int thr;
    int tid = g_tl_tid;
    int nthreads = g_nthreads;
    int rank = g_rank;
    int size = g_size;
    int hmpi_rank = g_hmpi_rank;

    HMPI_Type_size(sendtype, &send_size);

    //TODO - is there a cool way to do this without entry barriers?
    //We have to sync towards the end, since every rank needs data from every
    // other rank.

    //Each rank set its buf and flag when it arrives.
    // Maybe even do it as a linked list like MCS?
    //  Add self to queue -- if queue was empty, act as root and reduce the
    //   others as they arrive.  Then we can do an exit barrier like usual.

    comm->sbuf[tid] = sendbuf;
    //comm->scount[g_tl_tid] = sendcount;
    //comm->stype[g_tl_tid] = sendtype;

    //comm->rbuf[g_tl_tid] = recvbuf;
    //comm->rcount[g_tl_tid] = recvcount;
    //comm->rtype[g_tl_tid] = recvtype;

    int copy_len = send_size * sendcount;

    //Post receives from every non-local rank.
    libpsm_req_t* send_reqs =
            (libpsm_req_t*)alloca(sizeof(libpsm_req_t) * (size - 1) * nthreads);
    libpsm_req_t* recv_reqs =
            (libpsm_req_t*)alloca(sizeof(libpsm_req_t) * (size - 1) * nthreads);

        mcs_qnode_t q;
        MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    for(int i = 0; i < size - 1; i++) {
        int node = (rank + i + 1) % size;
        for(int j = 0; j < nthreads; j++) {
            int r = node * nthreads + j;
            //Post a receive from HMPI rank r
            post_recv_nl((void*)((uintptr_t)recvbuf + (r * copy_len)),
                    copy_len, BUILD_TAG(r, tid, HMPI_ALLTOALL_TAG),
                    TAGSEL_P2P, node, &recv_reqs[i * nthreads + j]);

            //Send to HMPI rank r
            post_send_nl((void*)((uintptr_t)sendbuf + (r * copy_len)),
                    copy_len, BUILD_TAG(hmpi_rank, j, HMPI_ALLTOALL_TAG),
                    node, &send_reqs[i * nthreads + j]);
        }
    }
        MCS_LOCK_RELEASE(&libpsm_lock, &q);

    //Do the self copy
    memcpy((void*)((uintptr_t)recvbuf + (hmpi_rank * copy_len)),
           (void*)((uintptr_t)sendbuf + (hmpi_rank * copy_len)), copy_len);

    if(tid == 0) {
        barrier_cb(&comm->barr, tid, barrier_iprobe);
    } else {
        barrier(&comm->barr, tid);
    }

    for(thr = 1; thr < g_nthreads; thr++) {
        int rt = (tid + thr) % nthreads;    //Index for sbuf
        int lt = rt + (nthreads * rank);    //Offset into recvbuf

        //Copy from everyone else into my recv buf.
        memcpy((void*)((uintptr_t)recvbuf + (lt * copy_len)),
               (void*)((uintptr_t)comm->sbuf[rt] + (hmpi_rank * copy_len)) , copy_len);
        //memcpy((void*)((uintptr_t)comm->rbuf[thr] + (g_tl_tid * copy_len)),
        //       (void*)((uintptr_t)sendbuf + (thr * copy_len)) , copy_len);
    }

    int not_done;
    int flag;
    do {
        not_done = 0;
        if(tid == 0) {
            poll();
        }

        for(int i = 0; i < (size - 1) * nthreads; i++) {
            if(recv_reqs[i] != NULL) {
                not_done |= !test(&recv_reqs[i], NULL);
            }

            if(send_reqs[i] != NULL) {
                not_done |= !test(&send_reqs[i], NULL);
            }
        }
    } while(not_done);

    if(tid == 0) {
        barrier_cb(&comm->barr, tid, barrier_iprobe);
    } else {
        barrier(&comm->barr, tid);
    }

    FULL_PROFILE_STOP(MPI_Alltoall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}
#endif

#else //No multi-node support

//AWF - this version of alltoall assumes only one node, so no need to mess with
//MPI stuff.  Made to be as fast as possible for the local case..
int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Alltoall);
    int32_t send_size;
    int thr;
    int tid = g_tl_tid;

    HMPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
    MPI_Aint send_extent, recv_extent, lb;
    int32_t recv_size;

    HMPI_Type_size(recvtype, &recv_size);

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


    //TODO - is there a cool way to do this without entry barriers?
    //We have to sync towards the end, since every rank needs data from every
    // other rank.

    //Each rank set its buf and flag when it arrives.
    // Maybe even do it as a linked list like MCS?
    //  Add self to queue -- if queue was empty, act as root and reduce the
    //   others as they arrive.  Then we can do an exit barrier like usual.

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
    FULL_PROFILE_STOP(MPI_Alltoall);
    FULL_PROFILE_START(MPI_Other);
  return MPI_SUCCESS;
}

#endif

