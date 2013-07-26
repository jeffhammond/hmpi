/* Copyright (c) 2010-2013 The Trustees of Indiana University.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * - Neither the Indiana University nor the names of its contributors may be
 *   used to endorse or promote products derived from this software without
 *   specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef _HMPI_H_
#define _HMPI_H_
#include <stdint.h>
#include <mpi.h>
#include <assert.h>
#include "barrier.h"
#include "lock.h"
#include "libpsm.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int g_nthreads;
extern int g_rank;

typedef struct {
  int32_t val;
  int32_t padding[15]; 
} cache_line_t;


//Placeholder typedef - groups aren't implemented yet
typedef void* HMPI_Group;

typedef struct {
  //Used for intra-node sharing in various collectives
  volatile void** sbuf;
  volatile int* scount;
  volatile MPI_Datatype* stype;
  volatile void** rbuf;
  volatile int* rcount;
  volatile MPI_Datatype* rtype;
  volatile void* mpi_sbuf; //Used by alltoall
  volatile void* mpi_rbuf; //Used by alltoall, gatherv

  barrier_t barr;       //Barrier for local ranks in this comm
  MPI_Comm mpicomm;     //Underyling MPI comm
  //MPI_Comm* tcomms;
} HMPI_Comm_info;

typedef HMPI_Comm_info* HMPI_Comm;

extern HMPI_Comm HMPI_COMM_WORLD;

#define HMPI_STATUS_IGNORE NULL
#define HMPI_STATUSES_IGNORE NULL

typedef struct HMPI_Status {
    size_t size; //Message size in bytes
    int MPI_SOURCE;
    int MPI_TAG;
    int MPI_ERROR;
} HMPI_Status;


//HMPI request types
#define HMPI_SEND 1
#define HMPI_RECV 2
#define MPI_SEND 3
#define MPI_RECV 4
#define HMPI_RECV_ANY_SOURCE 5

//HMPI request states
//ACTIVE and COMPLETE specifically chosen to match MPI test flags
#define HMPI_REQ_ACTIVE 0
#define HMPI_REQ_COMPLETE 1


typedef struct HMPI_Item {
    struct HMPI_Item* next;
//    struct HMPI_Item* prev;
} HMPI_Item;


//HMPI_Request is later defined as a pointer to this struct.
typedef struct HMPI_Request_info {
    HMPI_Item item; //Linked list subtype

    int type;       //Request type
    int proc;       //Always the source's rank regardless of type.
    int tag;        //MPI tag
    MPI_Datatype datatype;  //MPI datatype
    size_t size;    //Message size in bytes

    void* buf;      //User buffer
    struct HMPI_Request_info* match_req; //Set only sends; matching recv req

    volatile ssize_t offset;  //Copy offset for shared sender/recver copying
    lock_t match;             //Synchornization for sender/recver copying
    volatile uint8_t stat;    //Request state

    //MPI_Request req;  //Used only for off-node messages via underlying MPI
    libpsm_req_t req;
} HMPI_Request_info;

typedef HMPI_Request_info* HMPI_Request;

#define HMPI_REQUEST_NULL NULL



int HMPI_Init(int *argc, char ***argv, int nthreads, int (*start_routine)(int argc, char** argv));

int HMPI_Finalize();

int HMPI_Abort( HMPI_Comm comm, int errorcode );

int HMPI_Comm_rank ( HMPI_Comm comm, int *rank );
int HMPI_Comm_size ( HMPI_Comm comm, int *size );


//AWF new function -- return true (nonzero) if rank is another thread in the
// same process.
static inline int HMPI_Comm_local(HMPI_Comm comm, int rank)
{
#ifdef HMPI_SAFE
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
 
    return (g_rank == (rank / g_nthreads));  
}


//AWF new function -- return the thread ID of the specified rank.
static inline void HMPI_Comm_thread(HMPI_Comm comm, int rank, int* tid)
{
#ifdef HMPI_SAFE
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif

  *tid = rank % g_nthreads;
}


int HMPI_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm );
int HMPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status );

int HMPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *req );
int HMPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *req );

int HMPI_Iprobe(int source, int tag, HMPI_Comm comm, int* flag, HMPI_Status* status);
int HMPI_Probe(int source, int tag, HMPI_Comm comm, HMPI_Status* status);

int HMPI_Test(HMPI_Request *request, int *flag, HMPI_Status *status);
int HMPI_Testall(int count, HMPI_Request *requests, int* flag, HMPI_Status *statuses);
int HMPI_Wait(HMPI_Request *request, HMPI_Status *status);
int HMPI_Waitall(int count, HMPI_Request* requests, HMPI_Status* statuses);
int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status);

int HMPI_Get_count(HMPI_Status* status, MPI_Datatype datatype, int* count);

//
// Collectives
//

// AWF new function - barrier only among local threads
void HMPI_Barrier_local(HMPI_Comm comm);

int HMPI_Barrier(HMPI_Comm comm);

int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm);

int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm);

int HMPI_Scan(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm);

int HMPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, HMPI_Comm comm);

int HMPI_Scatter(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm);

int HMPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm);

int HMPI_Gatherv(void* sendbuf, int sendcnt, MPI_Datatype sendtype, void* recvbuf, int* recvcnts, int* displs, MPI_Datatype recvtype, int root, HMPI_Comm comm);

int HMPI_Allgather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm);

int HMPI_Allgatherv(void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int *recvcounts, int *displs, MPI_Datatype recvtype, HMPI_Comm comm);

int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm);

//Assumes all ranks are local.
int HMPI_Alltoall_local(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm);


//TODO NOT IMPLEMENTED YET
// Added to catch apps that call these routines.

static int HMPI_Comm_create(HMPI_Comm comm, MPI_Group group, HMPI_Comm* newcomm)
{
    assert(0);
    return MPI_SUCCESS;
}

static int HMPI_Comm_group(HMPI_Comm comm, HMPI_Group* group)
{
    *group = NULL;
    return MPI_SUCCESS;    
}


//Defines to redirect MPI calls to HMPI.
//Set HMPI_INTERNAL to turn this off.
#ifndef HMPI_INTERNAL

#define MPI_Comm HMPI_Comm

#ifdef MPI_COMM_WORLD
#undef MPI_COMM_WORLD
#endif

#define MPI_COMM_WORLD HMPI_COMM_WORLD

#ifdef MPI_REQUEST_NULL
#undef MPI_REQUEST_NULL
#endif

#ifdef MPI_STATUS_IGNORE
#undef MPI_STATUS_IGNORE
#endif

#ifdef MPI_STATUSES_IGNORE
#undef MPI_STATUSES_IGNORE
#endif

#define MPI_REQUEST_NULL HMPI_REQUEST_NULL

#define MPI_STATUS_IGNORE HMPI_STATUS_IGNORE
#define MPI_STATUSES_IGNORE HMPI_STATUSES_IGNORE

#define MPI_Status HMPI_Status

#define MPI_Request HMPI_Request


#define MPI_Init HMPI_Init

#define MPI_Comm_rank HMPI_Comm_rank
#define MPI_Comm_size HMPI_Comm_size

//These are HMPI specific routines, we define for consistency
#define MPI_Comm_local HMPI_Comm_local
#define MPI_Comm_thread HMPI_Comm_thread
#define MPI_Barrier_local HMPI_Barrier_local

#define MPI_Send HMPI_Send
#define MPI_Recv HMPI_Recv

#define MPI_Isend HMPI_Isend
#define MPI_Irecv HMPI_Irecv

#define MPI_Iprobe HMPI_Iprobe
#define MPI_Probe HMPI_Probe

#define MPI_Test HMPI_Test
#define MPI_Testall HMPI_Testall

#define MPI_Wait HMPI_Wait
#define MPI_Waitall HMPI_Waitall
#define MPI_Waitany HMPI_Waitany

#define MPI_Get_count HMPI_Get_count

#define MPI_Barrier HMPI_Barrier
#define MPI_Reduce HMPI_Reduce
#define MPI_Allreduce HMPI_Allreduce
#define MPI_Scan HMPI_Scan
#define MPI_Bcast HMPI_Bcast
#define MPI_Scatter HMPI_Scatter
#define MPI_Gather HMPI_Gather
#define MPI_Gatherv HMPI_Gatherv
#define MPI_Allgather HMPI_Allgather
#define MPI_Allgatherv HMPI_Allgatherv
#define MPI_Alltoall HMPI_Alltoall

//These are HMPI specific routines, we define for consistency
#define MPI_Alltoall_local HMPI_Alltoall_local
#define MPI_Alltoall_local2 HMPI_Alltoall_local2

#define MPI_Abort HMPI_Abort
#define MPI_Finalize HMPI_Finalize


//TODO NOT IMPLEMENTED YET
// Added to catch apps that call these routines.
#define MPI_Comm_create HMPI_Comm_create
#define MPI_Comm_group HMPI_Comm_group

#endif //HMPI_INTERNAL

#ifdef __cplusplus
}
#endif
#endif

