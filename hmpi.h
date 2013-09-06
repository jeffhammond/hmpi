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
#include <unistd.h>
#include <mpi.h>
#include <assert.h>
#include "lock.h"
//#include "barrier.h"


#ifdef __cplusplus
extern "C" {
#endif

//Define so users of HMPI can check for HMPI vs MPI
#define _USING_HMPI_ 1


//HMPI internal stuff

//BGQ is crazy!  Inserting nop's in some placed reduces NetPIPE latency.
//Use this macro to insert nop's only on BGQ.
#ifdef __bg__
#define BGQ_NOP __asm__("nop\n")
#else
#define BGQ_NOP while(0)
#endif


//These really are internal, but they are used in publicly viewable structs.
#define EAGER_LIMIT 256
#define PTOP 5

typedef struct HMPI_Item {
    struct HMPI_Item* next;
} HMPI_Item;



#ifdef HMPI_INTERNAL

#define printf(...) printf(__VA_ARGS__); fflush(stdout)

#ifdef __bg__
//PPC A2 issues a single load/store for accesses within a 32-byte aligned block.
#define MALLOC(t, s) (t*)memalign(32, sizeof(t) * s)
#else
//Aligning to 64 bytes seems to cause some weird latency without adding pad
// elements to structs.
//__builtin_assume_aligned is a GCC extension.
//#define MALLOC(t, s) (t*)__builtin_assume_aligned(memalign(8, sizeof(t) * s), 8)
#define MALLOC(t, s) (t*)memalign(8, sizeof(t) * s)
#endif



//Conditional to check if a pointer points to a SM buffer.
//#ifdef __bg__
//Everything is shared on BG/Q
//#define IS_SM_BUF(p) (1)
//#else
extern void* sm_lower;
extern void* sm_upper;

#define IS_SM_BUF(p) ((p) >= sm_lower && (p) < sm_upper)
//#endif


//Wrappers to GCC/ICC extensions

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)


//HMPI request types
#define HMPI_SEND 1
#define HMPI_RECV 2
#define MPI_SEND 4
#define MPI_RECV 8
#define HMPI_RECV_ANY_SOURCE 0x10

//Doesn't hurt to leave these on without OPI enabled.
#define OPI_GIVE 0x20
#define OPI_TAKE 0x40
#define OPI_TAKE_ANY_SOURCE 0x80

//States for internally allocated message buffers.
#define DO_NOT_FREE 0
#define DO_FREE 1
#define DO_OPI_FREE 2


//HMPI request states
//ACTIVE and COMPLETE specifically chosen to match MPI test flags
#define HMPI_REQ_ACTIVE 0
#define HMPI_REQ_COMPLETE 1


typedef struct HMPI_Request_list {
    HMPI_Item head;
    HMPI_Item* tail;
#ifdef __bg__
    char padding[64];
#endif

    lock_t lock;
#ifdef __bg__
    char padding2[64];
#endif
} HMPI_Request_list;

#endif //HMPI_INTERNAL


#if 0
typedef struct {
    volatile int32_t ptopsense;
    int32_t padding[15];
} padptop;
#endif

//Shared memory data used by collectives.
//One of these structs is attached to a communicator.
typedef struct hmpi_coll_t {
    volatile void** sbuf;
    volatile void** rbuf;
    volatile void** tmp;

    volatile void* mpi_sbuf;
    volatile void* mpi_rbuf;
    volatile void* mpi_tmp;

//    padptop* ptop[PTOP];
//    hbarrier_record* t_barr;
} hmpi_coll_t;


//Placeholder typedef - groups aren't implemented yet
typedef void* HMPI_Group;

typedef struct {
  MPI_Comm comm;        //Underyling MPI communicator: MUST BE FIRST
  MPI_Comm node_comm;   //Contains only ranks in this comm on the same node
  MPI_Comm net_comm;    //Contains one rank from each node
  MPI_Comm numa_comm;   //Contains only ranks in this comm on the same NUMA
  int context;          //Communicator context value, valid only in this node.
  int comm_rank;        //Rank in full communicator
  //int comm_size;      //Currently unused
  int node_rank;        //Rank within node_comm
  int node_size;        //Number of ranks on this node
  int node_root;        //Comm rank of first node rank on this node
  //int net_rank;
  //int net_size;

  hmpi_coll_t* coll;

  //This mysteriously improves latency for netpipe.
  //I used to have more variables here; removing them slowed netpipe down.
#ifndef __bg__
  //char pad[60];
#endif
} HMPI_Comm_info;

typedef HMPI_Comm_info* HMPI_Comm;

#define HMPI_COMM_NULL NULL

extern HMPI_Comm HMPI_COMM_WORLD;
extern HMPI_Comm HMPI_COMM_NODE;
//extern HMPI_Comm HMPI_COMM_CACHE;
extern HMPI_Comm HMPI_COMM_NETWORK;


#define HMPI_STATUS_IGNORE NULL
#define HMPI_STATUSES_IGNORE NULL

typedef struct HMPI_Status {
    size_t size; //Message size in bytes
    int MPI_SOURCE;
    int MPI_TAG;
    int MPI_ERROR;
} HMPI_Status;


//HMPI_Request is later defined as a pointer to this struct.
typedef struct HMPI_Request_info {
    union {
        HMPI_Item item; //Linked list subtype
        MPI_Request req; //Used for non-ANY_SRC MPI sends/receives
    } ir;

    size_t size;        //Message size in bytes
    void* buf;          //User buffer

    int proc;           //Always the source's rank regardless of type.
    int tag;            //MPI tag
    int context;        //Communicator context

    volatile uint32_t stat;    //Request state
    uint32_t type;      //Request type
    uint32_t do_free;   //Used for internally-allocated SM regions on send side.

#ifdef HMPI_CHECKSUM
    uint32_t csum;
#endif

    MPI_Datatype datatype;      //MPI datatype
    volatile uint32_t match;    //Synchronization for sender/recver copying

//    volatile uint32_t lock;
    union {
        struct HMPI_Request_info* match_req; //Use on local send req
        volatile size_t offset;              //Copy offset, used on recv req
        MPI_Request req;                     //ANY_SRC receives
    } u;

    char eager[EAGER_LIMIT];
} HMPI_Request_info;

typedef HMPI_Request_info* HMPI_Request;

#define HMPI_REQUEST_NULL NULL


typedef struct HMPI_Info_info {
} HMPI_Info_info;

typedef HMPI_Info_info* HMPI_Info;

#define HMPI_INFO_NULL NULL


int HMPI_Init(int *argc, char ***argv);

int HMPI_Finalize(void);


static int HMPI_Abort(HMPI_Comm comm, int errorcode) __attribute__((unused));

static int HMPI_Abort(HMPI_Comm comm, int errorcode) {
  fprintf(stderr, "HMPI: user code called MPI_Abort!\n");
  return MPI_Abort(comm->comm, errorcode);
}


static inline int HMPI_Comm_rank(HMPI_Comm comm, int *rank) __attribute__((unused));

static inline int HMPI_Comm_rank(HMPI_Comm comm, int *rank) {
  return MPI_Comm_rank(comm->comm, rank);
}


static inline int HMPI_Comm_size(HMPI_Comm comm, int *size) __attribute__((unused));

static inline int HMPI_Comm_size(HMPI_Comm comm, int *size) {
  return MPI_Comm_size(comm->comm, size);
}


//AWF new function -- return the node rank of some rank.
void HMPI_Comm_node_rank(HMPI_Comm comm, int rank, int* node_rank);

int HMPI_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm );
int HMPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status );

int HMPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *req );
int HMPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *req );

int HMPI_Sendrecv(void *sendbuf, int sendcount, MPI_Datatype sendtype, int dest, int sendtag, void *recvbuf, int recvcount, MPI_Datatype recvtype, int source, int recvtag, HMPI_Comm comm, HMPI_Status *status);

int HMPI_Iprobe(int source, int tag, HMPI_Comm comm, int* flag, HMPI_Status* status);
int HMPI_Probe(int source, int tag, HMPI_Comm comm, HMPI_Status* status);

int HMPI_Test(HMPI_Request *request, int *flag, HMPI_Status *status);
int HMPI_Testall(int count, HMPI_Request *requests, int* flag, HMPI_Status *statuses);
int HMPI_Testsome(int incount, HMPI_Request* array_of_requests, int *outcount,
                  int* array_of_indices, HMPI_Status* array_of_statuses);
int HMPI_Wait(HMPI_Request *request, HMPI_Status *status);
int HMPI_Waitall(int count, HMPI_Request* requests, HMPI_Status* statuses);
int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status);

int HMPI_Get_count(HMPI_Status* status, MPI_Datatype datatype, int* count);


//
// Collectives
//

#if 0
// AWF new function - barrier only among local threads
void HMPI_Barrier_local(HMPI_Comm comm);

int HMPI_Barrier(HMPI_Comm comm);
//#define HMPI_Barrier(c) PMPI_Barrier((c)->comm)

int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm);
/*#define HMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, c) \
    PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, (c)->comm)*/

int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm);
/*#define HMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, c) \
    PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, (c)->comm)*/

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

#endif


int HMPI_Cart_create(HMPI_Comm comm_old, int ndims, int* dims, int* periods,
        int reorder, HMPI_Comm* comm_cart);
int HMPI_Cart_sub(HMPI_Comm comm, int* remain_dims, HMPI_Comm* newcomm);

int HMPI_Comm_create(HMPI_Comm comm, MPI_Group group, HMPI_Comm* newcomm);
int HMPI_Comm_dup(HMPI_Comm comm, HMPI_Comm* newcomm);
int HMPI_Comm_free(HMPI_Comm* comm);
int HMPI_Comm_split(HMPI_Comm comm, int color, int key, HMPI_Comm* newcomm);

//TODO NOT IMPLEMENTED YET
// Added to catch apps that call these routines.

static int HMPI_Comm_group(HMPI_Comm comm, HMPI_Group* group) __attribute__((unused));

static int HMPI_Comm_group(HMPI_Comm comm, HMPI_Group* group)
{
    *group = NULL;
    return MPI_SUCCESS;    
}


#ifdef ENABLE_OPI
int OPI_Alloc(void** ptr, size_t length);
int OPI_Free(void** ptr);

int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, HMPI_Comm comm, HMPI_Request* req);

int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, HMPI_Comm comm, HMPI_Request* req);
#endif



//Defines to redirect MPI calls to HMPI.
//Set HMPI_INTERNAL to turn this off.
#ifndef HMPI_INTERNAL

#define MPI_Comm HMPI_Comm

#ifdef MPI_COMM_NULL
#undef MPI_COMM_NULL
#endif

#ifdef MPI_COMM_WORLD
#undef MPI_COMM_WORLD
#endif

#define MPI_COMM_WORLD HMPI_COMM_WORLD

#ifdef MPI_COMM_NODE
#undef MPI_COMM_NODE
#endif

#define MPI_COMM_NODE HMPI_COMM_NODE

#ifdef MPI_COMM_NETWORK
#undef MPI_COMM_NETWORK
#endif

#define MPI_COMM_NETWORK HMPI_COMM_NETWORK

#ifdef MPI_INFO_NULL
#undef MPI_INFO_NULL
#endif

#ifdef MPI_REQUEST_NULL
#undef MPI_REQUEST_NULL
#endif

#ifdef MPI_STATUS_IGNORE
#undef MPI_STATUS_IGNORE
#endif

#ifdef MPI_STATUSES_IGNORE
#undef MPI_STATUSES_IGNORE
#endif

#define MPI_COMM_NULL HMPI_COMM_NULL

#define MPI_INFO_NULL HMPI_INFO_NULL

#define MPI_REQUEST_NULL HMPI_REQUEST_NULL

#define MPI_STATUS_IGNORE HMPI_STATUS_IGNORE
#define MPI_STATUSES_IGNORE HMPI_STATUSES_IGNORE

#define MPI_Status HMPI_Status

#define MPI_Request HMPI_Request


#define MPI_Init HMPI_Init

#define MPI_Comm_rank HMPI_Comm_rank
#define MPI_Comm_size HMPI_Comm_size

#define MPI_Send HMPI_Send
#define MPI_Recv HMPI_Recv

#define MPI_Isend HMPI_Isend
#define MPI_Irecv HMPI_Irecv

#define MPI_Sendrecv HMPI_Sendrecv

#define MPI_Iprobe HMPI_Iprobe
#define MPI_Probe HMPI_Probe

#define MPI_Test HMPI_Test
#define MPI_Testall HMPI_Testall
#define MPI_Testsome HMPI_Testsome

#define MPI_Wait HMPI_Wait
#define MPI_Waitall HMPI_Waitall
#define MPI_Waitany HMPI_Waitany

#define MPI_Get_count HMPI_Get_count

#define MPI_Comm_create HMPI_Comm_create
#define MPI_Comm_dup HMPI_Comm_dup
#define MPI_Comm_free HMPI_Comm_free
#define MPI_Comm_split HMPI_Comm_split

#define MPI_Cart_coords(comm, rank, maxdims, coords) \
    MPI_Cart_coords((comm)->comm, rank, maxdims, coords)

#define MPI_Cart_get(comm, maxdims, dims, periods, coords) \
    MPI_Cart_get((comm)->comm, maxdims, dims, periods, coords)

#define MPI_Cart_map(comm, ndims, dims, periods, newrank) \
    MPI_Cart_map((comm)->comm, ndims, dims, periods, newrank)

#define MPI_Cart_rank(comm, coords, rank) \
    MPI_Cart_rank((comm)->comm, coords, rank)

#define MPI_Cart_shift(comm, direction, disp, rank_source, rank_dest) \
    MPI_Cart_shift((comm)->comm, direction, disp, rank_source, rank_dest)


#define MPI_Barrier(c) MPI_Barrier((c)->comm)

#define MPI_Bcast(buffer, count, datatype, root, c) \
    MPI_Bcast(buffer, count, datatype, root, (c)->comm)

#define MPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, c) \
    MPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, (c)->comm)

#define MPI_Allreduce(sendbuf, recvbuf, count, datatype, op, c) \
    MPI_Allreduce(sendbuf, recvbuf, count, datatype, op, (c)->comm)

#define MPI_Reduce_scatter(sendbuf, recvbuf, rcount, datatype, op, c) \
    MPI_Reduce_scatter(sendbuf, recvbuf, rcount, datatype, op, (c)->comm)

#define MPI_Scan(sendbuf, recvbuf, count, datatype, op, c) \
    MPI_Scan(sendbuf, recvbuf, count, datatype, op, (c)->comm)

#define MPI_Scatter(sbuf, scount, stype, rbuf, rcount, rtype, root, c) \
    MPI_Scatter(sbuf, scount, stype, rbuf, rcount, rtype, root, (c)->comm)

#define MPI_Scatterv(sbuf, scnts, displs, stype, rbuf, rcnt, rtype, root, c) \
    MPI_Scatterv(sbuf, scnts, displs, stype, rbuf, rcnt, rtype, root, (c)->comm)

#define MPI_Gather(sbuf, scount, stype, rbuf, rcount, rtype, root, c) \
    MPI_Gather(sbuf, scount, stype, rbuf, rcount, rtype, root, (c)->comm)

#define MPI_Gatherv(sbuf, scount, stype, rbuf, rcnts, displs, rtype, root, c) \
    MPI_Gatherv(sbuf, scount, stype, rbuf, rcnts, displs, rtype, root, (c)->comm)

#define MPI_Allgather(sbuf, scount, stype, rbuf, rcount, rtype, c) \
    MPI_Allgather(sbuf, scount, stype, rbuf, rcount, rtype, (c)->comm)

#define MPI_Allgatherv(sbuf, scount, stype, rbuf, rcounts, displs, rtype, c) \
    MPI_Allgatherv(sbuf, scount, stype, rbuf, rcounts, displs, rtype, (c)->comm)

#define MPI_Alltoall(sbuf, scount, stype, rbuf, rcount, rtype, c) \
    MPI_Alltoall(sbuf, scount, stype, rbuf, rcount, rtype, (c)->comm)

#define MPI_Alltoallv(sbuf, scnts, sdispls, stype, rbuf, rcnts, rdispls, rtype, c) \
    MPI_Alltoallv(sbuf, scnts, sdispls, stype, rbuf, rcnts, rdispls, rtype, (c)->comm)

#define MPI_Alltoallw(sbuf, scnts, sdispls, stypes, rbuf, rcnts, rdispls, rtypes, c) \
    MPI_Alltoallw(sbuf, scnts, sdispls, stypes, rbuf, rcnts, rdispls, rtypes, (c)->comm)

#define MPI_Abort HMPI_Abort
#define MPI_Finalize HMPI_Finalize


//TODO NOT IMPLEMENTED YET
// Added to catch apps that call these routines.
#define MPI_Comm_group HMPI_Comm_group

#endif //HMPI_INTERNAL

#ifdef __cplusplus
}
#endif
#endif

