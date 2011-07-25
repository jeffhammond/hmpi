#include <stdint.h>
#include <mpi.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
//#include "opa_primitives.h"

//#define PTHREAD_BARRIER
#define COUNTER_BARRIER
//#define ANDY_BARRIER
//#define DEBUG

#ifdef __cplusplus
extern "C" {
#endif

extern int g_nthreads;
extern int g_rank;

typedef struct {
  int32_t val;
  int32_t padding[15]; 
} cache_line_t;

#ifdef ANDY_BARRIER
typedef struct {
  //Centralized barrier
  int* local_sense;
  volatile int global_sense;
  int count;


#if 0
  //Dissemination
  int* flags[2];
  //int* partnerflags[2];

  int* parity;
  int* sense;
  int localflags;

  //int* allnodes;

  int log;
#endif
} barrier_t;
#endif

#ifdef COUNTER_BARRIER
typedef struct {
  int *counter;
  int *expected;
  pthread_mutex_t lock;
} barrier_t;
#endif 
#ifdef PTHREAD_BARRIER
typedef pthread_barrier_t barrier_t;
#endif

#if 0
typedef struct {
    volatile void* buf;
    volatile int count;
    volatile MPI_Datatype type;
} HMPI_Data_info;
#endif

typedef struct {
/*  volatile void *rootsbuf;
  volatile void *rootrbuf;
  volatile int rootscount;
  volatile int rootrcount;
  volatile MPI_Datatype rootstype;
  volatile MPI_Datatype rootrtype;
  */
  //HMPI_Data_info* sinfo;
  //HMPI_Data_info* rinfo;
  volatile void** sbuf;
  volatile int* scount;
  volatile MPI_Datatype* stype;
  volatile void** rbuf;
  volatile int* rcount;
  volatile MPI_Datatype* rtype;
  volatile void* mpi_sbuf;
  volatile void* mpi_rbuf;
  barrier_t barr;
  MPI_Comm mpicomm;
  //MPI_Comm* tcomms;
} HMPI_Comm_info;

typedef HMPI_Comm_info* HMPI_Comm;

extern HMPI_Comm HMPI_COMM_WORLD;

#define HMPI_SEND 1
#define HMPI_RECV 2
#define MPI_SEND 3
#define MPI_RECV 4
#define HMPI_RECV_ANY_SOURCE 5

//#define HMPI_ANY_SOURCE -55
//#define HMPI_ANY_TAG -55

/* this identifies a message for matching and also acts as request */
typedef struct ruqelem_t {
  int type;
  int proc;
  int tag;
  int size;
  void *buf;

  //OPA_int_t stat;
  //int stat;
  volatile uint8_t stat;

  struct ruqelem_t* next;
  struct ruqelem_t* prev;

  //pthread_mutex_t statlock;
  MPI_Request req;
  MPI_Status* status;
  // following only for HMPI_RECV_ANY_SOURCE
  MPI_Comm comm;
  MPI_Datatype datatype;
} ruqelem_t;

typedef ruqelem_t HMPI_Request;

int HMPI_Init(int *argc, char ***argv, int nthreads, void (*start_routine)());

int HMPI_Comm_rank ( HMPI_Comm comm, int *rank );
int HMPI_Comm_size ( HMPI_Comm comm, int *size );

//AWF new function -- return true (nonzero) if rank is another thread in the
// same process.

static inline int HMPI_Comm_local(HMPI_Comm comm, int rank)
{
#if HMPI_SAFE
  if(comm->mpicomm != MPI_COMM_WORLD) {
    printf("only MPI_COMM_WORLD is supported so far\n");
    MPI_Abort(comm->mpicomm, 0);
  }
#endif
 
    return (g_rank == (rank / g_nthreads));  
}


//AWF new function -- set the thread ID of the specified rank.
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


// AWF new function - barrier only among local threads
void HMPI_Barrier_local(HMPI_Comm comm);


int HMPI_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm );
int HMPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, MPI_Status *status );

int HMPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *req );
int HMPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *req );

int HMPI_Test(HMPI_Request *request, int *flag, MPI_Status *status);
int HMPI_Wait(HMPI_Request *request, MPI_Status *status);

int HMPI_Barrier(HMPI_Comm comm);
int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm);
int HMPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, HMPI_Comm comm);

int HMPI_Scatter(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm);

int HMPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, HMPI_Comm comm);

int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm);

int HMPI_Alltoall_local(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm);

int HMPI_Abort( HMPI_Comm comm, int errorcode );

int HMPI_Finalize();

#ifdef __cplusplus
}
#endif
