#ifndef __OPI_H__
#define __OPI_H__
#include "hmpi.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OPI_Request {
    HMPI_Request req;
    void* ptr;
    int do_free;
} OPI_Request;


void OPI_Init(void);
void OPI_Finalize(void);

int OPI_Alloc(void** ptr, size_t length);
int OPI_Free(void** ptr);

int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req);

int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req);

int OPI_Test(OPI_Request *request, int *flag, HMPI_Status *status);
int OPI_Testall(int count, OPI_Request *requests, int* flag, HMPI_Status *statuses);
int OPI_Wait(OPI_Request *request, HMPI_Status *status);
int OPI_Waitall(int count, OPI_Request* requests, HMPI_Status* statuses);
int OPI_Waitany(int count, OPI_Request* requests, int* index, HMPI_Status *status);

#ifdef __cplusplus
}
#endif
#endif
