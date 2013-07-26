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
#ifdef FULL_PROFILE
#include <mpi.h>
#include "profile2.h"

//PROFILE_DECLARE();
PROFILE_VAR(MPI_Other);
PROFILE_VAR(MPI_Isend);
PROFILE_VAR(MPI_Irecv);
PROFILE_VAR(MPI_Test);
PROFILE_VAR(MPI_Testall);
PROFILE_VAR(MPI_Wait);
PROFILE_VAR(MPI_Waitall);
PROFILE_VAR(MPI_Waitany);
PROFILE_VAR(MPI_Iprobe);

PROFILE_VAR(MPI_Barrier);
PROFILE_VAR(MPI_Reduce);
PROFILE_VAR(MPI_Allreduce);
PROFILE_VAR(MPI_Scan);
PROFILE_VAR(MPI_Bcast);
PROFILE_VAR(MPI_Scatter);
PROFILE_VAR(MPI_Gather);
PROFILE_VAR(MPI_Gatherv);
PROFILE_VAR(MPI_Allgather);
PROFILE_VAR(MPI_Allgatherv);
PROFILE_VAR(MPI_Alltoall);

int MPI_Init(int *argc, char ***argv)
{
    //PROFILE_INIT(0);
    PMPI_Init(argc, argv);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Init_thread(int *argc, char ***argv, int required, int* provided)
{
    //PROFILE_INIT(0);
    int ret = PMPI_Init_thread(argc, argv, required, provided);
    PROFILE_START(MPI_Other);
    return ret;
}

int MPI_Finalize()
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Other);

    PROFILE_SHOW(MPI_Isend);
    PROFILE_SHOW(MPI_Irecv);
    PROFILE_SHOW(MPI_Test);
    PROFILE_SHOW(MPI_Testall);
    PROFILE_SHOW(MPI_Wait);
    PROFILE_SHOW(MPI_Waitall);
    PROFILE_SHOW(MPI_Waitany);
    PROFILE_SHOW(MPI_Iprobe);

    PROFILE_SHOW(MPI_Barrier);
    PROFILE_SHOW(MPI_Reduce);
    PROFILE_SHOW(MPI_Allreduce);
    PROFILE_SHOW(MPI_Scan);
    PROFILE_SHOW(MPI_Bcast);
    PROFILE_SHOW(MPI_Scatter);
    PROFILE_SHOW(MPI_Gather);
    PROFILE_SHOW(MPI_Gatherv);
    PROFILE_SHOW(MPI_Allgather);
    PROFILE_SHOW(MPI_Allgatherv);
    PROFILE_SHOW(MPI_Alltoall);

    PROFILE_SHOW(MPI_Other);

    PMPI_Finalize();
    return MPI_SUCCESS;
}


int MPI_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *req )
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Isend);
    PMPI_Isend(buf, count, datatype, dest, tag, comm, req);
    PROFILE_STOP(MPI_Isend);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm )
{
    MPI_Request req;

    MPI_Isend(buf, count, datatype, dest, tag, comm, &req);
    MPI_Wait(&req, MPI_STATUS_IGNORE);
    return MPI_SUCCESS;
}


int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *req )
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Irecv);
    PMPI_Irecv(buf, count, datatype, source, tag, comm, req);
    PROFILE_STOP(MPI_Irecv);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status )
{
    MPI_Request req;

    MPI_Irecv(buf, count, datatype, source, tag, comm, &req);
    MPI_Wait(&req, MPI_STATUS_IGNORE);
    return MPI_SUCCESS;
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int* flag, MPI_Status* status)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Iprobe);
    PMPI_Iprobe(source, tag, comm, flag, status);
    PROFILE_STOP(MPI_Iprobe);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status* status)
{
    int flag;

    do {
        MPI_Iprobe(source, tag, comm, &flag, status);
    } while(!flag);

    return MPI_SUCCESS;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Test);
    PMPI_Test(request, flag, status);
    PROFILE_STOP(MPI_Test);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Testall(int count, MPI_Request *requests, int* flag, MPI_Status *statuses)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Testall);
    PMPI_Testall(count, requests, flag, statuses);
    PROFILE_STOP(MPI_Testall);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Wait);
    PMPI_Wait(request, status);
    PROFILE_STOP(MPI_Wait);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request* requests, MPI_Status* statuses)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Waitall);
    PMPI_Waitall(count, requests, statuses);
    PROFILE_STOP(MPI_Waitall);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Waitany(int count, MPI_Request* requests, int* index, MPI_Status *status)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Waitany);
    PMPI_Waitany(count, requests, index, status);
    PROFILE_STOP(MPI_Waitany);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int MPI_Barrier(MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Barrier);
    PMPI_Barrier(comm);
    PROFILE_STOP(MPI_Barrier);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Reduce);
    PMPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, comm);
    PROFILE_STOP(MPI_Reduce);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int MPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Allreduce);
    PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);
    PROFILE_STOP(MPI_Allreduce);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int MPI_Scan(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Scan);
    PMPI_Scan(sendbuf, recvbuf, count, datatype, op, comm);
    PROFILE_STOP(MPI_Scan);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Bcast);
    PMPI_Bcast(buffer, count, datatype, root, comm);
    PROFILE_STOP(MPI_Bcast);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Scatter(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Scatter);
    PMPI_Scatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
    PROFILE_STOP(MPI_Scatter);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Gather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Gather);
    PMPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm);
    PROFILE_STOP(MPI_Gather);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Gatherv(void* sendbuf, int sendcnt, MPI_Datatype sendtype, void* recvbuf, int* recvcnts, int* displs, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Gatherv);
    PMPI_Gatherv(sendbuf, sendcnt, sendtype, recvbuf, recvcnts, displs, recvtype, root, comm);
    PROFILE_STOP(MPI_Gatherv);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Allgather(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Allgather);
    PMPI_Allgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
    PROFILE_STOP(MPI_Allgather);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Allgatherv(void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int *recvcounts, int *displs, MPI_Datatype recvtype, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Allgatherv);
    PMPI_Allgatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, comm);
    PROFILE_STOP(MPI_Allgatherv);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

int MPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
    PROFILE_STOP(MPI_Other);
    PROFILE_START(MPI_Alltoall);
    PMPI_Alltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
    PROFILE_STOP(MPI_Alltoall);
    PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

#endif
