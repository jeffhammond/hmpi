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
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>

#define OPI_SCATTER_TAG 674643

int rank;
int size;

//Something I haven't done much with yet is owner-passing collective routines.
// There's optimizations that aren't employed here:
// - The root could overlap the sends with packing if this function is inlined and code is moved around
// - A more scalable scatter algorithm could be used
// - Start off-node communication, do the local owner passing, then finish
//   - Further yet, we can pack/unpack on-node while off-node communication
//     is in progress... future work!
int OPI_Scatter(void** sendbufs, int sendcnt, MPI_Datatype sendtype, void** recvbuf, int recvcnt, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    int i;
    OPI_Request req;

    if(rank == root) {
        for(i = 0; i < size; i++) {
            if(i == rank) {
                *recvbuf = sendbufs[i];
            } else {
                OPI_Give(sendbufs[i], i, OPI_SCATTER_TAG, comm, &req);
                MPI_Wait(&req, MPI_STATUS_IGNORE);
            }
        }
    } else {
        OPI_Take(recvbuf, root, OPI_SCATTER_TAG, comm, &req);
        OPI_Wait(&req, MPI_STATUS_IGNORE);
    }

    return MPI_SUCCESS;
}

int main(int argc, char** argv)
{
    int i, j;

    MPI_Init(&argc, &argv);
    OPI_Init();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    //Bad that we have to do an extra malloc here -- is there a better way?
    // Could replace with alloca, but still, is there something better?
    int** sendbufs; 

    if(rank == 0) {
        int** sendbufs = malloc(sizeof(int*) * size);

        //Allocate buffers and fill with data
        for(i = 0; i < size; i++) {
            OPI_Alloc(&sendbufs[i], sizeof(int) * 10);
            for(j = 0; j < 10; j++) {
                sendbufs[i][j] = i;
            }
        }
    }


    //Scatter a buffer from rank 0
    OPI_Scatter(sendbufs, 10, MPI_INT,
            &recvbuf, 10, MPI_INT, 0, MPI_COMM_WORLD);

    if(rank == 0) {
        free(sendbufs);
    }

    //Do something with the buffer
    int sum = 0;

    for(i = 0; i < 10; i++) {
        sum += recvbuf[i];
    }

    printf("rank %d sum: %d\n", rank, sum);

    OPI_Free(&recvbuf);

    OPI_Finalize();
    MPI_Finalize();
    return 0;
}

