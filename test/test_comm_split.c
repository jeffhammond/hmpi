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
#include <stdio.h>
#include <hmpi.h>
#include <error.h>

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int rank;
    int size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Comm dup;

    //Split COMM_WORLD into two halves, with the first size/2 ranks in a comm.
    int color = rank < (size / 2);
    MPI_Comm_split(MPI_COMM_WORLD, color, 0, &dup);

    int dup_rank;
    int dup_size;

    MPI_Comm_rank(dup, &dup_rank);
    MPI_Comm_size(dup, &dup_size);

    //Check that the rank in the split comm is correct.
    if(rank < size / 2) {
        if(rank != dup_rank) {
            ERROR("WORLD rank %d dup rank %d should match", rank, dup_rank);
        }

        if(dup_size != size / 2) {
            ERROR("%d WORLD size %d dup size %d should be %d",
                    rank, size, dup_size, size / 2);
        }
    } else {
        if(rank - (size / 2) != dup_rank) {
            ERROR("WORLD rank %d dup rank %d should be %d",
                    rank, dup_rank, rank - (size / 2));
        }

        if(size % 2 == 0 && dup_size != size / 2) {
            ERROR("%d WORLD size %d dup size %d should be %d",
                    rank, size, dup_size, size / 2);
        } else if(size % 2 == 1 && dup_size != (size / 2) + 1) {
            ERROR("%d WORLD size %d dup size %d should be %d",
                    rank, size, dup_size, (size / 2) + 1);
        }
    }


    MPI_Request sreq;

    for(int i = 1; i <= dup_size; i++) {
        int sbuf[2] = {17, 29};
        int rbuf[2] = {-1, -1};

        int send_rank = (dup_rank + i) % dup_size;
        int recv_rank = ((dup_rank - i) + dup_size) % dup_size;

        WARNING("WORLD rank %d dup rank %d send %d recv %d",
                rank, dup_rank, send_rank, recv_rank);

        MPI_Isend(sbuf, 2, MPI_INT, 
                (dup_rank + i) % dup_size, i, dup, &sreq);

        MPI_Recv(rbuf, 2, MPI_INT, 
                ((dup_rank - i) + dup_size) % dup_size, i, dup,
                MPI_STATUS_IGNORE);

        MPI_Wait(&sreq, MPI_STATUS_IGNORE);

        if(rbuf[0] != 17 || rbuf[1] != 29) {
            ERROR("%d dup_rank %d received %d %d should be %d %d",
                    rank, dup_rank, rbuf[0], rbuf[1], 17, 29);
        }
    }

    //if(rank == 0) {
        WARNING("%d PASS!", rank);
    //}
    MPI_Finalize();
    return 0;
}

