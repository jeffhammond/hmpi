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

#define MAX_ELEMS 64


int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int rank;
    int nbor;
    int size;
    int count;
    MPI_Request req;
    MPI_Status st;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    nbor = (rank + 1) % 2;

    WARNING("starting MPI_BYTE");
    {
        char sbuf[MAX_ELEMS] = {0};
        char rbuf[MAX_ELEMS] = {0};

        for(int i = 0; i < MAX_ELEMS; i++) {
            MPI_Irecv(rbuf, i, MPI_BYTE, nbor, i, MPI_COMM_WORLD, &req);
            MPI_Send(sbuf, i, MPI_BYTE, nbor, i, MPI_COMM_WORLD);

            MPI_Wait(&req, &st);

            count = -1;
            MPI_Get_count(&st, MPI_BYTE, &count);

            if(count != i) {
                WARNING("%d MPI_BYTE got count %d, should be %d", rank, count, i);
            }
        }
    }

    WARNING("starting MPI_INT");
    {
        int sbuf[MAX_ELEMS] = {0};
        int rbuf[MAX_ELEMS] = {0};

        for(int i = 0; i < MAX_ELEMS; i++) {
            MPI_Irecv(rbuf, i, MPI_INT, nbor, i, MPI_COMM_WORLD, &req);
            MPI_Send(sbuf, i, MPI_INT, nbor, i, MPI_COMM_WORLD);

            MPI_Wait(&req, &st);

            count = -1;
            MPI_Get_count(&st, MPI_INT, &count);

            if(count != i) {
                WARNING("%d MPI_INT got count %d, should be %d", rank, count, i);
            }
        }
    }

    WARNING("starting MPI_DOUBLE");
    {
        double sbuf[MAX_ELEMS] = {0};
        double rbuf[MAX_ELEMS] = {0};

        for(int i = 0; i < MAX_ELEMS; i++) {
            MPI_Irecv(rbuf, i, MPI_DOUBLE, nbor, i, MPI_COMM_WORLD, &req);
            MPI_Send(sbuf, i, MPI_DOUBLE, nbor, i, MPI_COMM_WORLD);

            MPI_Wait(&req, &st);

            count = -1;
            MPI_Get_count(&st, MPI_DOUBLE, &count);

            if(count != i) {
                WARNING("%d MPI_DOUBLE got count %d, should be %d", rank, count, i);
            }
        }
    }

    WARNING("finished!");
    MPI_Finalize();
    return 0;
}

