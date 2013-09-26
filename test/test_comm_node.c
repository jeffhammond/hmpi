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

int rank;
int size;


void test_comm_node_rank(MPI_Comm comm)
{
    int node_rank;

    //PROC_NULL should return MPI_UNDEFINED
    HMPI_Comm_node_rank(comm, MPI_PROC_NULL, &node_rank);
    if(node_rank != MPI_UNDEFINED) {
        ERROR("%d MPI_PROC_NULL %d MPI_UNDEFINED %d comm_node_rank %d",
                rank, MPI_PROC_NULL, MPI_UNDEFINED, node_rank);
    }
    //ANY_SOURCE should return ANY_SOURCE
    HMPI_Comm_node_rank(comm, MPI_ANY_SOURCE, &node_rank);
    if(node_rank != MPI_ANY_SOURCE) {
        ERROR("%d MPI_ANY_SOURCE %d comm_node_rank %d",
                rank, MPI_ANY_SOURCE, node_rank);
    }

}


int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    test_comm_node_rank(MPI_COMM_WORLD);

    MPI_Comm dup;

    WARNING("%d phase two", rank);
    //Split COMM_WORLD into even/odd halves.
    MPI_Comm_split(MPI_COMM_WORLD, rank % 2, 0, &dup);

    test_comm_node_rank(dup);

    //if(rank == 0) {
        WARNING("PASS!");
    //}
    MPI_Finalize();
    return 0;
}


