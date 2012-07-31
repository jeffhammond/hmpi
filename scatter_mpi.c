#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char** argv)
{
    int rank;
    int size;
    int i;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int* sendbuf;

    if(rank == 0) {
        sendbuf = malloc(sizeof(int) * 10 * size);
        //Fill the buffer with data
        for(i = 0; i < 10 * size; i++) {
            sendbuf[i] = i;
        }
    }

    int* recvbuf = malloc(sizeof(int) * 10);

    //Scatter a buffer from rank 0
    MPI_Scatter(sendbuf, 10, MPI_INT, recvbuf, 10, MPI_INT, 0, MPI_COMM_WORLD);

    //Do something with the buffer
    int sum = 0;

    for(i = 0; i < 10; i++) {
        sum += recvbuf[i];
    }

    printf("rank %d sum: %d\n", rank, sum);


    if(rank == 0) {
        free(sendbuf);
    }

    free(recvbuf);

    MPI_Finalize();
    return 0;
}

