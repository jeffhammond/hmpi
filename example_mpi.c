#include <mpi.h>
#include <stdlib.h>

int main(int argc, char** argv)
{
    int rank;
    int i;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
        //Send a buffer to rank 1
        int* buffer = malloc(sizeof(int) * 10);
        MPI_Request req;

        //Fill the buffer with data
        for(i = 0; i < 10; i++) {
            buffer[i] = i;
        }

        //Send the buffer
        MPI_Isend(buffer, 10, MPI_INT, 1, 0, MPI_COMM_WORLD, &req);
        MPI_Wait(&req, MPI_STATUS_IGNORE);

        free(buffer);
    } else if(rank == 1) {
        //Receive a buffer from rank 0
        int* buffer = malloc(sizeof(int) * 10);
        MPI_Request req;

        //Receive a buffer from the sender
        MPI_Irecv(buffer, 10, MPI_INT, 0, 0, MPI_COMM_WORLD, &req);
        MPI_Wait(&req, MPI_STATUS_IGNORE);

        //Do something with the buffer
        int sum = 0;

        for(i = 0; i < 10; i++) {
            sum += buffer[i];
        }

        print("sum: %d\n", sum);

        free(buffer);
    }

    MPI_Finalize();
    return 0;
}

