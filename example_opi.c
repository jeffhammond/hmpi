#include "hmpi.h"
#include "opi.h"
#include <stdlib.h>

int tmain(int argc, char** argv)
{
    int rank;
    int i;

    //Initialization
    //MPI_Init(&argc, &argv);
    OPI_Init();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
        //Send a buffer to rank 1
        int* buffer;
        OPI_Request req;

        OPI_Alloc((void**)&buffer, sizeof(int) * 10);

        //Fill the buffer with data
        for(i = 0; i < 10; i++) {
            buffer[i] = i;
        }

        //Pass ownership of the buffer
        OPI_Give((void**)&buffer, 10, MPI_INT, 1, 0, MPI_COMM_WORLD, &req);
        OPI_Wait(&req, MPI_STATUS_IGNORE);
    } else if(rank == 1) {
        //Receive a buffer from rank 0
        //When using ownership passing, we don't want to malloc here.
        //int* buffer = malloc(sizeof(int) * 10);
        int* buffer;
        OPI_Request req;

        //We need to track where the buffer comes from so we know where to
        // return it.
        //In the local case, it comes from some other rank; in the MPI case it
        // comes from ourselves.
        int src_rank;

        OPI_Take((void**)&buffer, 10, MPI_INT, 0, 0, MPI_COMM_WORLD, &req);
        OPI_Wait(&req, MPI_STATUS_IGNORE);

        //Do something with the buffer
        int sum = 0;

        for(i = 0; i < 10; i++) {
            sum += buffer[i];
        }

        printf("sum: %d\n", sum); fflush(stdout);

        //We return the buffer to the pool instead of freeing it.
        OPI_Free((void**)&buffer);
    }

    //Shut down
    OPI_Finalize();
    MPI_Finalize();
    return 0;
}

