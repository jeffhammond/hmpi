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

    MPI_Comm_dup(MPI_COMM_WORLD, &dup);

    int dest_rank = (rank + 1) % size;
    int src_rank = (rank + size - 1) % size;

    WARNING("%d dest %d src %d", rank, dest_rank, src_rank);
    //Send a message with the same proc/tag on each comm at the same time.
    // Give them different data.
    MPI_Request sreqs[2];

    for(int i = 0; i < 10; i++) {
        int sbuf[2] = {17, 29};
        int rbuf[2] = {-1, -1};

        MPI_Isend(&sbuf[0], 1, MPI_INT, dest_rank, 15, MPI_COMM_WORLD, &sreqs[0]);
        MPI_Isend(&sbuf[1], 1, MPI_INT, dest_rank, 15, dup, &sreqs[1]);

        MPI_Recv(&rbuf[0], 1, MPI_INT, src_rank, 15, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&rbuf[1], 1, MPI_INT, src_rank, 15, dup, MPI_STATUS_IGNORE);

        MPI_Waitall(2, sreqs, MPI_STATUSES_IGNORE);

        if(rbuf[0] != 17) {
            ERROR("rbuf[0] = %d should be 17", rbuf[0]);
        }

        if(rbuf[1] != 29) {
            ERROR("rbuf[1] = %d should be 17", rbuf[0]);
        }
    }

    //Send in a different order from receiving.
    for(int i = 0; i < 10; i++) {
        int sbuf[2] = {17, 29};
        int rbuf[2] = {-1, -1};

        MPI_Isend(&sbuf[1], 1, MPI_INT, dest_rank, 15, dup, &sreqs[1]);
        MPI_Isend(&sbuf[0], 1, MPI_INT, dest_rank, 15, MPI_COMM_WORLD, &sreqs[0]);

        MPI_Recv(&rbuf[0], 1, MPI_INT, src_rank, 15, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&rbuf[1], 1, MPI_INT, src_rank, 15, dup, MPI_STATUS_IGNORE);

        MPI_Waitall(2, sreqs, MPI_STATUSES_IGNORE);

        if(rbuf[0] != 17) {
            ERROR("rbuf[0] = %d should be 17", rbuf[0]);
        }

        if(rbuf[1] != 29) {
            ERROR("rbuf[1] = %d should be 17", rbuf[0]);
        }
    }

    MPI_Comm_free(&dup);

    //Make sure COMM_WORLD still works.
    for(int i = 0; i < 10; i++) {
        int sbuf[2] = {17, 29};
        int rbuf[2] = {-1, -1};

        MPI_Isend(&sbuf[0], 1, MPI_INT, dest_rank, 14, MPI_COMM_WORLD, &sreqs[0]);
        MPI_Isend(&sbuf[1], 1, MPI_INT, dest_rank, 15, MPI_COMM_WORLD, &sreqs[1]);

        MPI_Recv(&rbuf[0], 1, MPI_INT, src_rank, 14, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&rbuf[1], 1, MPI_INT, src_rank, 15, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        MPI_Waitall(2, sreqs, MPI_STATUSES_IGNORE);

        if(rbuf[0] != 17) {
            ERROR("rbuf[0] = %d should be 17", rbuf[0]);
        }

        if(rbuf[1] != 29) {
            ERROR("rbuf[1] = %d should be 17", rbuf[0]);
        }
    }

    //if(rank == 0) {
        WARNING("PASS!");
    //}
    MPI_Finalize();
    return 0;
}

