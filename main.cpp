#include "hmpi.h"

void tmain(int argc, char** argv){

  int p,r;
  HMPI_Comm_rank (HMPI_COMM_WORLD, &r);
  HMPI_Comm_size (HMPI_COMM_WORLD, &p);
  
  printf("rank %i of %i\n", r, p);


//#define PINGPONG
#ifdef PINGPONG
#define MAXSIZE 1024 //*1024*5
#define SAMPLES 1000
  char *sbuf = (char*)malloc(MAXSIZE);
  char *rbuf = (char*)malloc(MAXSIZE);
  if(p>=2) {
    if(r == 0) {
      for(int sample=0; sample<SAMPLES; sample++) {
        printf("yay 0\n"); fflush(stdout);
        HMPI_Send(sbuf, 1, MPI_BYTE, 1, 99, HMPI_COMM_WORLD);
        HMPI_Recv(rbuf, 1, MPI_BYTE, 1, 99, HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
      }
    } else {
      for(int sample=0; sample<SAMPLES; sample++) {
        printf("yay 1\n"); fflush(stdout);
        //printf("begin size: %i, %x, sample: %i, %x\n", size, &size, sample, &sample);
        HMPI_Recv(sbuf, 1, MPI_BYTE, 0, 99, HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
        HMPI_Send(rbuf, 1, MPI_BYTE, 0, 99, HMPI_COMM_WORLD);
        //printf("end size: %i, sample: %i\n", size, sample);
      }
    }

    // end warmup
#if 0
    for(int size=1; size<MAXSIZE; size*=2) {
      if(r == 0) {
        double t=-MPI_Wtime();
        for(int sample=0; sample<SAMPLES; sample++) {
          HMPI_Send(sbuf, size, MPI_BYTE, 1, 99, HMPI_COMM_WORLD);
          HMPI_Recv(rbuf, size, MPI_BYTE, 1, 99, HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        t+=MPI_Wtime();
        printf("%i %f us %f MiB/s\n", size, t/SAMPLES*1e6, (double)size/(1024*1024)/(t/SAMPLES));
      } else {
        for(int sample=0; sample<SAMPLES; sample++) {
          //printf("begin size: %i, %x, sample: %i, %x\n", size, &size, sample, &sample);
          HMPI_Recv(sbuf, size, MPI_BYTE, 0, 99, HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
          HMPI_Send(rbuf, size, MPI_BYTE, 0, 99, HMPI_COMM_WORLD);
          //printf("end size: %i, sample: %i\n", size, sample);
        }
      }
    }
#endif
  }
#endif
#define TEST
#ifdef TEST
//  int buf=0;
#if 0
  if(p>=2) 
  if(r == 0) {
    buf = 100;
    HMPI_Send(&buf, 1, MPI_INT, 1, 99, HMPI_COMM_WORLD);
    printf("send finished\n");
  } else if(r == 1) {
    HMPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, 99, HMPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("buf: %i [%x] (should be 100 ;-)\n", buf, &buf);
  }
#endif

  //int x=1, y=0;
  uint64_t x[4] = {1, 2, 3, 4};
  uint64_t y[4] = {0};
  HMPI_Reduce(x, y, 4, MPI_UINT64_T, MPI_SUM, 0, HMPI_COMM_WORLD);
  //printf("[%i] reduce buf: %i %d %d %d\n", r, y[0], y[1], y[2], y[3]);

  uint64_t* z = (uint64_t*)malloc(sizeof(uint64_t) * p * 2);
  uint64_t* w = (uint64_t*)malloc(sizeof(uint64_t) * p * 2);
  memset(z, 0, sizeof(uint64_t) * p * 2);
  memset(w, 0, sizeof(uint64_t) * p * 2);

  x[0] = r;
  HMPI_Allgather(x, 1, MPI_UINT64_T, z, 1, MPI_UINT64_T, HMPI_COMM_WORLD);

  //for(int i = 0; i < p; i++) {
  //  printf("[%i] allgather buf: %d\n", r, z[i]);
  //}

  int* displs = (int*)malloc(sizeof(int) * p);
  int* counts = (int*)malloc(sizeof(int) * p);

  for(int i = 0; i < p; i++) {
      displs[i] = 2 * i;
      counts[i] = 1;
      //z[i * 2] = r;
      //z[i * 2 + 1] = 0;
  }

  HMPI_Allgatherv(x, 1, MPI_UINT64_T,
          w, counts, displs, MPI_UINT64_T, HMPI_COMM_WORLD);

  for(int i = 0; i < p; i++) {
    printf("[%i] allgatherv buf[%d]: %d %d\n", r, i * 2, w[i * 2], w[i * 2 + 1]);
  }



//  if(r == 0) y=1;
//  HMPI_Bcast(&y, 1, MPI_INT, 0, HMPI_COMM_WORLD);
//  printf("[%i] bcast buf: %i\n", r, y);
#endif

//#define TEST2
#ifdef TEST2
    int* sendbuf = (int*)malloc(sizeof(int) * 8192);
    int* recvbuf = (int*)malloc(sizeof(int) * 8192);
#if 0
    for(int i = 0; i < p; i++) {
        sendbuf[i] = i;
    }

    HMPI_Scatter(sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, 0, HMPI_COMM_WORLD);

    printf("[%i] scatter: %d %s\n", r, *recvbuf, r == *recvbuf ? "GOOD" : "BAD");

    fflush(stdout);
    HMPI_Barrier(HMPI_COMM_WORLD);

    sendbuf[0] = r + 10;
    
    HMPI_Gather(sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, 0, HMPI_COMM_WORLD);

    if(r == 0) {
      for(int i = 0; i < p; i++) {
        printf("[%i] gather %d: %d %s\n", r, i, recvbuf[i], (i + 10) == recvbuf[i] ? "GOOD" : "BAD");
      }
    }
#endif
    printf("%d sendbuf %p recvbuf %p\n", r, sendbuf, recvbuf);
    fflush(stdout);
    HMPI_Barrier(HMPI_COMM_WORLD);

    for(int i = 0; i < p; i++) {
        sendbuf[i] = r;
        recvbuf[i] = -1;
    }

    HMPI_Alltoall(sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, HMPI_COMM_WORLD);

    for(int i = 0; i < p; i++) {
        printf("[%i] alltoall %d: %d %s\n", r, i, recvbuf[i], i == recvbuf[i] ? "GOOD" : "BAD");
    }
    fflush(stdout);
#endif
  HMPI_Finalize();
}


int main(int argc, char** argv) {

    int g_numthreads;

    if(argc < 2) {
        printf("ERROR must specify number of threads: ./main <numthreads>\n");
        return -1;
    }

    //TODO - may not be portable to other MPIs?
    HMPI_Init(&argc, &argv, atoi(argv[1]), &tmain);
}
