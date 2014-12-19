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
#include "hmpi.h"
#include <string.h>
#include <stdio.h>
#include <math.h>

#include "profile2.h"

PROFILE_VAR(barrier);
PROFILE_DECLARE();

//void* sm_malloc(size_t bytes);
//void sm_free(void* mem);

int main(int argc, char** argv)
{
  int p,r;
  MPI_Init(&argc, &argv);

  MPI_Comm_rank (HMPI_COMM_WORLD, &r);
  MPI_Comm_size (HMPI_COMM_WORLD, &p);

  printf("r %d p %d\n", r, p);

  int* foo;
  MPI_Alloc_mem(sizeof(int), HMPI_INFO_NULL, &foo);
  if(r == 0) {
      *foo = 3474;
      MPI_Send(foo, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
  } else if(r == 1) {
      MPI_Recv(foo, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("got foo %d\n", *foo);
  }
  MPI_Free_mem(foo);

  int* rbuf;
  int* sbuf;
  MPI_Request req;

  //MPI_Alloc_mem(sizeof(int) * 1024, HMPI_INFO_NULL, &rbuf);
  //MPI_Alloc_mem(sizeof(int) * 1024, HMPI_INFO_NULL, &sbuf);
  rbuf = malloc(sizeof(int) * 1024);
  sbuf = malloc(sizeof(int) * 1024);

  for(int i = 0; i < 1024; i++) {
      rbuf[i] = 0xCAFEBABE;
      sbuf[i] = 0x42374237;
  }

  MPI_Irecv(rbuf, 1024, MPI_INT, r, r, MPI_COMM_WORLD, &req);
  MPI_Send(sbuf, 1024, MPI_INT, r, r, MPI_COMM_WORLD);
  MPI_Wait(&req, MPI_STATUS_IGNORE);

  for(int i = 0; i < 1024; i++) {
      if(rbuf[i] != 0x42374237) {
          printf("ERROR rbuf[%d] = %x (should be 0x42374237)\n", i, rbuf[i]);
      }
      if(sbuf[i] != 0x42374237) {
          printf("ERROR sbuf[%d] = %x (should be 0x42374237)\n", i, rbuf[i]);
      }
  }

  //MPI_Free_mem(rbuf);
  //MPI_Free_mem(sbuf);
  free(rbuf);
  free(sbuf);

  sleep(1);
  printf("%d finalize!\n", r);
  MPI_Finalize();
  return 0;


  PROFILE_INIT(r);
  
  //printf("rank %i of %i\n", r, p);

  for(int i = 0; i < 10000; i++) {
    PROFILE_START(barrier);
    MPI_Barrier(MPI_COMM_WORLD);
    PROFILE_STOP(barrier);
  }

  PROFILE_SHOW(barrier);

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

#if 0
  //int x=1, y=0;
  for(int k = 0; k < 1000; k++)
  {
      double x[4] = {1.1, 2.2, 3.3, 4.4};
      double y[4] = {0.0, 0.0, 0.0, 0.0};

      //printf("[%i] pre reduce buf: %f %f %f %f\n", r, y[0], y[1], y[2], y[3]);
      HMPI_Allreduce(x, y, 4, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD);
      //printf("[%i] reduce buf: %f %f %f %f\n", r, y[0], y[1], y[2], y[3]);

      for(int i = 0; i < 4; i++) {
          if(fabs(y[i] - (x[i] * (double)p)) > 0.00001) {
              printf("BAD VALUE k %d y[i] %g x[i] %g expect %g\n", k, y[i], x[i], x[i] * (double)p);
          }
      }
  }

#endif

#if 0
  uint64_t x[4];
  uint64_t y[4];
  uint64_t* z = (uint64_t*)malloc(sizeof(uint64_t) * p * 2);
  uint64_t* w = (uint64_t*)malloc(sizeof(uint64_t) * p * 2);
  memset(z, 0, sizeof(uint64_t) * p * 2);
  memset(w, 0, sizeof(uint64_t) * p * 2);

  x[0] = r;
  HMPI_Allgather(x, 1, MPI_UNSIGNED_LONG_LONG, z, 1, MPI_UNSIGNED_LONG_LONG, HMPI_COMM_WORLD);

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

  HMPI_Allgatherv(x, 1, MPI_UNSIGNED_LONG_LONG,
          w, counts, displs, MPI_UNSIGNED_LONG_LONG, HMPI_COMM_WORLD);

  for(int i = 0; i < p; i++) {
    printf("[%i] allgatherv buf[%d]: %llu %llu\n", r, i * 2, w[i * 2], w[i * 2 + 1]);
  }
#endif


//  if(r == 0) y=1;
//  HMPI_Bcast(&y, 1, MPI_INT, 0, HMPI_COMM_WORLD);
//  printf("[%i] bcast buf: %i\n", r, y);
#endif

//#define TEST2
#ifdef TEST2
    //int* sendbuf = (int*)malloc(sizeof(int) * 8192);
    //int* recvbuf = (int*)malloc(sizeof(int) * 8192);
    int* sendbuf;
    int* recvbuf;

    MPI_Alloc_mem(sizeof(int) * 8192, MPI_INFO_NULL, &sendbuf);
    MPI_Alloc_mem(sizeof(int) * 8192, MPI_INFO_NULL, &recvbuf);
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
    //printf("%d sendbuf %p recvbuf %p\n", r, sendbuf, recvbuf);
    //fflush(stdout);
    HMPI_Barrier(HMPI_COMM_WORLD);

    for(int i = 0; i < p; i++) {
        sendbuf[i] = r;
        recvbuf[i] = -1;
    }

    HMPI_Alltoall2(sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, HMPI_COMM_WORLD);

    for(int i = 0; i < p; i++) {
        printf("[%i] alltoall %d: %d %s\n", r, i, recvbuf[i], i == recvbuf[i] ? "GOOD" : "BAD");
    }
    fflush(stdout);
#endif
  HMPI_Finalize();
  return 0;
}


#if 0
int main(int argc, char** argv) {


    if(argc < 2) {
        printf("ERROR must specify number of threads: ./main <numthreads> <numcores> <numsockets>\n");
        return -1;
    }

    //TODO - may not be portable to other MPIs?
    HMPI_Init(&argc, &argv, &tmain, atoi(argv[1]), atoi(argv[2]), atoi(argv[3]));
}

#endif

