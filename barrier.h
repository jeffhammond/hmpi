#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>

//#define CACHE_LINE 64
//#define CACHE_LINE 1

typedef struct {
  //Centralized barrier
  int* local_sense;
  volatile int global_sense;
  int count;
  int threads;
} barrier_t;


static int barrier_init(barrier_t *barrier, int threads) {
  barrier->local_sense = (int*)calloc(sizeof(int) /** CACHE_LINE*/, threads);
  barrier->global_sense = 0;
  barrier->count = threads;
  barrier->threads = threads;
  return 0;
}


static int barrier_destroy(barrier_t *barrier) {
  free(barrier->local_sense);
  return 0;
}


static inline void barrier(barrier_t *barrier, int tid) {
  int local_sense = barrier->local_sense[tid/* * CACHE_LINE*/] = ~barrier->local_sense[tid/* * CACHE_LINE*/];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  int val = __sync_fetch_and_sub(&barrier->count, (int)1);
  if(val == 1) {
      barrier->count = barrier->threads;
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense) {
#if 0
        int flag;
        MPI_Status st;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &st);
#endif
  }
  return;
}


static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) {
  int local_sense = barrier->local_sense[tid /** CACHE_LINE*/] = ~barrier->local_sense[tid /** CACHE_LINE*/];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  int val = __sync_fetch_and_sub(&barrier->count, (int)1);
  if(val == 1) {
      barrier->count = barrier->threads;
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense) {
      cbfn();
  }
  return;
}


#endif
