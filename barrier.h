#ifndef BARRIER_H
#define BARRIER_H
//#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "lock.h"

//#define CACHE_LINE 64
//#define CACHE_LINE 1

typedef struct {
  //Centralized barrier
  int32_t* local_sense;
  volatile int32_t global_sense;
  volatile int32_t count;
  int32_t threads;
} barrier_t;


static int barrier_init(barrier_t *barrier, int threads) __attribute__((unused));

static int barrier_init(barrier_t *barrier, int threads) {
  barrier->local_sense = (int32_t*)calloc(sizeof(int32_t) /** CACHE_LINE*/, threads);
  barrier->global_sense = 0;
  barrier->count = threads;
  barrier->threads = threads;
  return 0;
}


static int barrier_destroy(barrier_t *barrier) __attribute__((unused));

static int barrier_destroy(barrier_t *barrier) {
  free(barrier->local_sense);
  return 0;
}


//Standard barrier
static inline void barrier(barrier_t *barrier, int tid) __attribute__((unused));

static inline void barrier(barrier_t *barrier, int tid) {
  int32_t local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  //int64_t val = __sync_fetch_and_sub(&barrier->count, (int64_t)1);
  int32_t val = FETCH_ADD32((int*)&barrier->count, (int32_t)-1);

  if(val == 1) {
      barrier->count = barrier->threads;
      STORE_FENCE();
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense);
  return;
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) __attribute__((unused));

static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) {
  int32_t local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  //int32_t val = __sync_fetch_and_sub(&barrier->count, (int32_t)1);
  int32_t val = FETCH_ADD32((int*)&barrier->count, (int32_t)-1);

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
