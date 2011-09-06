#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>
#include <stdint.h>

//#define CACHE_LINE 64
//#define CACHE_LINE 1

//GCC only provides a builtin for mfence, not sfence or lfence.
#define STORE_FENCE() __asm__("sfence")
#define LOAD_FENCE() __asm__("lfence")

typedef struct {
  //Centralized barrier
  int64_t* local_sense;
  volatile int64_t global_sense;
  volatile int64_t count;
  int64_t threads;
} barrier_t;


static int barrier_init(barrier_t *barrier, int threads) {
  barrier->local_sense = (int64_t*)calloc(sizeof(int64_t) /** CACHE_LINE*/, threads);
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
  int64_t local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  int64_t val = __sync_fetch_and_sub(&barrier->count, (int64_t)1);

  if(val == 1) {
      barrier->count = barrier->threads;
      //__sync_synchronize();
      STORE_FENCE();
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense);
  return;
}


static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) {
  int local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  int64_t val = __sync_fetch_and_sub(&barrier->count, (int64_t)1);
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
