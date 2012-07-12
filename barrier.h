#ifndef BARRIER_H
#define BARRIER_H
//#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "lock.h"

//#define CACHE_LINE 64
//#define CACHE_LINE 1
#if (defined __IBMC__ || defined __IBMCPP__) && defined __64BIT__ && defined __bg__

#include <spi/include/kernel/memory.h>
#include <spi/include/l2/barrier.h>

typedef L2_Barrier_t L2_barrier_t;

static int L2_barrier_init(L2_barrier_t *barrier, int threads) __attribute__((unused));

static int L2_barrier_init(L2_barrier_t *barrier, int threads) {
    if(Kernel_L2AtomicsAllocate(barrier, sizeof(L2_barrier_t))) {
        printf("ERROR Unable to allocate L2 atomic memory\n");
        fflush(stdout);
        assert(0);
    }

    //*barrier = L2_BARRIER_INITIALIZER;
    memset(barrier, 0, sizeof(L2_barrier_t));
    return 0;
}


static int L2_barrier_destroy(L2_barrier_t *barrier) __attribute__((unused));

static int L2_barrier_destroy(L2_barrier_t *barrier) {
  return 0;
}


//Standard barrier
static inline void L2_barrier(L2_barrier_t *barrier, int numthreads) __attribute__((unused));

static inline void L2_barrier(L2_barrier_t *barrier, int numthreads) {
    L2_Barrier(barrier, numthreads);
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void L2_barrier_cb(L2_barrier_t *barrier, int numthreads, void (*cbfn)(void)) __attribute__((unused));


//This is a copy of IBM's L2_Barrier modified to use the callback while waiting.
static inline void L2_barrier_cb(L2_barrier_t *barrier, int numthreads, void (*cbfn)(void)) {
    uint64_t target = barrier->start + numthreads;
    uint64_t current = L2_AtomicLoadIncrement(&barrier->count) + 1;

    if (current == target) {
        barrier->start = current;  // advance to next round
    } else {
        while (barrier->start < current) {  // wait for advance to next round
            cbfn();
        }

	// NOTE: It's critical to compare b->start with current, NOT with
	//       target.  It's improbable, but target could possibly be based
	//       on a b->start that was already advanced to the next round.
    }
}


#endif
//#else //Default

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

//#endif

#endif
