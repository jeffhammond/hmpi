#ifndef BARRIER_H
#define BARRIER_H
//#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>

#include "lock.h"

#ifndef MALLOC
#define MALLOC(t, s) (t*)memalign(64, sizeof(t) * s)
#endif

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
#if 0
  //Centralized barrier
  int32_t* local_sense;
  volatile int32_t global_sense;
  volatile int32_t count;
  int32_t threads;
#endif
  uint64_t threads;
  volatile uint64_t count;
  char padding[48];
  volatile uint64_t cur;
} barrier_t;


static int barrier_init(barrier_t *barrier, int threads) __attribute__((unused));

static int barrier_init(barrier_t *barrier, int threads) {
#if 0
  barrier->local_sense = (int32_t*)calloc(sizeof(int32_t) /** CACHE_LINE*/, threads);
  barrier->global_sense = 0;
  barrier->count = threads;
  barrier->threads = threads;
#endif
  barrier->threads = threads;
  barrier->count = 0;
  barrier->cur = 0;
  return 0;
}


static int barrier_destroy(barrier_t *barrier) __attribute__((unused));

static int barrier_destroy(barrier_t *barrier) {
  //free(barrier->local_sense);
  return 0;
}


//Standard barrier
static inline void barrier(barrier_t *barrier, int tid) __attribute__((unused));

static inline void barrier(barrier_t *barrier, int tid) {
    uint64_t count = barrier->count + barrier->threads;
    uint64_t cur = FETCH_ADD64(&barrier->cur, (uint64_t)1) + 1;

    if(cur == count) {
        //Last thread in.. change barrier's count.
        barrier->count = count;
        return;
    }

    //Wait for barrier's count to change value.
    while(barrier->count < count);


#if 0
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
#endif
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) __attribute__((unused));

static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) {
    uint64_t count = barrier->count + barrier->threads;
    uint64_t cur = FETCH_ADD64(&barrier->cur, (uint64_t)1) + 1;

    if(cur == count) {
        barrier->count = count;
        return;
    }

    while(barrier->count < count) {
        cbfn();
    }

#if 0
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

#if 0
  do {
      for(int i = 0; i < 100; i++) {
          if(barrier->global_sense == local_sense) {
              return;
          }
      }

      cbfn();
  } while(1);
#endif
#endif
}

//#endif


typedef struct treenode_t
{
    int sense;
    volatile int p_sense;
    volatile uint8_t* p_ptr;
    volatile int* c_ptrs[2];
    char padding[32+64];
    union {
        uint8_t b[8];
        uint32_t w;
    } have_child;
    //volatile uint8_t child_not_ready[4];
    union {
        volatile uint8_t b[4];
        volatile uint32_t w;
    } child_not_ready;
    int dummy;
    char pad[44+64];
} treenode_t;

typedef struct treebarrier_t
{
    treenode_t* nodes;
} treebarrier_t;


static int treebarrier_init(treebarrier_t *barrier, int nthreads) __attribute__((unused));
static int treebarrier_init(treebarrier_t* barrier, int nthreads)
{
    barrier->nodes = MALLOC(treenode_t, nthreads);

    for(int i = 0; i < nthreads; i++) {
        treenode_t* n = &barrier->nodes[i];

        n->sense = 1;
        n->p_sense = 0;

        for(int j = 0; j < 4; j++) {
            if(4 * i + j + 1 < nthreads) {
                n->have_child.b[j] = 1;
            } else {
                n->have_child.b[j] = 0;
            }

            n->child_not_ready.b[j] = n->have_child.b[j];
        }

        if(i == 0) {
            n->p_ptr = (uint8_t*)&n->dummy;
        } else {
            n->p_ptr =
                &barrier->nodes[(i - 1) / 4].child_not_ready.b[(i - 1) % 4];
        }

        if(2*i+1 >= nthreads) {
            n->c_ptrs[0] = &n->dummy;
        } else {
            n->c_ptrs[0] = &barrier->nodes[2*i+1].p_sense;
        }

        if(2*i+2 >= nthreads) {
            n->c_ptrs[1] = &n->dummy;
        } else {
            n->c_ptrs[1] = &barrier->nodes[2*i+2].p_sense;
        }
    }

    return 0;
}


static void treebarrier(treebarrier_t *barrier, int tid) __attribute__((unused));

static void treebarrier(treebarrier_t* barrier, int tid)
{
    treenode_t* n = &barrier->nodes[tid];

    //Wait on children
    while(n->child_not_ready.w);

    //Reset flags for next barrier
    n->child_not_ready.w = n->have_child.w;

    *(n->p_ptr) = 0;

    int sense = n->sense;
    if(tid != 0) {
        while(n->p_sense != sense);
    }

    *(n->c_ptrs[0]) = sense;
    *(n->c_ptrs[1]) = sense;
    n->sense = ~n->sense;
}


#endif
