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
#ifndef BARRIER_H
#define BARRIER_H
//#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
//#include <malloc.h>

#include "lock.h"


//TODO - use SM_MALLOC
//#ifndef MALLOC
//#define MALLOC(t, s) (t*)memalign(64, sizeof(t) * s)
//#define MALLOC(t, s) (t*)malloc(sizeof(t) * s)
//#endif

//#define CACHE_LINE 64
//#define CACHE_LINE 1
//#if (defined __IBMC__ || defined __IBMCPP__) && defined __64BIT__ && defined __bg__

#if 0
#define USE_L2_BARRIER
#include <spi/include/kernel/memory.h>
#include <spi/include/l2/barrier.h>

//typedef L2_Barrier_t L2_barrier_t;
typedef struct barrier_t
{
    L2_Barrier_t barrier;
    int nthreads;
} barrier_t;

static int barrier_init(barrier_t *barrier, int nthreads) __attribute__((unused));

static int barrier_init(barrier_t *barrier, int nthreads) {
    if(Kernel_L2AtomicsAllocate(&barrier->barrier, sizeof(L2_Barrier_t))) {
        printf("ERROR Unable to allocate L2 atomic memory\n");
        fflush(stdout);
        assert(0);
    }

    memset(&barrier->barrier, 0, sizeof(L2_Barrier_t));
    barrier->nthreads = nthreads;
    return 0;
}


static int barrier_destroy(barrier_t *barrier) __attribute__((unused));

static int barrier_destroy(barrier_t *barrier) {
  return 0;
}


//Standard barrier
static inline void barrier(barrier_t *barrier) __attribute__((unused));

static inline void barrier(barrier_t *barrier) {
    L2_Barrier(&barrier->barrier, barrier->nthreads);
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void barrier_cb(barrier_t *barrier, void (*cbfn)(void)) __attribute__((unused));


//This is a copy of IBM's L2_Barrier modified to use the callback while waiting.
static inline void barrier_cb(barrier_t *barrier, void (*cbfn)(void)) {
    L2_Barrier_t* b = &barrier->barrier;
    uint64_t target = b->start + barrier->nthreads;
    uint64_t current = L2_AtomicLoadIncrement(&b->count) + 1;

    if (current == target) {
        b->start = current;  // advance to next round
    } else {
        while (b->start < current) {  // wait for advance to next round
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
  uint64_t nthreads;
  volatile uint64_t count;
  char padding[48];
  volatile uint64_t cur;
} barrier_t;


static int barrier_init(barrier_t *barrier, int nthreads) __attribute__((unused));

static int barrier_init(barrier_t *barrier, int nthreads) {
  barrier->nthreads = nthreads;
  barrier->count = 0;
  barrier->cur = 0;
  return 0;
}


static int barrier_destroy(barrier_t *barrier) __attribute__((unused));

static int barrier_destroy(barrier_t *barrier) {
  return 0;
}


//Standard barrier
static inline void barrier(barrier_t *barrier) __attribute__((unused));

static inline void barrier(barrier_t *barrier) {
    uint64_t count = barrier->count + barrier->nthreads;
    uint64_t cur = FETCH_ADD64((long*)&barrier->cur, (uint64_t)1) + 1;

    if(cur == count) {
        //Last thread in.. change barrier's count.
        barrier->count = count;
        return;
    }

    //Wait for barrier's count to change value.
    while(barrier->count < count);
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void barrier_cb(barrier_t *barrier, void (*cbfn)(void)) __attribute__((unused));

static inline void barrier_cb(barrier_t *barrier, void (*cbfn)(void)) {
    uint64_t count = barrier->count + barrier->nthreads;
    uint64_t cur = FETCH_ADD64((long*)&barrier->cur, (uint64_t)1) + 1;

    if(cur == count) {
        barrier->count = count;
        return;
    }

    while(barrier->count < count) {
        cbfn();
    }
}



#if 0
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
    //TODO - use SM_MALLOC
    //barrier->nodes = MALLOC(treenode_t, nthreads);
    barrier->nodes = (treenode_t*)memalign(64, sizeof(treenode_t) * nthreads);

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
//#endif //Default implementations (x86)

#endif
