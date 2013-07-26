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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include "hmpi.h"
#include "lock.h"
#include "error.h"

#include "profile.h"

#ifdef FULL_PROFILE
#define FULL_PROFILE_TIMER(v) PROFILE_TIMER(v)
#define FULL_PROFILE_TIMER_EXTERN(v) PROFILE_TIMER_EXTERN(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v) PROFILE_SHOW_REDUCE(v)
#else
#define FULL_PROFILE_TIMER(v)
#define FULL_PROFILE_TIMER_EXTERN(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v)
#endif

FULL_PROFILE_TIMER_EXTERN(OPI_Alloc);
FULL_PROFILE_TIMER_EXTERN(OPI_Free);
FULL_PROFILE_TIMER_EXTERN(OPI_Give);
FULL_PROFILE_TIMER_EXTERN(OPI_Take);

#ifdef __bg__
//BGQ needs sender side pools.. weird crashes occur otherwise.
#define RECVER_POOL 1
//#define SENDER_POOL 2
#else
//#define SENDER_POOL 2
#define RECVER_POOL 1
#endif

//BGQ L2 atomics should be better, but cause segfaults?
//#define USE_BGQ_LOCKS 1
//#define USE_LOCK_FREE 2

#define ALIGNMENT 4096

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define HDR_TO_PTR(ft)  (void*)((uintptr_t)(ft) + ALIGNMENT)
#define PTR_TO_HDR(ptr) (opi_hdr_t*)((uintptr_t)(ptr) - ALIGNMENT)

//#define TRIM_POOL 1 //Enable pool trimming: limit number of bufs in pool
#define MAX_BUF_COUNT 128 //This threshold triggers buffer frees
#define MIN_BUF_COUNT (MAX_BUF_COUNT - 32) //Keep this many buffers

#define MAGIC_VAL 0x13579BDF02468ACELLU

//#define MPOOL_CHECK 1

typedef struct opi_hdr_t {
#ifdef MPOOL_CHECK
    size_t magic;
#endif
    struct opi_hdr_t* next;
    size_t length;
    struct mpool_t* mpool;
#ifdef MPOOL_CHECK
    int in_pool;
#endif
} opi_hdr_t;


typedef struct mpool_t {
#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
    lock_t lock;
#elif !defined(USE_LOCK_FREE)
    int lock;
#endif
#endif
    opi_hdr_t* head;

#ifdef TRIM_POOL
    int buf_count;
#endif
} mpool_t;

static mpool_t* g_mpool = NULL;



void OPI_Init(void)
{
    //Initialize the local memory pool.
    g_mpool = memalign(ALIGNMENT, sizeof(mpool_t));

    g_mpool->head = NULL;
#ifdef TRIM_POOL
    g_mpool->buf_count = 0;
#endif

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
    LOCK_INIT(&g_mpool->lock);
#elif !defined(USE_LOCK_FREE)
    g_mpool->lock = 0;
#endif
#endif
}


void OPI_Finalize(void)
{
    //On BGQ, apparently a free call generates a bcast.. wtf?
    // It was causing hangs and observed with STAT in miniMD
#if 0
    opi_hdr_t* cur;

    while(g_mpool->head != NULL) {
        cur = g_mpool->head;
        g_mpool->head = cur->next;
        free(cur);
    }

    free(g_mpool);
    g_mpool = NULL;
#endif
}


int OPI_Alloc(void** ptr, size_t length)
{
#if 0
    *ptr = malloc(length);
    return MPI_SUCCESS;
#endif
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Alloc);
    mpool_t* mp = g_mpool;

    //Round length up to a page.
    if(length % ALIGNMENT) {
        length = ((length / ALIGNMENT) + 1) * ALIGNMENT;
    }

    //First look for an existing allocation -- first fit for now.
    opi_hdr_t* cur;
    opi_hdr_t* prev;

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
    LOCK_ACQUIRE(&mp->lock);
#elif !defined(USE_LOCK_FREE)
    while(__sync_lock_test_and_set(&mp->lock, 1) != 0);
#endif //__bg__
#endif //SENDER_POOL

    for(prev = NULL, cur = mp->head; cur != NULL;
            prev = cur, cur = cur->next) {
        if(length <= cur->length) {
            //Good buffer, claim it.
            if(prev == NULL) {
#ifdef USE_LOCK_FREE
                if(!CAS_PTR_BOOL(&mp->head, cur, cur->next)) {
                    //cur is no longer the head: traverse to find prev.
                    //TODO: should BGQ have a load fence here?
                    // Maybe it could read a stale next value?
                    for(prev = mp->head; prev->next != cur; prev = prev->next);

                    //Not at head of list, just remove.
                    prev->next = cur->next;
                }
#else
                mp->head = cur->next;
#endif
            } else {
                //Not at head of list, just remove.
                prev->next = cur->next;
            }

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
            LOCK_RELEASE(&mp->lock);
#elif !defined(USE_LOCK_FREE)
            __sync_lock_release(&mp->lock);
#endif //__bg__
#endif //SENDER_POOL

            //printf("%p reuse addr %p length %llu\n", mp, cur, (uint64_t)length); fflush(stdout);
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
#ifdef TRIM_POOL
            mp->buf_count--;
#endif
            *ptr = HDR_TO_PTR(cur);
            return MPI_SUCCESS;
        }
    }

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
        LOCK_RELEASE(&mp->lock);
#elif !defined(USE_LOCK_FREE)
        __sync_lock_release(&mp->lock);
#endif //__bg__
#endif //SENDER_POOL

    //If no existing allocation is found, allocate a new one.
    opi_hdr_t* hdr = (opi_hdr_t*)memalign(ALIGNMENT, length + ALIGNMENT);

    hdr->length = length;
#ifdef SENDER_POOL
    hdr->mpool = mp;
#endif

#ifdef MPOOL_CHECK
    hdr->magic = MAGIC_VAL;
    hdr->in_pool = 0;
#endif

    //printf("%p alloc addr %p length %llu\n", mp, hdr, (uint64_t)length); fflush(stdout);

    *ptr = HDR_TO_PTR(hdr);
    FULL_PROFILE_STOP(OPI_Alloc);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int OPI_Free(void** ptr)
{
#if 0
    free(*ptr);
    return MPI_SUCCESS;
#endif
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Free);
#ifdef RECVER_POOL
    mpool_t* mp = g_mpool;
#endif
    opi_hdr_t* hdr = PTR_TO_HDR((*ptr));
#ifdef SENDER_POOL
    mpool_t* mp = hdr->mpool;
#endif

    //printf("%p free ptr %p hdr %p length %llu\n", mp, ptr, HDR_TO_PTR(hdr), (uint64_t)hdr->length);
    //fflush(stdout);

#ifdef MPOOL_CHECK
    if(unlikely(hdr->magic != MAGIC_VAL)) {
        free(*ptr);
        return MPI_SUCCESS;
    }

    assert(hdr->in_pool == 0);
    hdr->in_pool = 1;
#endif



#if TRIM_POOL
    if(unlikely(mp->buf_count >= MAX_BUF_COUNT)) {
        //Remove old buffers.
        opi_hdr_t* cur = mp->head;

        //Traverse forward
        for(int i = 1; i < MIN_BUF_COUNT; i++) {
            cur = cur->next;
        }

        opi_hdr_t* temp;
        while(cur != NULL) {
            temp = cur->next;
            free(cur);
            cur = temp;
        }

        mp->buf_count = MIN_BUF_COUNT;
    } else{
        mp->buf_count++;
    }
#endif

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
    LOCK_ACQUIRE(&mp->lock);
#elif !defined(USE_LOCK_FREE)
    while(__sync_lock_test_and_set(&mp->lock, 1) != 0);
#endif //__bg__
#endif

#ifdef USE_LOCK_FREE
    //CAS the head: repeat until successful
    opi_hdr_t* head;
    do {
        head = mp->head;
        hdr->next = head;
    } while(!CAS_PTR_BOOL(&mp->head, head, hdr));
#else
    hdr->next = mp->head;
    mp->head = hdr;
#endif

#ifdef SENDER_POOL
#ifdef USE_BGQ_LOCKS
    LOCK_RELEASE(&mp->lock);
#elif !defined(USE_LOCK_FREE)
        __sync_lock_release(&mp->lock);
#endif //__bg__
#endif

    //*ptr = NULL;
    FULL_PROFILE_STOP(OPI_Free);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

