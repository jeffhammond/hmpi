#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include "hmpi.h"
#include "lock.h"

#include "profile2.h"

#ifdef FULL_PROFILE
#define FULL_PROFILE_VAR(v) PROFILE_VAR(v)
#define FULL_PROFILE_EXTERN(v) PROFILE_EXTERN(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v) PROFILE_SHOW_REDUCE(v)
#else
#define FULL_PROFILE_VAR(v)
#define FULL_PROFILE_EXTERN(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_SHOW_REDUCE(v)
#endif

FULL_PROFILE_EXTERN(OPI_Alloc);
FULL_PROFILE_EXTERN(OPI_Free);
FULL_PROFILE_EXTERN(OPI_Give);
FULL_PROFILE_EXTERN(OPI_Take);

#define ALIGNMENT 4096

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define HDR_TO_PTR(ft)  (void*)((uintptr_t)(ft) + ALIGNMENT)
#define PTR_TO_HDR(ptr) (opi_hdr_t*)((uintptr_t)(ptr) - ALIGNMENT)

#define MAX_BUF_COUNT 128 //This threshold triggers buffer frees
#define MIN_BUF_COUNT (MAX_BUF_COUNT - 16) //Keep this many buffers

#define MAGIC_VAL 0x13579BDF02468ACELLU

#define MPOOL_CHECK 1

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
    opi_hdr_t* head;
    //int buf_count;

    lock_t lock;
} mpool_t;

static __thread mpool_t g_mpool;


void OPI_Init(void)
{
    //Initialize the local memory pool.
    g_mpool.head = NULL;
    //g_mpool.buf_count = 0;

    LOCK_INIT(&g_mpool.lock);
}


void OPI_Finalize(void)
{
    opi_hdr_t* cur;

    while(g_mpool.head != NULL) {
        cur = g_mpool.head;
        g_mpool.head = cur->next;
        free(cur);
    }
}


int OPI_Alloc(void** ptr, size_t length)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Alloc);
    mpool_t* mp = &g_mpool;

    //Round length up to a page.
    if(length % ALIGNMENT) {
        length = ((length / ALIGNMENT) + 1) * ALIGNMENT;
    }

    //First look for an existing allocation -- first fit for now.
    opi_hdr_t* cur;
    opi_hdr_t* prev;

    LOCK_ACQUIRE(&mp->lock);

    for(prev = NULL, cur = mp->head; cur != NULL;
            prev = cur, cur = cur->next) {
        if(length <= cur->length) {
            //Good buffer, claim it.
            if(prev == NULL) {
                mp->head = cur->next;
            } else {
                //Not at head of list, just remove.
                prev->next = cur->next;
            }

            LOCK_RELEASE(&mp->lock);

            //printf("%p reuse addr %p length %llu\n", mp, cur, (uint64_t)length); fflush(stdout);
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
            //mp->buf_count--;
            *ptr = HDR_TO_PTR(cur);
            return MPI_SUCCESS;
        }
    }

        LOCK_RELEASE(&mp->lock);

    //If no existing allocation is found, allocate a new one.
    opi_hdr_t* hdr = (opi_hdr_t*)memalign(ALIGNMENT, length + ALIGNMENT);

    hdr->length = length;
    hdr->mpool = mp;

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
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Free);
    //mpool_t* mp = &g_mpool;
    opi_hdr_t* hdr = PTR_TO_HDR((*ptr));
    mpool_t* mp = hdr->mpool;

    //printf("%p free ptr %p hdr %p length %llu\n", mp, ptr, HDR_TO_PTR(hdr), (uint64_t)hdr->length);
    //fflush(stdout);

#ifdef MPOOL_CHECK
#if 0
    if(unlikely(hdr->magic != MAGIC_VAL)) {
        free(*ptr);
        return MPI_SUCCESS;
    }
#endif

    //assert(hdr->in_pool == 0);
    if(hdr->in_pool != 0) {
        abort();
    }
    hdr->in_pool = 1;
#endif


    LOCK_ACQUIRE(&mp->lock);

#if 0
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

    hdr->next = mp->head;
    mp->head = hdr;

    LOCK_RELEASE(&mp->lock);

    *ptr = NULL;
    FULL_PROFILE_STOP(OPI_Free);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}

