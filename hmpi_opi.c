#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include "hmpi.h"
#include "lock.h"

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

#define RECVER_POOL 1
//#define SENDER_POOL 2

#define ALIGNMENT 4096

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define HDR_TO_PTR(ft)  (void*)((uintptr_t)(ft) + ALIGNMENT)
#define PTR_TO_HDR(ptr) (opi_hdr_t*)((uintptr_t)(ptr) - ALIGNMENT)

#define MAX_BUF_COUNT 128 //This threshold triggers buffer frees
#define MIN_BUF_COUNT (MAX_BUF_COUNT - 16) //Keep this many buffers

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
    opi_hdr_t* head;
    int buf_count;

#ifdef SENDER_POOL
//    lock_t lock;
    int lock;
#endif
} mpool_t;

static mpool_t* g_mpool = NULL;


//Reuse the shared lock Q node from the P2P code.
//Have to make sure not to use the Q node twice -- here, we only use it in
// alloc and free, and those aren't called from locked P2P code.
extern mcs_qnode_t* g_lock_q;


void OPI_Init(void)
{
    //Initialize the local memory pool.
    g_mpool = memalign(8, sizeof(mpool_t));

    g_mpool->head = NULL;
    //g_mpool.buf_count = 0;

#ifdef SENDER_POOL
    //LOCK_INIT(&g_mpool->lock);
    g_mpool->lock = 0;
#endif
}


void OPI_Finalize(void)
{
    opi_hdr_t* cur;

    while(g_mpool->head != NULL) {
        cur = g_mpool->head;
        g_mpool->head = cur->next;
        free(cur);
    }

    free(g_mpool);
    g_mpool = NULL;
}


int OPI_Alloc(void** ptr, size_t length)
{
    *ptr = malloc(length);
    return MPI_SUCCESS;
#if 0
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
#ifdef __bg__
    LOCK_ACQUIRE(&mp->lock);
#else
    //mcs_qnode_t* q = g_lock_q;
    //__LOCK_ACQUIRE(&mp->lock, q);
    while(__sync_lock_test_and_set(&mp->lock, 1) != 0);
#endif //__bg__
#endif //SENDER_POOL

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

#ifdef SENDER_POOL
#ifdef __bg__
            LOCK_RELEASE(&mp->lock);
#else
            //__LOCK_RELEASE(&mp->lock, q);
            __sync_lock_release(&mp->lock);
#endif //__bg__
#endif //SENDER_POOL

            //printf("%p reuse addr %p length %llu\n", mp, cur, (uint64_t)length); fflush(stdout);
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
            //mp->buf_count--;
            *ptr = HDR_TO_PTR(cur);
            return MPI_SUCCESS;
        }
    }

#ifdef SENDER_POOL
#ifdef __bg__
        LOCK_RELEASE(&mp->lock);
#else
        //__LOCK_RELEASE(&mp->lock, q);
        __sync_lock_release(&mp->lock);
#endif //__bg__
#endif //SENDER_POOL

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
#endif
}


int OPI_Free(void** ptr)
{
    free(*ptr);
    return MPI_SUCCESS;
#if 0
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Free);
#ifdef RECVER_POOL
    mpool_t* mp = g_mpool;
#endif
    opi_hdr_t* hdr = PTR_TO_HDR((*ptr));
#ifdef SENDER_POOL
    mpool_t* mp = hdr->mpool;
#endif

//    printf("%p free ptr %p hdr %p length %llu\n", mp, ptr, HDR_TO_PTR(hdr), (uint64_t)hdr->length);
//    fflush(stdout);

#ifdef MPOOL_CHECK
    if(unlikely(hdr->magic != MAGIC_VAL)) {
        free(*ptr);
        return MPI_SUCCESS;
    }

    assert(hdr->in_pool == 0);
    hdr->in_pool = 1;
#endif



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

#ifdef SENDER_POOL
#ifdef __bg__
    LOCK_ACQUIRE(&mp->lock);
#else
    //mcs_qnode_t* q = g_lock_q;
    //__LOCK_ACQUIRE(&mp->lock, q);
    while(__sync_lock_test_and_set(&mp->lock, 1) != 0);
#endif //__bg__
#endif

    hdr->next = mp->head;
    mp->head = hdr;

#ifdef SENDER_POOL
#ifdef __bg__
    LOCK_RELEASE(&mp->lock);
#else
        //__LOCK_RELEASE(&mp->lock, q);
        __sync_lock_release(&mp->lock);
#endif //__bg__
#endif

    *ptr = NULL;
    FULL_PROFILE_STOP(OPI_Free);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
#endif
}

