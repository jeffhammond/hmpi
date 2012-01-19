#ifndef __MPOOL_H_
#define __MPOOL_H_
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <hwloc.h>
#include "lock.h"

//A normal lock is used, any number of threads can alloc and free safely.
//#define LOCK_FULL 1

//Alloc and free are designed such that one thread can safely enter alloc,
//while multiple threads can simultaneously free a buffer.
#define LOCK_FREE 1

#ifdef LOCK_FULL
#ifdef LOCK_FREE
#error "Cannot define both LOCK_FULL and LOCK_FREE"
#endif
#endif

//if *ptr == oldval, then write *newval
#ifndef CAS_PTR
#define CAS_PTR(ptr, oldval, newval) \
  __sync_val_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif

#ifndef CAS_PTR_BOOL
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif

#ifndef FETCH_ADD
#define FETCH_ADD(ptr, val) \
    __sync_fetch_and_add(ptr, val)
#endif


typedef struct mpool_footer_t {
    struct mpool_footer_t* next;
    void* base;
    size_t length; //Does not include footer structure!
#ifdef MPOOL_CHECK
    int in_pool;
#endif
} mpool_footer_t;


typedef struct mpool_t {
    mpool_footer_t* head;
#ifdef LOCK_FULL
    lock_t lock;
#endif
    hwloc_cpuset_t cpuset;
    size_t pagesize;
//    uint64_t num_allocs;
//    uint64_t num_reuses;
} mpool_t;

static lock_t g_mpool_lock = {0};
static int g_mpool_init = 0;
static hwloc_topology_t g_topo;


//Create a new mpool object.
static mpool_t* mpool_open(void)
{
    mpool_t* mp = (mpool_t*)malloc(sizeof(mpool_t));
    
    //TODO - could have one global topo object.
    LOCK_SET(&g_mpool_lock);
    if(g_mpool_init == 0) {
        printf("doing hwloc init\n"); fflush(stdout);
        if(hwloc_topology_init(&g_topo) == -1) {
            printf("hwloc_topology_init error\n");
            fflush(stdout);
        }
        if(hwloc_topology_load(g_topo) == -1) {
            printf("hwloc_topology_load error\n");
            fflush(stdout);
        }
        g_mpool_init = 1;
    }

    mp->cpuset = hwloc_bitmap_alloc();
    hwloc_get_cpubind(g_topo, mp->cpuset, HWLOC_CPUBIND_THREAD);
    LOCK_CLEAR(&g_mpool_lock);

    mp->head = NULL;
#ifdef LOCK_FULL
    LOCK_INIT(&mp->lock, 0);
#endif
    mp->pagesize = getpagesize();
    //mp->num_allocs = 0;
    //mp->num_reuses = 0;
    return mp;
}


//Close an mpool object, freeing any cached allocations.
static void mpool_close(mpool_t* mp)
{
    mpool_footer_t* cur;

#if 0
    for(cur = mp->head; cur != NULL;) {
        printf("%p close addr %p length %llu\n", mp, cur->base, (uint64_t)cur->length); fflush(stdout);
        //hwloc_free(g_topo, cur->base, cur->length + sizeof(mpool_footer_t));
        //Weird sequence is because we need to grab cur->next before freeing.
        mpool_footer_t* next = cur->next;
        free(cur);
        cur = next;
    }
#endif

    while(mp->head != NULL) {
        mpool_footer_t* cur = mp->head;
        //printf("%p close addr %p length %llu\n", mp, cur->base, (uint64_t)cur->length); fflush(stdout);
        mp->head = cur->next;
        //free(cur);
        hwloc_free(g_topo, cur, cur->length + sizeof(mpool_footer_t));
    }

    hwloc_bitmap_free(mp->cpuset);
    //hwloc_topology_destroy(mp->topo);
    //printf("%p num_allocs %llu\n", mp, mp->num_allocs);
    //printf("%p num_reuses %llu\n", mp, mp->num_reuses);
    //fflush(stdout);
    free(mp);
}


//Allocate a buffer, first checking the mpool for an existing allocation.
static void* mpool_alloc(mpool_t* mp, size_t length)
{
    //Round length up to a page.
    if(length % mp->pagesize) {
        length = ((length / mp->pagesize) + 1) * mp->pagesize;
    }

    //First look for an existing allocation -- first fit for now.
    //TODO - Free places buffers at the start of the list.. do I want this?
    // Would cause the owner core to generate eviction notices to the recver
    // Might be better to add at the end...
    mpool_footer_t* cur;
    mpool_footer_t* prev;

#ifdef LOCK_FULL
    LOCK_SET(&mp->lock);
    for(prev = NULL, cur = mp->head; cur != NULL; prev = cur, cur = cur->next) {
        if(length <= cur->length) {
            //Good buffer, claim it.
            if(prev == NULL) {
                mp->head = cur->next;
            } else {
                //Not at head of list, just remove.
                prev->next = cur->next;
            }
            LOCK_CLEAR(&mp->lock);

            //printf("%p reuse addr %p length %llu\n", mp, cur->base, (uint64_t)length); fflush(stdout);
            //mp->num_reuses++;
            cur->next = NULL;
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
            return cur->base;
        }
    }
    
    LOCK_CLEAR(&mp->lock);
#endif

#ifdef LOCK_FREE
    //Need to make this thread-safe for multiple allocating threads.
    for(prev = NULL, cur = mp->head; cur != NULL; prev = cur, cur = cur->next) {
        if(length <= cur->length) {
            //Good buffer, claim it.
            if(prev == NULL) {
                //Head of list -- CAS to remove
                if(!CAS_PTR_BOOL(&mp->head, cur, cur->next)) {
                    //Element is no longer the head.. find its prev,
                    // then remove it.
                    for(prev = mp->head; prev->next != cur; prev = prev->next);
                    prev->next = cur->next;
                }
            } else {
                //Not at head of list, just remove.
                prev->next = cur->next;
            }

            //printf("%p reuse addr %p length %llu\n", mp, cur->base, (uint64_t)length); fflush(stdout);
            //mp->num_reuses++;
            cur->next = NULL;
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
            return cur->base;
        }
    }
#endif //LOCK_FREE


    //If no existing allocation is found, allocate a new one.
    mpool_footer_t* ft = (mpool_footer_t*)hwloc_alloc_membind(g_topo,
            length + mp->pagesize,
            mp->cpuset, HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_THREAD);
    //mpool_footer_t* ft = (mpool_footer_t*)memalign(mp->pagesize, length + mp->pagesize);

    //mpool_footer_t* ft = (mpool_footer_t*)((uintptr_t)ptr + length);
    ft->next = NULL;
    ft->base = (void*)((uintptr_t)ft + mp->pagesize);
    ft->length = length;
#ifdef MPOOL_CHECK
    ft->in_pool = 0;
#endif
    //mp->num_allocs++;
    //printf("%p alloc addr %p length %llu\n", mp, ft->base, (uint64_t)length); fflush(stdout);
    return ft->base;
}


//Return a buffer to the mpool for later reuse.
static void mpool_free(mpool_t* mp, void* ptr)
{
    //mpool_footer_t* ft = (mpool_footer_t*)((uintptr_t)ptr + length);
    mpool_footer_t* ft = (mpool_footer_t*)((uintptr_t)ptr - mp->pagesize);

    //printf("%p free ptr %p length %llu\n", mp, ptr, (uint64_t)ft->length);
    //fflush(stdout);

#ifdef MPOOL_CHECK
    if(ft->in_pool == 1) {
        printf("ERROR double free?\n");
        fflush(stdout);
        assert(0);
    }

    ft->in_pool = 1;
#endif

#ifdef LOCK_FULL
    LOCK_SET(&mp->lock);
    ft->next = mp->head;
    mp->head = ft;
    LOCK_CLEAR(&mp->lock);
#endif

#ifdef LOCK_FREE
    //Atomically insert at head of list.
    mpool_footer_t* next;
    do {
        next = ft->next = mp->head;
    } while(!CAS_PTR_BOOL(&mp->head, next, ft));
#endif
}


//Need a communication pattern where many ranks write to different locations
//in one big receive buffer.
//Receiver waits for a pointer from everyone as before.
//Why bother with any message passing? just use a buffer and semaphore.
//In this case I can't do the nice mpool sync avoiding stuff.

//Allocate the buffer for a tag.
// owner specifies which rank's memory it should reside in.
//AMG - do the senders know the final size of receive buffers?
void* mpool_alloc_buf(int tag, int owner, int size);

//Next, an atomic counter (semaphore?) is used to track when the buffer is rdy.

#endif

