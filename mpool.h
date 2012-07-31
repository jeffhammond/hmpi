#ifndef __MPOOL_H_
#define __MPOOL_H_
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include "lock.h"

#define ALIGNMENT 4096

#ifndef ALIGN
#ifdef __bg__
//#define ALIGN(ptr) __alignx(ALIGNMENT, ptr)
#define ALIGN(ptr)
#else
#define ALIGN(ptr)
#endif
#endif

#ifndef PREFETCH
#define PREFETCH(x) __builtin_prefetch(x)
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
#ifdef USE_MCS
    mcs_lock_t lock;
#else
    lock_t lock;
#endif

//    uint64_t num_allocs;
//    uint64_t num_reuses;
} mpool_t;



//Create a new mpool object.
static mpool_t* mpool_open(void)
{
    mpool_t* mp = (mpool_t*)malloc(sizeof(mpool_t));
    
    mp->head = NULL;

#ifdef USE_MCS
    MCS_LOCK_INIT(&mp->lock);
#else
    LOCK_INIT(&mp->lock, 0);
#endif

    //mp->num_allocs = 0;
    //mp->num_reuses = 0;
    return mp;
}


//Close an mpool object, freeing any cached allocations.
static void mpool_close(mpool_t* mp)
{
    mpool_footer_t* cur;

    while(mp->head != NULL) {
        cur = mp->head;
        //printf("%p close addr %p length %llu\n", mp, cur->base, (uint64_t)cur->length); fflush(stdout);
        mp->head = cur->next;
        free(cur);
    }

    //printf("%p num_allocs %llu\n", mp, mp->num_allocs);
    //printf("%p num_reuses %llu\n", mp, mp->num_reuses);
    //fflush(stdout);
    free(mp);
}


//Allocate a buffer, first checking the mpool for an existing allocation.
static void* mpool_alloc(mpool_t* mp, size_t length)
{
    //Round length up to a page.
    if(length % ALIGNMENT) {
        length = ((length / ALIGNMENT) + 1) * ALIGNMENT;
    }

    //First look for an existing allocation -- first fit for now.
    //TODO - Free places buffers at the start of the list.. do I want this?
    // Would cause the owner core to generate eviction notices to the recver
    // Might be better to add at the end...
    mpool_footer_t* cur;
    mpool_footer_t* prev;

#ifdef USE_MCS
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&mp->lock, &q);
#else
    LOCK_SET(&mp->lock);
#endif
    //__lwsync();
#if 0
    cur = mp->head;
    if(cur != NULL) {
        if(length <= cur->length) {
            mp->head = cur->next;
            //MCS_LOCK_RELEASE(&mp->lock, &q);
            LOCK_CLEAR(&mp->lock);
#ifdef MPOOL_CHECK
            cur->in_pool = 0;
#endif
            return cur->base;
        }
        LOCK_CLEAR(&mp->lock);
#endif            

        //for(prev = cur, cur = cur->next; cur != NULL;
        for(prev = NULL, cur = mp->head; cur != NULL;
                prev = cur, cur = cur->next) {
            ALIGN(cur);
            if(length <= cur->length) {
                //Good buffer, claim it.
                if(prev == NULL) {
                    mp->head = cur->next;
                } else {
                    //Not at head of list, just remove.
                    prev->next = cur->next;
                }
#ifdef USE_MCS
                MCS_LOCK_RELEASE(&mp->lock, &q);
#else
                LOCK_CLEAR(&mp->lock);
#endif

                //printf("%p reuse addr %p length %llu\n", mp, cur->base, (uint64_t)length); fflush(stdout);
                //mp->num_reuses++;
#ifdef MPOOL_CHECK
                cur->in_pool = 0;
#endif
                cur->next = NULL;
                ALIGN(cur->base);
                return cur->base;
            }
        }

#ifdef USE_MCS
        MCS_LOCK_RELEASE(&mp->lock, &q);
#else
        LOCK_CLEAR(&mp->lock);
#endif
        //__lwsync();
#if 0
    } else {
        //MCS_LOCK_RELEASE(&mp->lock, &q);
        LOCK_CLEAR(&mp->lock);
    }
#endif


    //If no existing allocation is found, allocate a new one.
    mpool_footer_t* ft = (mpool_footer_t*)memalign(ALIGNMENT, length + ALIGNMENT);

    //mpool_footer_t* ft = (mpool_footer_t*)((uintptr_t)ptr + length);
    //ft->next = NULL;
    ft->base = (void*)((uintptr_t)ft + ALIGNMENT);
    ft->length = length;
#ifdef MPOOL_CHECK
    ft->in_pool = 0;
#endif
    //mp->num_allocs++;
    //printf("%p alloc addr %p length %llu\n", mp, ft->base, (uint64_t)length); fflush(stdout);
    ft->next = NULL;
    //__lwsync();
    ALIGN(ft->base);
    return ft->base;
}


//Return a buffer to the mpool for later reuse.
static void mpool_free(mpool_t* mp, void* ptr)
{
    mpool_footer_t* ft = (mpool_footer_t*)((uintptr_t)ptr - ALIGNMENT);

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


#ifdef USE_MCS
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&mp->lock, &q);
#else
    LOCK_SET(&mp->lock);
#endif

    ft->next = mp->head;
    //__lwsync();
    mp->head = ft;

#ifdef USE_MCS
    MCS_LOCK_RELEASE(&mp->lock, &q);
#else
    LOCK_CLEAR(&mp->lock);
#endif
}


#endif

