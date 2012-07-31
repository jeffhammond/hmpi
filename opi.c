#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include "hmpi.h"
#include "opi.h"
#include "lock.h"

#define ALIGNMENT 4096

#define HDR_TO_PTR(ft)  (void*)((uintptr_t)ft + ALIGNMENT)
#define PTR_TO_HDR(ptr) (header_t*)((uintptr_t)ptr - ALIGNMENT)


typedef struct header_t {
    struct header_t* next;
    //void* base;
    struct mpool_t* mpool;  //Owner of this allocation
    size_t length;          //Does not include footer structure!
#ifdef MPOOL_CHECK
    int in_pool;
#endif
} header_t;


typedef struct mpool_t {
    header_t* head;
#ifdef USE_MCS
    mcs_lock_t lock;
#else
    lock_t lock;
#endif
} mpool_t;


//Private mpool object
//TODO - deal with declaring this only once.
static __thread mpool_t mpool;


void OPI_Init(void)
{
    //Initialize the local memory pool.
    mpool.head = NULL;

#ifdef USE_MCS
    MCS_LOCK_INIT(&mpool.lock);
#else
    LOCK_INIT(&mpool.lock, 0);
#endif


}


void OPI_Finalize(void)
{
    header_t* cur;

    while(mpool.head != NULL) {
        cur = mpool.head;
        //printf("%p close addr %p length %llu\n", mp, cur->base, (uint64_t)cur->length); fflush(stdout);
        mpool.head = cur->next;
        free(cur);
    }
}


int OPI_Alloc(void** ptr, size_t length)
{
    mpool_t* mp = &mpool;

    //Round length up to a page.
    if(length % ALIGNMENT) {
        length = ((length / ALIGNMENT) + 1) * ALIGNMENT;
    }

    //First look for an existing allocation -- first fit for now.
    //TODO - Free places buffers at the start of the list.. do I want this?
    // Would cause the owner core to generate eviction notices to the recver
    // Might be better to add at the end...
    header_t* cur;
    header_t* prev;

#ifdef USE_MCS
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&mp->lock, &q);
#else
    LOCK_SET(&mp->lock);
#endif
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
                *ptr = HDR_TO_PTR(cur);
                printf("allocated old buf %p %p\n", cur, *ptr);
                return MPI_SUCCESS;
            }
        }

#ifdef USE_MCS
        MCS_LOCK_RELEASE(&mp->lock, &q);
#else
        LOCK_CLEAR(&mp->lock);
#endif
#if 0
    } else {
        //MCS_LOCK_RELEASE(&mp->lock, &q);
        LOCK_CLEAR(&mp->lock);
    }
#endif


    //If no existing allocation is found, allocate a new one.
    header_t* hdr = (header_t*)memalign(ALIGNMENT, length + ALIGNMENT);

    //hdr->next = NULL;
    hdr->mpool = mp;
    hdr->length = length;

#ifdef MPOOL_CHECK
    hdr->in_pool = 0;
#endif

    //printf("%p alloc addr %p length %llu\n", mp, ft->base, (uint64_t)length); fflush(stdout);

    *ptr = HDR_TO_PTR(hdr);
    return MPI_SUCCESS;
}


int OPI_Free(void** ptr)
{
    header_t* hdr = PTR_TO_HDR(*ptr);
    mpool_t* mp = hdr->mpool;

    //printf("%p free ptr %p length %llu\n", mp, ptr, (uint64_t)hdr->length);
    //fflush(stdout);

#ifdef MPOOL_CHECK
    if(hdr->in_pool == 1) {
        printf("ERROR double free?\n");
        fflush(stdout);
        assert(0);
    }

    hdr->in_pool = 1;
#endif


#ifdef USE_MCS
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&mp->lock, &q);
#else
    LOCK_SET(&mp->lock);
#endif

    hdr->next = mp->head;
    //__lwsync();
    mp->head = hdr;

#ifdef USE_MCS
    MCS_LOCK_RELEASE(&mp->lock, &q);
#else
    LOCK_CLEAR(&mp->lock);
#endif

    *ptr = NULL;
    return MPI_SUCCESS;
}


int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req)
{
    req->ptr = *ptr;

    if(HMPI_Comm_local(comm, rank)) {
        //Owner passing!
        MPI_Isend(&req->ptr, sizeof(void*), MPI_BYTE,
                rank, tag, comm, (MPI_Request*)req);
        req->do_free = 0;
    } else {
        //Remote node, use MPI
        MPI_Isend(*ptr, count, datatype, rank, tag, comm, (MPI_Request*)req);
        req->do_free = 1;
    }

    *ptr = NULL;
    return MPI_SUCCESS;
}


int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req)
{
    if(HMPI_Comm_local(comm, rank)) {
        //Owner passing!
        MPI_Irecv(ptr, sizeof(void*), MPI_BYTE,
                rank, tag, comm, (MPI_Request*)req);
    } else {
        //Remote node, use MPI
        int type_size;
        MPI_Type_size(datatype, &type_size);

        OPI_Alloc(ptr, type_size * count);

        MPI_Irecv(*ptr, count, datatype, rank, tag, comm, (MPI_Request*)req);
    }

    req->do_free = 0;
    return MPI_SUCCESS;
}


int OPI_Test(OPI_Request *request, int *flag, HMPI_Status *status)
{
    int ret = HMPI_Test((HMPI_Request*)request, flag, status);
    if(flag && request->do_free == 1) {
        OPI_Free(&request->ptr);
    }

    return ret;
}

int OPI_Testall(int count, OPI_Request *requests, int* flag, HMPI_Status *statuses)
{
    for(int i = 0; i < count; i++) {
        int ret = OPI_Test(&requests[i], flag, &statuses[i]);
        if(*flag == 0) {
            return ret;
        }
    }

    return MPI_SUCCESS;
}

int OPI_Wait(OPI_Request *request, HMPI_Status *status)
{
    int ret = HMPI_Wait((MPI_Request*)request, status);
    if(request->do_free == 1) {
        OPI_Free(&request->ptr);
    }

    return ret;
}

int OPI_Waitall(int count, OPI_Request* requests, HMPI_Status* statuses)
{
    //TODO - maybe this could deadlock?
    for(int i = 0; i < count; i++) {
        OPI_Wait(&requests[i], &statuses[i]);
    }

    return MPI_SUCCESS;
}

int OPI_Waitany(int count, OPI_Request* requests, int* index, HMPI_Status *status)
{
    int not_null = 0;
    int flag;

    do {
        for(int i = 0; i < count; i++) {
            if(requests[i].req == HMPI_REQUEST_NULL) {
                continue;
            }

            not_null = 1;

            OPI_Test(&requests[i], &flag, status);

            if(flag) {
                *index = i;
                return MPI_SUCCESS;
            }
        }
    } while(not_null);

    *index = MPI_UNDEFINED;
    return MPI_SUCCESS;
}

