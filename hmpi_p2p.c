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
#ifdef MPI
#define MPI_FOO
#undef MPI
#endif
#define HMPI_INTERNAL 
#include "hmpi.h"
#ifdef MPI_FOO
#define MPI
#else
#undef MPI
#endif

#include "profile.h"


//Block size to use when using the accelerated sender-receiver copy.
#ifdef __bg__
#define BLOCK_SIZE_ONE 16384
//#define BLOCK_SIZE_ONE 4096
#define BLOCK_SIZE_TWO 65536
#else
#define BLOCK_SIZE_ONE 4096
#define BLOCK_SIZE_TWO 12288
#endif

#define MIN_COPY_SIZE 4096

#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "error.h"
#include "lock.h"


#include "profile.h"

#ifdef FULL_PROFILE
#define FULL_PROFILE_INIT() PROFILE_INIT()
#define FULL_PROFILE_TIMER(v) PROFILE_TIMER(v)
#define FULL_PROFILE_TIMER_EXTERN(v) PROFILE_TIMER_EXTERN(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_TIMER_RESET(v) PROFILE_TIMER_RESET(v)
#define FULL_PROFILE_TIMER_SHOW(v) PROFILE_TIMER_SHOW(v)
#else
#define FULL_PROFILE_INIT()
#define FULL_PROFILE_TIMER(v)
#define FULL_PROFILE_TIMER_EXTERN(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_TIMER_RESET(v)
#define FULL_PROFILE_TIMER_SHOW(v)
#endif

FULL_PROFILE_TIMER_EXTERN(MPI_Other);
FULL_PROFILE_TIMER_EXTERN(MPI_Send);
FULL_PROFILE_TIMER_EXTERN(MPI_Recv);
FULL_PROFILE_TIMER_EXTERN(MPI_Isend);
FULL_PROFILE_TIMER_EXTERN(MPI_Irecv);
FULL_PROFILE_TIMER_EXTERN(MPI_Test);
FULL_PROFILE_TIMER_EXTERN(MPI_Testall);
FULL_PROFILE_TIMER_EXTERN(MPI_Wait);
FULL_PROFILE_TIMER_EXTERN(MPI_Waitall);
FULL_PROFILE_TIMER_EXTERN(MPI_Waitany);
FULL_PROFILE_TIMER_EXTERN(MPI_Iprobe);

FULL_PROFILE_TIMER_EXTERN(MPI_Barrier);
FULL_PROFILE_TIMER_EXTERN(MPI_Reduce);
FULL_PROFILE_TIMER_EXTERN(MPI_Allreduce);
FULL_PROFILE_TIMER_EXTERN(MPI_Scan);
FULL_PROFILE_TIMER_EXTERN(MPI_Bcast);
FULL_PROFILE_TIMER_EXTERN(MPI_Scatter);
FULL_PROFILE_TIMER_EXTERN(MPI_Gather);
FULL_PROFILE_TIMER_EXTERN(MPI_Gatherv);
FULL_PROFILE_TIMER_EXTERN(MPI_Allgather);
FULL_PROFILE_TIMER_EXTERN(MPI_Allgatherv);
FULL_PROFILE_TIMER_EXTERN(MPI_Alltoall);

#ifdef ENABLE_OPI
FULL_PROFILE_TIMER_EXTERN(OPI_Alloc);
FULL_PROFILE_TIMER_EXTERN(OPI_Free);
FULL_PROFILE_TIMER_EXTERN(OPI_Give);
FULL_PROFILE_TIMER_EXTERN(OPI_Take);
#endif


//Statistics on message size, counts.
#ifdef HMPI_STATS
#define HMPI_STATS_INIT() PROFILE_INIT()
#define HMPI_STATS_COUNTER(v) PROFILE_COUNTER(v)
#define HMPI_STATS_COUNTER_EXTERN(v) PROFILE_COUNTER_EXTERN(v)
#define HMPI_STATS_ACCUMULATE(v, c) PROFILE_ACCUMULATE(v, c)
#define HMPI_STATS_COUNTER_RESET(v) PROFILE_COUNTER_RESET(v)
#define HMPI_STATS_COUNTER_SHOW(v) PROFILE_COUNTER_SHOW(v)
#else
#define HMPI_STATS_INIT()
#define HMPI_STATS_COUNTER(v)
#define HMPI_STATS_COUNTER_EXTERN(v)
#define HMPI_STATS_ACCUMULATE(v, c)
#define HMPI_STATS_COUNTER_RESET(v)
#define HMPI_STATS_COUNTER_SHOW(v)
#endif

HMPI_STATS_COUNTER_EXTERN(send_size);
HMPI_STATS_COUNTER_EXTERN(send_local);
HMPI_STATS_COUNTER_EXTERN(send_remote);
HMPI_STATS_COUNTER_EXTERN(send_imm);
HMPI_STATS_COUNTER_EXTERN(send_syn);
HMPI_STATS_COUNTER_EXTERN(recv_syn);
HMPI_STATS_COUNTER_EXTERN(recv_mem);
HMPI_STATS_COUNTER_EXTERN(recv_anysrc);




// Debugging functionality

#if 0
#include <execinfo.h>

static void show_backtrace(void) __attribute__((unused));

static void show_backtrace(void)
{
    void* buffer[64];
    int nptrs;

    nptrs = backtrace(buffer, 64);
    backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO);
    fflush(stdout);
}
#endif


#ifdef HMPI_CHECKSUM
uint32_t compute_csum(uint8_t* buf, size_t len)
{
    uint32_t csum = 0;

    for(size_t i = 0; i < len; i++) {
        csum = csum * 31 + buf[i];
    }

    return csum;
}
#endif


#ifdef HMPI_LOGCALLS
extern int g_log_fd;

#define LOG_MPI_CALL log_mpi_call
void log_mpi_call(char* fmt, ...);
#else
#define LOG_MPI_CALL(fmt, ...)
#endif


// Internal global structures

//Each thread has a list of send and receive requests.
//The receive requests are managed privately by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them in match_recv().

HMPI_Item g_recv_reqs_head = {NULL};
HMPI_Item* g_recv_reqs_tail = NULL;


#ifdef USE_MCS
mcs_qnode_t* g_lock_q;                   //Q node for lock.
#endif
HMPI_Request_list* g_send_reqs = NULL;   //Shared: Senders add sends here
HMPI_Request_list* g_tl_my_send_reqs;    //Shortcut to my global send Q
HMPI_Request_list g_tl_send_reqs;        //Receiver-local send Q

//Pool of unused reqs to save malloc time.
static HMPI_Item* g_free_reqs = NULL;

#define get_reqstat(req) req->stat

static inline void update_reqstat(HMPI_Request req, int stat) {
#ifdef __bg__
    __lwsync();
    //FENCE();
#endif
    req->stat = stat;
}


//TODO - Maybe send reqs should be allocated on the receiver.  How?
HMPI_Request acquire_req(void)
{
    HMPI_Item* item = g_free_reqs;

    //Malloc a new req only if none are in the pool.
    if(item == NULL) {
        HMPI_Request req = (HMPI_Request)MALLOC(HMPI_Request_info, 1);
        req->match = 0;
        req->do_free = DO_NOT_FREE;
        return req;
    } else {
        g_free_reqs = item->next;
        return (HMPI_Request)item;
    }
}


static inline void release_req(HMPI_Request req)
{
    //Return a req to the pool -- once allocated, a req is never freed.
    HMPI_Item* item = (HMPI_Item*)req;

    switch(req->do_free) {
        case DO_FREE:
            free(req->buf);
            req->do_free = DO_NOT_FREE;
            break;
#ifdef ENABLE_OPI
        case DO_OPI_FREE:
            OPI_Free(&req->buf);
            req->do_free = DO_NOT_FREE;
            break;
#endif
        default:
            break;
    }


    item->next = g_free_reqs;
    g_free_reqs = item;
}


static inline void add_send_req(HMPI_Request_list* req_list,
                                HMPI_Request req) {
    //Insert req at tail.
    HMPI_Item* item = (HMPI_Item*)req;

#ifdef DEBUG
    item->next = NULL;
#endif

#ifdef USE_MCS
    mcs_qnode_t* q = g_lock_q;  //Could fold this back into macros..
    __LOCK_ACQUIRE(&req_list->lock, q);
#else
    LOCK_ACQUIRE(&req_list->lock);
#endif

    //NOTE -- On BG/Q other cores can see these two writes in a different order
    // than what is written here.  Thus update_send_reqs() needs to be careful
    // to acquire the lock before relying on some ordering here.
    req_list->tail->next = item;
    req_list->tail = item;

#ifdef USE_MCS
    __LOCK_RELEASE(&req_list->lock, q);
#else
    LOCK_RELEASE(&req_list->lock);
#endif
}


static inline void remove_send_req(HMPI_Request_list* req_list,
                                   HMPI_Item* prev, const HMPI_Item* cur)
{
    //Since we only remove from the receiver-local send Q, there is no need for
    //locking.

    if(cur->next == NULL) {
        prev->next = NULL;
        req_list->tail = prev;
    } else {
        prev->next = cur->next;
    }
}


static inline void update_send_reqs(HMPI_Request_list* local_list, HMPI_Request_list* shared_list)
{
    if(shared_list->tail != &shared_list->head) {
        HMPI_Item* tail;
        //NOTE - we can safely compare head/tail here, but we need the lock to
        // do more on BQ/Q.  On x86 it's safe to grab the first node in the
        // shared Q, But on BG/Q we can see the two writes in add_send_req() in
        // reverse order.  Thus we need to acquire the lock, ensuring that we
        // see both writes before grabbing head.next in the next statement.  If
        // we move the lock after that statement, it is possible to see the
        // updated tail in add_send_req(), come through and grab the shared
        // head.next before we see the updated head.next.

#ifdef __x86_64__
        //For x86, this statement is safe outside the lock.
        // See comments above and non-x86 statement below.
        // The branch ensures at least one node.  Senders only add at the tail,
        // so head.next won't change out from under us.
        local_list->tail->next = shared_list->head.next;
#endif

#ifdef USE_MCS
        mcs_qnode_t* q = g_lock_q; //Could fold this back into macros..;
        __LOCK_ACQUIRE(&shared_list->lock, q);
#else
        LOCK_ACQUIRE(&shared_list->lock);
#endif

#ifndef __x86_64__ //NOT x86
        //For non x86 (eg PPC) this statement needs to be protected.
        // See comments and x86 statement above.
        local_list->tail->next = shared_list->head.next;
#endif

        tail = shared_list->tail;
        shared_list->tail = &shared_list->head;

#ifdef USE_MCS
        __LOCK_RELEASE(&shared_list->lock, q);
#else
        LOCK_RELEASE(&shared_list->lock);
#endif

        //This is safe, the pointers involved here are now only accessible by
        // this core.
        local_list->tail = tail;
        tail->next = NULL;
    }
}



static inline void add_recv_req(HMPI_Request req) {
    HMPI_Item* item = (HMPI_Item*)req;

    //Add at tail to ensure matching occurs in order.
    item->next = NULL;
    g_recv_reqs_tail->next = item;
    g_recv_reqs_tail = item;

}


static inline void remove_recv_req(HMPI_Item* prev, const HMPI_Item* cur) {
    if(cur->next == NULL) {
        g_recv_reqs_tail = prev;
        prev->next = NULL;
    } else {
        prev->next = cur->next;
    }
}


//Match for receives that are *NOT* ANY_SOURCE
static inline HMPI_Request match_recv(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    int proc = recv_req->proc;
    int tag = recv_req->tag;
    int context = recv_req->context;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

#ifdef ENABLE_OPI
        if(req->type == HMPI_SEND &&
                req->proc == proc &&
                (req->tag == tag || tag == MPI_ANY_TAG) &&
                req->context == context) {
#else
        if(req->proc == proc &&
                (req->tag == tag || tag == MPI_ANY_TAG) &&
                req->context == context) {
#endif
            remove_send_req(req_list, prev, cur);

            //recv_req->proc = req->proc; //Not necessary, no ANY_SRC
            recv_req->tag = req->tag;

            //WARNING("%d match_recv req %p proc %d tag %d ctx %d send req %p\n",
            //        HMPI_COMM_WORLD->comm_rank, recv_req, proc, tag, context, req);
            return req;
        }
    }

    return HMPI_REQUEST_NULL;
}


//Match for takes
#ifdef ENABLE_OPI
static inline HMPI_Request match_take(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    int proc = recv_req->proc;
    int tag = recv_req->tag;
    int context = recv_req->context;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

        if(req->type == OPI_GIVE &&
                req->proc == proc &&
                (req->tag == tag || tag == MPI_ANY_TAG) &&
                req->context == context) {
            remove_send_req(req_list, prev, cur);

            //recv_req->proc = req->proc; //Not necessary, no ANY_SRC
            recv_req->tag = req->tag;
            //printf("%d matched recv req %d proc %d tag %d to send req %p\n",
            //        g_hmpi_rank, recv_req, proc, tag, req);
            return req;
        }
    }

    return HMPI_REQUEST_NULL;
}
#endif


//Match for receives with ANY_SOURCE.
//Three things can happen here:
// No matching send is found:
//  return HMPI_REQUEST_NULL, req->u.req != MPI_REQUEST_NULL
// Matching MPI (inter-node) send is found:
//  req->u.req == MPI_REQUEST_NULL and return HMPI_REQUEST_NULL
// Matching local send is found:
//  req->u.req == MPI_REQUEST_NULL and return send_req
//Callers should check return value for local matches, and req->u.req for
// inter-node matches.
static inline HMPI_Request match_recv_any(HMPI_Request_list* req_list, HMPI_Request recv_req)
{
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    int tag = recv_req->tag;
    int context = recv_req->context;

    for(prev = &req_list->head, cur = prev->next;
            cur != NULL; prev = cur, cur = cur->next) {
        req = (HMPI_Request)cur;

        if((req->tag == tag || tag == MPI_ANY_TAG) &&
                context == req->context) {
            MPI_Status status;
            int flag;

            //Matched a local message -- try to cancel the MPI-level receive.
            //If not successful, we throw out the local match and use what MPI
            // gave us.  If cancel succeeds, we use the local match.
            MPI_Cancel(&recv_req->u.req);
            MPI_Wait(&recv_req->u.req, &status);
            MPI_Test_cancelled(&status, &flag);
            if(!flag) {
                //Not cancelled - use the inter-node message from MPI.
                int count;
                int type_size;

                MPI_Get_count(&status, recv_req->datatype, &count);
                MPI_Type_size(recv_req->datatype, &type_size);

                recv_req->proc = status.MPI_SOURCE;
                recv_req->tag = status.MPI_TAG;
                recv_req->size = count * type_size;
                update_reqstat(recv_req, HMPI_REQ_COMPLETE);

                //Indicate no local req was matched.
                return HMPI_REQUEST_NULL;
            }

            //Cancel succeeded, use the local send match.
            remove_send_req(req_list, prev, cur);

            recv_req->proc = req->proc;
            recv_req->tag = req->tag;
            return req;
        }
    }

    return HMPI_REQUEST_NULL;
}


static /*inline*/ int match_probe(int source, int tag, int context, HMPI_Request* send_req) {
    HMPI_Item* cur;
    HMPI_Request req;
    HMPI_Request_list* req_list = &g_tl_send_reqs;

    update_send_reqs(req_list, g_tl_my_send_reqs);

    for(cur = req_list->head.next; cur != NULL; cur = cur->next) {
        req = (HMPI_Request)cur;

        //The send request can't have ANY_SOURCE or ANY_TAG,
        // so don't check for that.
        if((req->proc == source || source == MPI_ANY_SOURCE) &&
                (req->tag == tag || tag == MPI_ANY_TAG) &&
                req->context == context) {
            //We don't want to do anything other than return the send req.
            *send_req = req;
            return 1;
        }
    }

    return 0;
}


//We assume req->type == HMPI_SEND
static inline int HMPI_Progress_send(const HMPI_Request send_req)
{
    if(get_reqstat(send_req) == HMPI_REQ_COMPLETE) {
        return HMPI_REQ_COMPLETE;
    }

    //Write blocks on this send req if receiver has matched it.
    //If mesage is short, receiver won't bother clearing the match lock, and
    // instead just does the copy and marks completion.
    if(send_req->match &&
            CAS_T_BOOL(volatile uint32_t, &send_req->match, (uint32_t)1, (uint32_t)0)) {
        HMPI_Request recv_req = (HMPI_Request)send_req->u.match_req;
        uintptr_t rbuf = (uintptr_t)recv_req->buf;

        //Receiver does any size sanity checking.
        size_t size = send_req->size;
        size_t block_size = (size_t)BLOCK_SIZE_ONE;
        if(size >= (size_t)BLOCK_SIZE_TWO << 1) {
            block_size = (size_t)BLOCK_SIZE_TWO;
        }

        uintptr_t sbuf = (uintptr_t)send_req->buf;
        volatile size_t* offsetptr = &recv_req->u.offset;
        size_t offset;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD64(offsetptr, block_size)) < size) {
            size_t left = size - offset;
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < block_size ? left : block_size));
        }

        //Signal that the sender is done copying.
        //Possible for the receiver to still be copying here.
#ifdef __bg__
        STORE_FENCE();
#endif
        send_req->match = 1;

        //Receiver will set completion soon, wait rather than running off.
        //TODO: test the performance with and without this on say AMG.
        while(get_reqstat(send_req) != HMPI_REQ_COMPLETE);

        HMPI_STATS_ACCUMULATE(send_syn, 1);
        return HMPI_REQ_COMPLETE;
    }

#if 0
    if(send_req->match &&
            CAS_T_BOOL(volatile uint32_t, &send_req->match, (uint32_t)1, (uint32_t)0)) {
        HMPI_Request recv_req = (HMPI_Request)send_req->u.match_req;
        uintptr_t sbuf = (uintptr_t)send_req->buf;
        uintptr_t rbuf = (uintptr_t)recv_req->buf;

        size_t size = recv_req->size;
        size_t offset;
        size_t len;

        while(1) {
            //LOCK
            while(__sync_lock_test_and_set(&recv_req->lock, 1) != 0);
            offset = recv_req->u.offset;
            len = (size - offset) >> 1; //Half of remaining length

            recv_req->u.offset = offset + len;
            //UNLOCK
            __sync_lock_release(&recv_req->lock);

            if(size - offset <= MIN_COPY_SIZE) {
                break;
            }


            //WARNING("%d send copy offset %ld len %ld size %ld",
            //        HMPI_COMM_WORLD->comm_rank, offset, len, size);
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset), len);
        }

        //Signal that the sender is done copying.
        //Possible for the receiver to still be copying here.
#ifdef __bg__
        STORE_FENCE();
#endif
        send_req->match = 1;

        //Receiver will set completion soon, wait rather than running off.
        //TODO: test the performance with and without this on say AMG.
        while(get_reqstat(send_req) != HMPI_REQ_COMPLETE);

        HMPI_STATS_ACCUMULATE(send_syn, 1);
        return HMPI_REQ_COMPLETE;
    }
#endif

    return HMPI_REQ_ACTIVE;
}


//For req->type == HMPI_RECV
static inline void HMPI_Complete_recv(HMPI_Request recv_req, HMPI_Request send_req)
{
    size_t send_size = send_req->size;
    size_t size = recv_req->size;

#ifdef DEBUG
    if(unlikely(send_size > size)) {
        ERROR("%d recv message from %d of size %ld truncated to %ld",
                HMPI_COMM_WORLD->comm_rank, send_req->proc, send_size, size);
    }
#endif

    if(send_size < size) {
        //Adjust receive size if the incoming message is smaller.
        //WARNING("%d recv from %d is %d bytes, recv is %d bytes",
        //        HMPI_COMM_WORLD->comm_rank, send_req->proc, send_size, size);
        recv_req->size = send_size;
        size = send_size;
    }

    uintptr_t rbuf = (uintptr_t)recv_req->buf;
    uintptr_t sbuf = (uintptr_t)send_req->buf;

    if(size < (size_t)BLOCK_SIZE_ONE << 1 || !IS_SM_BUF((void*)rbuf)) {
        //Use memcpy for small messages, and when the user's receive buf isn't
        // in the SM region.  On the recv path, buf is always the user's recv
        // buf, whether it's an SM region or not.
        memcpy((void*)rbuf, (void*)sbuf, size);
        HMPI_STATS_ACCUMULATE(recv_mem, 1);
    } else {
        //The setting of send_req->match_req signals to sender that they can
        // start doing copying as well, if they are testing the req.

        recv_req->u.offset = (size_t)0;
        send_req->u.match_req = recv_req;
        STORE_FENCE();
        send_req->match = 1;

        size_t block_size = BLOCK_SIZE_ONE;
        if(size >= (size_t)BLOCK_SIZE_TWO << 1) {
            block_size = (size_t)BLOCK_SIZE_TWO;
        }

        volatile size_t* offsetptr = &recv_req->u.offset;
        size_t offset = 0;

        //length to copy is min of len - offset and BLOCK_SIZE
        while((offset = FETCH_ADD64(offsetptr, block_size)) < size) {
            size_t left = size - offset;

            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset),
                    (left < block_size ? left : block_size));
        }

        //Wait if the sender is copying.
        while(!CAS_T_BOOL(volatile uint32_t, &send_req->match, 1, 0));
        HMPI_STATS_ACCUMULATE(recv_syn, 1);
    }

#if 0
    if(size < (size_t)MIN_COPY_SIZE || !IS_SM_BUF((void*)rbuf)) {
        //Use memcpy for small messages, and when the user's receive buf isn't
        // in the SM region.  On the recv path, buf is always the user's recv
        // buf, whether it's an SM region or not.
        memcpy((void*)rbuf, (void*)sbuf, size);
        HMPI_STATS_ACCUMULATE(recv_mem, 1);
    } else {
        //Use the offset on the send req.
        send_req->u.match_req = recv_req;

        //Maybe this should be on recv_req?
        //Setting nonzero signals to sender that they can copy.
        size_t offset = 0;
        size_t len = recv_req->u.offset = (size_t)size >> 1; 

        STORE_FENCE();
        send_req->match = 1;

        //Make sure this proc has up to date data from sender (ie msg data)
        //TODO - necessary? check on BGQ
        LOAD_FENCE();

        do {
            //WARNING("%d loop copy offset %ld len %ld size %ld",
            //        HMPI_COMM_WORLD->comm_rank, offset, len, size);
            memcpy((void*)(rbuf + offset), (void*)(sbuf + offset), len);

            //LOCK
            while(__sync_lock_test_and_set(&recv_req->lock, 1) != 0);
            offset = recv_req->u.offset;
            len = (size - offset) >> 1; //Half of remaining length

            recv_req->u.offset = offset + len;
            //UNLOCK
            __sync_lock_release(&recv_req->lock);
        } while(size - offset > MIN_COPY_SIZE);

        //WARNING("%d finish copy offset %ld len %ld  (%ld) size %ld",
        //            HMPI_COMM_WORLD->comm_rank, offset, len, size - offset, size);
        memcpy((void*)(rbuf + offset), (void*)(sbuf + offset), size - offset);


        //Wait if the sender is copying.
        while(!CAS_T_BOOL(volatile uint32_t, &send_req->match, 1, 0));
        HMPI_STATS_ACCUMULATE(recv_syn, 1);
    }
#endif

#ifdef HMPI_CHECKSUM
#warning "csum enabled"
    uint32_t recv_csum = compute_csum(recv_req->buf, size);
    if(recv_csum != send_req->csum) {
        printf("%d csum %d mismatched sender %d csum %d\n",
                HMPI_COMM_WORLD->comm_rank, recv_csum, send_req->proc, send_req->csum);
    }
#endif

#ifdef DEBUG
    printf("%d completed local-level RECV buf %p size %lu source %d tag %d\n",
            HMPI_COMM_WORLD->comm_rank, recv_req->buf, recv_req->size, recv_req->proc, recv_req->tag);
    printf("%d completed local-level SEND buf %p size %lu dest %d tag %d\n",
            send_req->proc, send_req->buf, send_req->size, HMPI_COMM_WORLD->comm_rank, send_req->tag);
#endif

    //Mark send and receive requests done
    update_reqstat(send_req, HMPI_REQ_COMPLETE);
    update_reqstat(recv_req, HMPI_REQ_COMPLETE);
}


#ifdef ENABLE_OPI
//For req->type == OPI_TAKE
static inline void HMPI_Complete_take(HMPI_Request recv_req, HMPI_Request send_req)
{
#if DEBUG
    if(unlikely(send_req->size > recv_req->size)) {
        ERROR("%d recv message from %d of size %ld truncated to %ld\n",
                HMPI_COMM_WORLD->comm_rank, send_req->proc, send_size, size);
    }
#endif

    recv_req->size = send_req->size;

#if 0
    //TODO - check this out -- with immediate, the send side doesn't have to
    // alloc/free, though the receive side still does.
    if(size < 256) {
        //Size is too small - just memcpy the buffer instead of doing OP.
        //But, I have to alloc and free.. blah
        OPI_Alloc(recv_req->buf, size);
        memcpy(*((void**)recv_req->buf), send_req->buf, size);
        OPI_Free(&send_req->buf);
    } else {
#endif
        //Easy OP
        *((void**)recv_req->buf) = send_req->buf;
    //}

    //Mark send and receive requests done
    update_reqstat(send_req, HMPI_REQ_COMPLETE);
    update_reqstat(recv_req, HMPI_REQ_COMPLETE);
}
#endif


//For req->type == MPI_SEND || req->type == MPI_RECV
// Not HMPI_RECV_ANY_SOURCE!
static int HMPI_Progress_mpi(HMPI_Request req)
{
    int flag;
    MPI_Status status;

#if DEBUG
    if(req->ir.req == MPI_REQUEST_NULL) {
        ERROR("%d Progress_mpi on null request!", HMPI_COMM_WORLD->comm_rank);
    }
#endif

    MPI_Test(&req->ir.req, &flag, &status);

    if(flag) {
        //Update status
        int count;
        int type_size;

        //This isn't necessary for sends:  message size, proc, and tag are
        // already known, so don't query for them.
        if(req->type == MPI_RECV) {
            MPI_Get_count(&status, req->datatype, &count);
            MPI_Type_size(req->datatype, &type_size);

            req->tag = status.MPI_TAG;

            //This cast is costly but important for msgs >2/4gb
            req->size = (size_t)count * (size_t)type_size;
        }

        //Not necessary: req will always be completed and free'd upon return.
        //update_reqstat(req, HMPI_REQ_COMPLETE);
        return HMPI_REQ_COMPLETE;
    }

    return HMPI_REQ_ACTIVE;
}


//For req->type == HMPI_RECV_ANY_SOURCE
static int HMPI_Progress_mpi_any(HMPI_Request req)
{
    int flag;
    MPI_Status status;

#if DEBUG
    if(req->u.req == MPI_REQUEST_NULL) {
        ERROR("%d Progress_mpi on null request!", HMPI_COMM_WORLD->comm_rank);
    }
#endif

    MPI_Test(&req->u.req, &flag, &status);

    if(flag) {
        //Update status
        int count;
        int type_size;

        MPI_Get_count(&status, req->datatype, &count);
        MPI_Type_size(req->datatype, &type_size);

        req->proc = status.MPI_SOURCE;

        req->tag = status.MPI_TAG;
        req->size = count * type_size;

        update_reqstat(req, HMPI_REQ_COMPLETE);
        return HMPI_REQ_COMPLETE;
    }

    return HMPI_REQ_ACTIVE;
}


//#define HMPI_PRINTQUEUE 1
#ifdef HMPI_PRINTQUEUE
#include <time.h>

void printqueue(HMPI_Item* recv_reqs_head, HMPI_Request_list* local_list)
{
    static time_t last_time = 0;
    time_t cur_time = time(NULL);

    //Don't print more than once every 3 seconds.
    if(cur_time - last_time < 3) {
        return;
    }

    last_time = cur_time;

    HMPI_Item* cur;

    WARNING("%d printing reqs", HMPI_COMM_WORLD->comm_rank);

    //Print the receive requests, if any.
    if(recv_reqs_head->next == NULL) {
        WARNING("%d no recv reqs", HMPI_COMM_WORLD->comm_rank);
    } else {
        for(cur = recv_reqs_head->next; cur != NULL; cur = cur->next) {
            HMPI_Request req = (HMPI_Request)cur;

            WARNING("%d recv req proc %d tag %d context %d size %ld",
                    HMPI_COMM_WORLD->comm_rank, req->proc,
                    req->tag, req->context, req->size);
        }
    }

    //Print the incoming send requests, if any.
    if(local_list->head.next == NULL) {
        WARNING("%d no send reqs", HMPI_COMM_WORLD->comm_rank);
    } else {
        for(cur = local_list->head.next; cur != NULL; cur = cur->next) {
            HMPI_Request req = (HMPI_Request)cur;

            WARNING("%d send req proc %d tag %d context %d size %ld",
                    HMPI_COMM_WORLD->comm_rank, req->proc,
                    req->tag, req->context, req->size);
        }
    }
}
#endif


//Progress local receive requests.
//TODO - this could benefit from BGQ nops.
static void HMPI_Progress(HMPI_Item* recv_reqs_head,
        HMPI_Request_list* local_list, HMPI_Request_list* shared_list) {
    HMPI_Item* cur;
    HMPI_Item* prev;
    HMPI_Request req;

    //TODO - poll MPI here?

    update_send_reqs(local_list, shared_list);

#ifdef HMPI_PRINTQUEUE
    printqueue(recv_reqs_head, local_list);
#endif

    //Progress receive requests.
    //We remove items from the list, but they are still valid; nothing in this
    //function will free or modify a req.  So, it's safe to do cur = cur->next.
    //Note the careful updating of prev; we need to leave it alone on iterations
    //where cur is matched successfully and only update it otherwise.
    // This prevents the recv_reqs list from getting corrupted due to a bad
    // prev pointer.
    for(prev = recv_reqs_head, cur = prev->next;
            cur != NULL; cur = cur->next) {
        req = (HMPI_Request)cur;

        if(likely(req->type == HMPI_RECV)) {
            HMPI_Request send_req = match_recv(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            }
#ifdef ENABLE_OPI
        } else if(req->type == OPI_TAKE) {
            HMPI_Request send_req = match_take(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_take(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            }
#endif
        } else { //req->type == HMPI_RECV_ANY_SOURCE
            //First, check for a local match.
            // match_recv_any() may complete the MPI-level receive here.
            // In that case, it returns REQUEST_NULL indicating no match,
            // but the request will be in completed state.
            HMPI_Request send_req = match_recv_any(local_list, req);
            if(send_req != HMPI_REQUEST_NULL) {
                HMPI_Complete_recv(req, send_req);

                remove_recv_req(prev, cur);
                continue; //Whenever we remove a req, dont update prev
            } else if(req->u.req == MPI_REQUEST_NULL) {
                //This means match_recv_any tried to cancel the MPI recv and
                // failed, so we completed the MPI request.
                remove_recv_req(prev, cur);
                continue;
            } else {
                //Check MPI-level completion.
                if(HMPI_Progress_mpi_any(req)) {
                    remove_recv_req(prev, cur);
                    continue;
                }
            }
        }

        //Update prev -- we only do this if cur wasn't matched.
        prev = cur;
    }

    //TODO - probe/progress MPI here if nothing was completed?
}



//Internal function to test completion of a request.
//Does not call progress, but may progress local sends, or underlying MPI.
//TODO - can i remove the MPI progress, too?
// Not easily -- maybe I should maintain a list of MPI reqs.
// I can have a general MPI progress where I call Testsome and complete them.
// Then in places like this, I can just check completion of the request by
// looking at the state.
// Should save calling down into MPI and progressing many times.
//Returns 1 if request was completed, 0 otherwise.
static int HMPI_Test_internal(HMPI_Request* request, HMPI_Status* status)
{
    HMPI_Request req = *request;

    if(unlikely(req == HMPI_REQUEST_NULL)) {
        if(status != HMPI_STATUS_IGNORE) {
            //Make Get_count return 0 count
            status->size = 0;
        }

        return 1;
    } 
   
    int state = get_reqstat(req);

    if(state != HMPI_REQ_COMPLETE) {
        //Poll local sends and MPI for completion.
        if(req->type == HMPI_SEND) {
            state = HMPI_Progress_send(req);
        } else if(req->type & (MPI_SEND | MPI_RECV)) {
            state = HMPI_Progress_mpi(req);
        }
    }

    //Careful here -- the above branches can result in flipping the state over
    //to COMPLETE, so an 'else' is not appropriate.

    if(state == HMPI_REQ_COMPLETE) {
        if(status != HMPI_STATUS_IGNORE) {
            status->size = req->size;
            status->MPI_SOURCE = req->proc;
            status->MPI_TAG = req->tag;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            status->MPI_ERROR = MPI_SUCCESS;
        }

        release_req(req);
        *request = HMPI_REQUEST_NULL;
    }

    return state;
}


int HMPI_Test(HMPI_Request *request, int *flag, HMPI_Status *status)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Test);
#ifdef HMPI_LOGCALLS
    HMPI_Request req = *request;

    LOG_MPI_CALL("MPI_Test(request=%p, flag=%p, status=%p) type=%d",
            request, flag, status, req->type);
#endif

    HMPI_Progress(&g_recv_reqs_head, &g_tl_send_reqs, g_tl_my_send_reqs);

    //HMPI req state is chosen to match MPI test flags.
    *flag = HMPI_Test_internal(request, status);

    FULL_PROFILE_STOP(MPI_Test);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Testall(int count, HMPI_Request *requests, int* flag, HMPI_Status *statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Testall);
    LOG_MPI_CALL(
            "MPI_Testall(count=%d, requests=%p, flag=%p, statuses=%p)",
            count, requests, flag, statuses);

    HMPI_Progress(&g_recv_reqs_head, &g_tl_send_reqs, g_tl_my_send_reqs);
    
    *flag = 1;

    //Return as soon as any one request isn't complete.
    //TODO - poll each request anyway, to try and progress?
    for(int i = 0; i < count && *flag; i++) {
        if(requests[i] == HMPI_REQUEST_NULL) {
            continue;
        } else {
            HMPI_Status* status;

            if(statuses == HMPI_STATUSES_IGNORE) {
                status = HMPI_STATUS_IGNORE;
            } else {
                status = &statuses[i];
            }

            if(!HMPI_Test_internal(&requests[i], status)) {
                *flag = 0;
                break;
            }
        }
    }

    FULL_PROFILE_STOP(MPI_Testall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Testsome(int incount, HMPI_Request* array_of_requests, int *outcount,
                  int* array_of_indices, HMPI_Status* array_of_statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Testall);
    LOG_MPI_CALL("MPI_Testsome(incount=%d, array_of_requests=%p, outcount=%p, "
                 "array_of_indices=%p, array_of_statuses=%p)",
                 incount, array_of_requests, outcount, array_of_indices,
                 array_of_statuses);

    HMPI_Progress(&g_recv_reqs_head, &g_tl_send_reqs, g_tl_my_send_reqs);

    int count = 0;
    int flag;

    for(int i = 0; i < incount; i++) {
        flag = HMPI_Test_internal(&array_of_requests[i],
                &array_of_statuses[count]);
        if(flag) {
            array_of_indices[count] = i;
            count += 1;
        }
    }

    *outcount = count;

    FULL_PROFILE_STOP(MPI_Testall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Wait(HMPI_Request *request, HMPI_Status *status)
{
#if 0
    HMPI_Request req = *request;
    MPI_Wait(&req->ir.req, MPI_STATUS_IGNORE);
    release_req(req);
    *request = HMPI_REQUEST_NULL;
    return MPI_SUCCESS;
#endif

    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Wait);

    HMPI_Request req = *request;

    LOG_MPI_CALL("MPI_Wait(request=%p, statuses=%p) type=%d",
            request, status, req->type);


    if(unlikely(req == HMPI_REQUEST_NULL)) {
        if(status != HMPI_STATUS_IGNORE) {
            //Make Get_count return 0 count
            status->size = 0;
        }

        FULL_PROFILE_STOP(MPI_Wait);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }

    HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;

    if(req->type & (MPI_RECV | MPI_SEND)) {
//        while(HMPI_Progress_mpi(req) != HMPI_REQ_COMPLETE) {
//            HMPI_Progress(recv_reqs_head, local_list, shared_list);
//        }
        int flag;
        MPI_Status st;
        MPI_Status* p_st = (status == HMPI_STATUS_IGNORE ? MPI_STATUS_IGNORE : &st);

        do {
            HMPI_Progress(recv_reqs_head, local_list, shared_list);
            MPI_Test(&req->ir.req, &flag, p_st);
        } while(flag == 0);

        if(req->type == MPI_RECV && status != HMPI_STATUS_IGNORE) {
            //Update status
            int count;
            int type_size;

            MPI_Get_count(&st, req->datatype, &count);
            MPI_Type_size(req->datatype, &type_size);

#if 0
            req->tag = st.MPI_TAG;
            req->size = (size_t)count * (size_t)type_size;
#endif

            status->size = (size_t)count * (size_t)type_size;
            status->MPI_SOURCE = req->proc;
            status->MPI_TAG = st.MPI_TAG;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            //status->MPI_ERROR = MPI_SUCCESS;
        }
    } else {
        //Waiting for all types of local requests.
        //HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        //HMPI_Request_list* local_list = &g_tl_send_reqs;
        //HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        if(req->type == HMPI_SEND) {
            do {
                HMPI_Progress(recv_reqs_head, local_list, shared_list);
            } while(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE);
        } else {/*if(req->type & //Bitwise comparison! Any types below.
                (HMPI_RECV | HMPI_RECV_ANY_SOURCE | OPI_GIVE | OPI_TAKE)) {*/
            //If OPI isn't enabled, the constants are still present.
            //It doesn't cost anything to always check them here, so do that.

            do {
                HMPI_Progress(recv_reqs_head, local_list, shared_list);
            } while(get_reqstat(req) != HMPI_REQ_COMPLETE);
#if 0
        } else {
            ERROR("%d unknown request type %x\n",
                    HMPI_COMM_WORLD->comm_rank, req->type);
#endif
        }

        if(status != HMPI_STATUS_IGNORE) {
            status->size = req->size;
            status->MPI_SOURCE = req->proc;
            status->MPI_TAG = req->tag;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            //status->MPI_ERROR = MPI_SUCCESS;
        }
    }


    //Req is complete at this point.

#ifdef __bg__
    //What is this for? I think i was debugging something..
    //__fence();
#endif

    release_req(req);
    *request = HMPI_REQUEST_NULL;

    FULL_PROFILE_STOP(MPI_Wait);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Waitall(int count, HMPI_Request *requests, HMPI_Status *statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Waitall);
    LOG_MPI_CALL("MPI_Waitall(count=%d, requests=%p, statuses=%p)",
            count, requests, statuses);

#if 0
    for(int i = 0; i < count; i++) {
        HMPI_Status* p_st = (statuses == HMPI_STATUSES_IGNORE ? HMPI_STATUS_IGNORE : &statuses[i]);
        HMPI_Wait(&requests[i], p_st);
    }
#endif

    HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;
    int done;

    //TODO - try doing a simple wait on each req in succession.
    do {
        HMPI_Progress(recv_reqs_head, local_list, shared_list);
        done = 0;

        for(int i = 0; i < count; i++) {
            HMPI_Request req = requests[i];

            if(req == HMPI_REQUEST_NULL) {
                done += 1;
                continue;
            }

            if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
                //For some types, we can make progress and maybe complete the
                // request, so try doing that.  Other types, just continue.
                if(req->type == HMPI_SEND) {
                    if(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE) {
                        continue;
                    }
                } else if(req->type & (MPI_SEND | MPI_RECV)) {
                    if(!HMPI_Progress_mpi(req)) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            //req is complete but status not handled
            if(statuses != HMPI_STATUSES_IGNORE) {
                HMPI_Status* st = &statuses[i];
                st->size = req->size;
                st->MPI_SOURCE = req->proc;
                st->MPI_TAG = req->tag;
                st->MPI_ERROR = MPI_SUCCESS;
            }

            release_req(req);
            requests[i] = HMPI_REQUEST_NULL;
            done += 1;
        }
    } while(done < count);

    FULL_PROFILE_STOP(MPI_Waitall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


#if 0
int HMPI_Waitall(int count, HMPI_Request *requests, HMPI_Status *statuses)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Waitall);
    LOG_MPI_CALL("MPI_Waitall(count=%d, requests=%p, statuses=%p)",
            count, requests, statuses);

    //Split the request list into local and remote reqs.
    //Strategy:
    //Use a similar loop to complete all the local reqs.
    // Don't progress MPI reqs, but do call a dummy Iprobe so MPI progresses.
    //When all local reqs are complete, waitall on the MPI requests.
    // Do the necessary completion work for all of these.
    MPI_Request* remote_reqs = alloca(sizeof(MPI_Request) * count);
    HMPI_Request* local_reqs = alloca(sizeof(HMPI_Request) * 128);
    int num_remote_reqs = 0;
    int num_local_reqs = 0;

    for(int i = 0; i < count; i++) {
        HMPI_Request req = requests[i];

        if(req->type & (MPI_SEND | MPI_RECV)) {
            remote_reqs[num_remote_reqs++] = req->ir.req;
            //requests[i] = HMPI_REQUEST_NULL;
        } else {
            local_reqs[num_local_reqs++] = req;
        }
    }

    HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;
    int done;

    //TODO - try doing a simple wait on each req in succession.
        //MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,
        //        &done, MPI_STATUS_IGNORE);
    do {
        HMPI_Progress(recv_reqs_head, local_list, shared_list);
        done = 0;

        //for(int i = 0; i < count; i++) {
        for(int i = 0; i < num_local_reqs; i++) {
            //HMPI_Request req = requests[i];
            HMPI_Request req = local_reqs[i];

            if(req == HMPI_REQUEST_NULL) {
                done += 1;
                continue;
            }

            if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
                //For some types, we can make progress and maybe complete the
                // request, so try doing that.  Other types, just continue.
                if(req->type == HMPI_SEND) {
                    if(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE) {
                        continue;
                    }
#if 0
                } else if(req->type & (MPI_SEND | MPI_RECV)) {
                    if(!HMPI_Progress_mpi(req)) {
                        continue;
                    }
#endif
                } else {
                    continue;
                }
            }

            //req is complete but status not handled
            if(statuses != HMPI_STATUSES_IGNORE) {
                HMPI_Status* st = &statuses[i];
                st->size = req->size;
                st->MPI_SOURCE = req->proc;
                st->MPI_TAG = req->tag;
                st->MPI_ERROR = MPI_SUCCESS;
            }

            release_req(req);
            //requests[i] = HMPI_REQUEST_NULL;
            local_reqs[i] = HMPI_REQUEST_NULL;
            done += 1;
        }
    //} while(done < count);
    } while(done < num_local_reqs);


    //Now, wait on all the MPI-level reqs.
#if 0
    MPI_Status* mpi_statuses = MPI_STATUSES_IGNORE;
    if(statuses != HMPI_STATUSES_IGNORE) {
        mpi_statuses = alloca(sizeof(MPI_Status) * count);
    }
#endif

    //MPI_Waitall(count, remote_reqs, mpi_statuses);
    MPI_Waitall(num_remote_reqs, remote_reqs, MPI_STATUSES_IGNORE);


#if 0
    //Now all the requests are done -- but need to update status for MPI reqs.
    if(statuses != HMPI_STATUSES_IGNORE) {
        for(int i = 0; i < count; i++) {
            HMPI_Request req = requests[i];

            if(req->type & (MPI_SEND | MPI_RECV)) {
                HMPI_Status* st = &statuses[i];
                MPI_Status* mpi_st = &mpi_statuses[i];

                if(req->type == MPI_RECV) {
                    int count;
                    int type_size;

                    MPI_Get_count(mpi_st, req->datatype, &count);
                    MPI_Type_size(req->datatype, &type_size);

                    st->size = (size_t)count * (size_t)type_size;
                } else {
                    st->size = req->size;
                }

                st->MPI_SOURCE = req->proc;
                st->MPI_TAG = mpi_st->MPI_TAG;
                st->MPI_ERROR = MPI_SUCCESS;
            }
        }
    }
#endif

    //memset(requests, 0, sizeof(HMPI_Request) * count);
    FULL_PROFILE_STOP(MPI_Waitall);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}
#endif


int HMPI_Waitany(int count, HMPI_Request* requests, int* index, HMPI_Status *status)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Waitany);
    LOG_MPI_CALL("MPI_Waitany(count=%d, requests=%p, index=%p, status=%p)",
            count, requests, index, status);

    HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;
    int done;

    //Call Progress once, then check each req.
    do {
        done = 1;

        //What if I have progress return whether a completion was made?
        //Then I can skip the poll loop unless something finishes.
        HMPI_Progress(recv_reqs_head, local_list, shared_list);

        //TODO - is there a way to speed this up?
        // Maybe keep my own list of non-null entries that i can reorder to
        // skip null requests..
        for(int i = 0; i < count; i++) {
            HMPI_Request req = requests[i];
            if(req == HMPI_REQUEST_NULL) {
                continue;
            }

            //Not done! at least one req was not NULL.
            done = 0;

            //Check completion of the req.
            if(get_reqstat(req) != HMPI_REQ_COMPLETE) {
                //For some types, we can make progress and maybe complete the
                // request, so try doing that.  Other types, just continue.
                if(req->type == HMPI_SEND) {
                    if(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE) {
                        continue;
                    }
                } else if(req->type & (MPI_SEND | MPI_RECV)) {
                    if(!HMPI_Progress_mpi(req)) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            //req is complete, handle status.
            if(status != HMPI_STATUS_IGNORE) {
                status->size = req->size;
                status->MPI_SOURCE = req->proc;
                status->MPI_TAG = req->tag;
                status->MPI_ERROR = MPI_SUCCESS;
            }

            release_req(req);
            requests[i] = HMPI_REQUEST_NULL;
            *index = i;

            FULL_PROFILE_STOP(MPI_Waitany);
            FULL_PROFILE_START(MPI_Other);
            return MPI_SUCCESS;
        }
    } while(done == 0);

    //All requests were NULL.
    *index = MPI_UNDEFINED;
    if(status != HMPI_STATUS_IGNORE) {
        status->size = 0;
    }

    FULL_PROFILE_STOP(MPI_Waitany);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Get_count(HMPI_Status* status, MPI_Datatype datatype, int* count)
{
    LOG_MPI_CALL("MPI_Get_count(status=%d, datatype=%d, count=%p)",
            status, datatype, count);

    int type_size;

    MPI_Type_size(datatype, &type_size);

    /*if(unlikely(type_size == 0)) {
        *count = 0;
    } else*/ if(unlikely(status->size % type_size != 0)) {
        *count = MPI_UNDEFINED;
    } else {
        //Status size is a 64-bit size_t, type_size is an int.
        //This code does two size conversions, type_size up to 64bits,
        // then the result back down again to go into count.
        *count = status->size / (size_t)type_size;
    }

    return MPI_SUCCESS;
}


int HMPI_Iprobe(int source, int tag, HMPI_Comm comm, int* flag, HMPI_Status* status)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Iprobe);
    LOG_MPI_CALL("MPI_Iprobe(source=%d, tag=%d, comm=%d, flag=%p, status=%p)",
            source, tag, comm, flag, status);

    HMPI_Request send_req = NULL;

    //Progress here prevents deadlocks.
    HMPI_Progress(&g_recv_reqs_head, &g_tl_send_reqs, g_tl_my_send_reqs);

    //Probe HMPI (on-node) layer
    *flag = match_probe(source, tag, comm->context, &send_req);
    if(*flag) {
        if(status != HMPI_STATUS_IGNORE) {
            status->size = send_req->size;
            status->MPI_SOURCE = send_req->proc;
            status->MPI_TAG = send_req->tag;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            //status->MPI_ERROR = MPI_SUCCESS;
        }
    } else {
        //Probe off-node (MPI)
        MPI_Status st;
        MPI_Iprobe(source, tag, comm->comm, flag, &st);

        if(*flag && status != HMPI_STATUS_IGNORE) {
            int count;
            MPI_Get_count(&st, MPI_BYTE, &count);
            status->size = count;
            status->MPI_SOURCE = st.MPI_SOURCE;
            status->MPI_TAG = st.MPI_TAG;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            //status->MPI_ERROR = st.MPI_ERROR;
        }
    }

    FULL_PROFILE_STOP(MPI_Iprobe);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Probe(int source, int tag, HMPI_Comm comm, HMPI_Status* status)
{
    //LOG_MPI_CALL("MPI_Probe(source=%d, tag=%d, comm=%d, status=%p)",
    //        source, tag, comm, status);

    int flag;

    do {
        HMPI_Iprobe(source, tag, comm, &flag, status);
    } while(flag == 0);

    return MPI_SUCCESS;
}


//Returns the translation of the world rank to its local node rank,
// or MPI_UNDEFINED otherwise.  MPI_ANY_SOURCE is translated to itself.
//Other special values like MPI_PROC_NULL return MPI_UNDEFINED.
inline void HMPI_Comm_node_rank(const HMPI_Comm comm, const int rank, int* node_rank)
{
    int diff = rank - comm->node_root;

    if(diff >= 0 && diff < comm->node_size) {
        *node_rank = diff;
    } else if(unlikely(rank == MPI_ANY_SOURCE)) {
        *node_rank = MPI_ANY_SOURCE;
    } else {
        *node_rank = MPI_UNDEFINED;
    }
}


//This routine takes a NODE-scope dest rank, NOT a world-scope rank!
//Request must be allocated before calling this routine!
static void HMPI_Local_isend(void* buf, int count, MPI_Datatype datatype,
        int dest, int tag, HMPI_Comm comm, HMPI_Request req)
{
    int type_size;
    MPI_Type_size(datatype, &type_size);

    size_t size = (size_t)count * (size_t)type_size;

    req->size = size;
    req->buf = buf;
    req->proc = comm->comm_rank;
    req->tag = tag;
    req->context = comm->context;

    //update_reqstat() has a memory fence on BGQ, avoid it here.
    req->stat = HMPI_REQ_ACTIVE;
    req->type = HMPI_SEND;

    req->datatype = datatype;

#ifdef HMPI_CHECKSUM
    req->csum = compute_csum((uint8_t*)buf, size);
#endif

#ifdef __bg__
    //On BGQ, immediate doesn't provide a performance speedup.  But, the inline
    // buffer is still useful if the user's buf isn't in SM -- malloc/free can
    // be avoided for messages < 256bytes.
    if(buf != NULL && !IS_SM_BUF(buf)) {
        if(size <= EAGER_LIMIT) {
            memcpy(req->eager, buf, size);
            req->buf = req->eager;

            HMPI_STATS_ACCUMULATE(send_imm, 1);
        } else {
            req->do_free = DO_FREE;
            req->buf = MALLOC(uint8_t, size);
            memcpy(req->buf, buf, size);
        }
    }
#else
    //For small messages, copy into the eager buffer.
    //If the user's send buffer is not in the SM region, allocate an SM buf
    // and copy the data over.
    if(size <= EAGER_LIMIT) {
        memcpy(req->eager, buf, size);
        req->buf = req->eager;

        HMPI_STATS_ACCUMULATE(send_imm, 1);
    } else if(buf != NULL && !IS_SM_BUF(buf)) {
        req->do_free = DO_FREE;
        req->buf = MALLOC(uint8_t, size);
        memcpy(req->buf, buf, size);
    } 
#endif

    add_send_req(&g_send_reqs[dest], req);

    HMPI_STATS_ACCUMULATE(send_size, size);
    HMPI_STATS_ACCUMULATE(send_local, 1);
}


int HMPI_Send(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Send);
    LOG_MPI_CALL("HMPI_Send(buf=%p, count=%d, datatype=%d, "
            "dest=%d, tag=%d, comm=%p)", buf, count, datatype, dest, tag, comm);

#if 0
    HMPI_Request req;

    HMPI_Isend(buf, count, datatype, dest, tag, comm, &req);
    HMPI_Wait(&req, HMPI_STATUS_IGNORE);
    return MPI_SUCCESS;
#endif

#if DEBUG
    if(dest < 0) {
        ERROR("%d dest %d MPI_PROC_NULL %d MPI_ANY_SOURCE %d",
              HMPI_COMM_WORLD->comm_rank, dest, MPI_PROC_NULL, MPI_ANY_SOURCE);
    }
#endif

    //If dest is PROC_NULL, dest_node_rank == MPI_UNDEFINED.
    //MPI will then handle PROC_NULL, so we don't need to check for it.
    int dest_node_rank;
    HMPI_Comm_node_rank(comm, dest, &dest_node_rank);

    if(dest_node_rank != MPI_UNDEFINED) {
        HMPI_Request req = acquire_req();

        HMPI_Local_isend(buf, count, datatype, dest_node_rank, tag, comm, req);

        HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        HMPI_Request_list* local_list = &g_tl_send_reqs;
        HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        do {
            HMPI_Progress(recv_reqs_head, local_list, shared_list);
        } while(HMPI_Progress_send(req) != HMPI_REQ_COMPLETE);

        release_req(req);
    } else {
        MPI_Request req;
        int flag = 0;

        //Can't use MPI_Send here :(
        //Deadlocks are possible if local progress isn't made.
        MPI_Isend(buf, count, datatype, dest, tag, comm->comm, &req);

        HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        HMPI_Request_list* local_list = &g_tl_send_reqs;
        HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        do {
            HMPI_Progress(recv_reqs_head, local_list, shared_list);
            MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
        } while(flag == 0);

#ifdef HMPI_STATS
        int type_size;
        MPI_Type_size(datatype, &type_size);

        HMPI_STATS_ACCUMULATE(send_size, (size_t)count * (size_t)type_size);
        HMPI_STATS_ACCUMULATE(send_remote, 1);
#endif
    }

    //At one point, returning separately in each branch was faster on BGQ.
    FULL_PROFILE_STOP(MPI_Send);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Isend(void* buf, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request *request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Isend);
    LOG_MPI_CALL("MPI_Isend(buf=%p, count=%d, datatype=%d, dest=%d, tag=%d, comm=%p, request=%p)",
            buf, count, datatype, dest, tag, comm, request);

#if DEBUG
    if(dest < 0) {
        ERROR("%d invalid dest %d MPI_PROC_NULL %d MPI_ANY_SOURCE %d",
                HMPI_COMM_WORLD->comm_rank, dest, MPI_PROC_NULL, MPI_ANY_SOURCE);
    }
#endif

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = acquire_req();

#if 0
    if(unlikely(dest == MPI_PROC_NULL)) { 
        req->type = HMPI_SEND;
        update_reqstat(req, HMPI_REQ_COMPLETE);
        FULL_PROFILE_STOP(MPI_Isend);
        FULL_PROFILE_START(MPI_Other);
        BGQ_NOP;
        BGQ_NOP;
        BGQ_NOP;
        return MPI_SUCCESS;
    }
#endif

    int dest_node_rank;
    HMPI_Comm_node_rank(comm, dest, &dest_node_rank);

    if(dest_node_rank != MPI_UNDEFINED) {
        HMPI_Local_isend(buf, count, datatype,
                dest_node_rank, tag, comm, req);
    } else {
        MPI_Isend(buf, count, datatype,
                dest, tag, comm->comm, &req->ir.req);

        //update_reqstat() has a memory fence on BGQ, avoid it here.
        req->stat = HMPI_REQ_ACTIVE;
        req->type = MPI_SEND;
        //req->context = comm->context; //Not needed but faster on BGQ
        req->datatype = datatype;

#ifdef HMPI_STATS
        int type_size;
        MPI_Type_size(datatype, &type_size);

        HMPI_STATS_ACCUMULATE(send_size, (size_t)count * (size_t)type_size);
#endif
        HMPI_STATS_ACCUMULATE(send_remote, 1);
    }

    *request = req;
    FULL_PROFILE_STOP(MPI_Isend);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


//This routine takes a WORLD-scope source rank, NOT a node-scope rank!
static void HMPI_Local_irecv(void* buf, int count, MPI_Datatype datatype,
        int source, int tag, HMPI_Comm comm, HMPI_Request req)
{
#ifdef DEBUG
    //TODO - Torsten says this is wrong.
    //We can have extent == size, but the order is non-contiguous
    MPI_Aint extent, lb;
    MPI_Type_get_extent(datatype, &lb, &extent);
    if(extent != size) {
        printf("non-contiguous derived datatypes are not supported yet!\n");
        MPI_Abort(comm->comm, 0);
    }
#endif

#ifdef DEBUG
    if(buf != NULL && !IS_SM_BUF(buf)) {
        WARNING("%d non-SM buf %p size %ld\n",
                HMPI_COMM_WORLD->comm_rank, buf, req->size);
    }
#endif

    if(unlikely(source == MPI_ANY_SOURCE)) {
        MPI_Irecv(buf, count, datatype,
                source, tag, comm->comm, &req->u.req);

        req->type = HMPI_RECV_ANY_SOURCE;
    } else  {
        req->type = HMPI_RECV;
    }

    //TODO - move some of these assignments out to reduce argument count?
    int type_size;
    MPI_Type_size(datatype, &type_size);

    req->size = (size_t)count * (size_t)type_size;
    req->buf = buf;
    req->proc = source;
    req->tag = tag;
    req->context = comm->context;
    req->stat = HMPI_REQ_ACTIVE; //Avoid fence in update_reqstat() on BGQ
    req->datatype = datatype;

    add_recv_req(req);

    HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
    HMPI_Request_list* local_list = &g_tl_send_reqs;
    HMPI_Request_list* shared_list = g_tl_my_send_reqs;

    HMPI_Progress(recv_reqs_head, local_list, shared_list);
}


int HMPI_Recv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Status *status)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Recv);

    //If source is PROC_NULL, src_node_rank == MPI_UNDEFINED.
    //MPI will then handle PROC_NULL, so we don't need to check for it.
    int src_node_rank;
    HMPI_Comm_node_rank(comm, source, &src_node_rank);

    if(src_node_rank != MPI_UNDEFINED) {
        HMPI_Request req = acquire_req();

        //Yes, Local_irecv uses source, not src_node_rank.
        HMPI_Local_irecv(buf, count, datatype, source, tag, comm, req);
        //HMPI_Wait(&req, status);

        HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        HMPI_Request_list* local_list = &g_tl_send_reqs;
        HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        do {
            HMPI_Progress(recv_reqs_head, local_list, shared_list);
        } while(get_reqstat(req) != HMPI_REQ_COMPLETE);

        if(status != HMPI_STATUS_IGNORE) {
            status->size = req->size;
            status->MPI_SOURCE = req->proc;
            status->MPI_TAG = req->tag;
            //MPI 1.1 sec 3.2.5: Set MPI_ERROR only in multi-completion fns.
            //status->MPI_ERROR = MPI_SUCCESS;
        }

        release_req(req);
    } else {
        MPI_Request req;
        int flag = 0;

        MPI_Irecv(buf, count, datatype, source, tag, comm->comm, &req);

        HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        HMPI_Request_list* local_list = &g_tl_send_reqs;
        HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
        while(flag == 0) {
            HMPI_Progress(recv_reqs_head, local_list, shared_list);
            MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
        }
    }

#ifdef HMPI_STATS
    if(source == MPI_ANY_SOURCE) {
        HMPI_STATS_ACCUMULATE(recv_anysrc, 1);
    }
#endif

    FULL_PROFILE_STOP(MPI_Recv);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Irecv(void* buf, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request *request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Irecv);
    LOG_MPI_CALL("MPI_Irecv(buf=%p, count=%d, datatype=%d, src=%d, tag=%d, comm=%p, request=%p)",
            buf, count, datatype, source, tag, comm, request);

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();
    //HMPI_Request req = acquire_req();

#if 0
    if(unlikely(source == MPI_PROC_NULL)) { 
        req->size = 0;
        req->proc = MPI_PROC_NULL;
        req->tag = MPI_ANY_TAG;
        req->stat = HMPI_REQ_COMPLETE;
        req->type = HMPI_RECV;
        FULL_PROFILE_STOP(MPI_Irecv);
        FULL_PROFILE_START(MPI_Other);
        return MPI_SUCCESS;
    }
#endif

    //If source is PROC_NULL, src_node_rank == MPI_UNDEFINED.
    //MPI will then handle PROC_NULL, so we don't need to check for it.
    int src_node_rank;
    HMPI_Comm_node_rank(comm, source, &src_node_rank);

    if(src_node_rank != MPI_UNDEFINED) {
        //Yes, Local_irecv uses source, not src_node_rank.
        HMPI_Local_irecv(buf, count, datatype, source, tag, comm, req);
    } else { //Recv off-node, but not ANY_SOURCE
        MPI_Irecv(buf, count, datatype,
                source, tag, comm->comm, &req->ir.req);

        //update_reqstat() has a memory fence on BGQ, avoid it here.
        req->stat = HMPI_REQ_ACTIVE;
        req->type = MPI_RECV;
        req->datatype = datatype;
    }

#ifdef HMPI_STATS
    if(source == MPI_ANY_SOURCE) {
        HMPI_STATS_ACCUMULATE(recv_anysrc, 1);
    }
#endif

    //*request = req;
    FULL_PROFILE_STOP(MPI_Irecv);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Sendrecv(void *sendbuf, int sendcount, MPI_Datatype sendtype, 
                int dest, int sendtag,
                void *recvbuf, int recvcount, MPI_Datatype recvtype, 
                int source, int recvtag,
                HMPI_Comm comm, HMPI_Status *status)
{
    HMPI_Request req;

    //Irecv/Send/Wait is chosen intentionally: this creates the possibility
    // for sender-side acceleration in the synergistic protocol.  Doing
    // Isend/Recv/Wait would be less likely to do so since it'll only poll
    // the recv until that completes, then the send.  Irecv/Send polls both.
    HMPI_Irecv(recvbuf, recvcount, recvtype, source, recvtag, comm, &req);
    HMPI_Send(sendbuf, sendcount, sendtype, dest, sendtag, comm);

    HMPI_Wait(&req, status);
    return MPI_SUCCESS;
}


#ifdef ENABLE_OPI
int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int dest, int tag, HMPI_Comm comm, HMPI_Request* request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Give);
    LOG_MPI_CALL("OPI_Give(ptr=%p, count=%d, datatype=%d, dest=%d, tag=%d, comm=%p, request=%p)",
            ptr, count, datatype, dest, tag, comm, request);

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = acquire_req();

    //If dest is PROC_NULL, dest_node_rank == MPI_UNDEFINED.
    //MPI will then handle PROC_NULL, so we don't need to check for it.
    int dest_node_rank;
    HMPI_Comm_node_rank(comm, dest, &dest_node_rank);

    void* buf = *ptr;

    if(dest_node_rank != MPI_UNDEFINED) {

        int type_size;
        MPI_Type_size(datatype, &type_size);
        uint64_t size = (uint64_t)count * (uint64_t)type_size;

        req->size = size;
        req->buf = buf; //We store the pointer being shared directly.
        req->proc = comm->comm_rank;
        req->tag = tag;
        req->context = comm->context;

        req->stat = HMPI_REQ_ACTIVE;
        req->type = OPI_GIVE;

        req->datatype = datatype;

        add_send_req(&g_send_reqs[dest_node_rank], req);
    } else {
        //This is easy, just convert to a send.
        MPI_Isend(buf, count, datatype,
                dest, tag, comm->comm, &req->ir.req);

        //Although normal MPI sends don't need the buf to be set, Give does!
        // The buffer will be freed later, so we need the ptr to free it.
        req->buf = buf;

        req->stat = HMPI_REQ_ACTIVE;
        req->type = MPI_SEND;

        //OPI_Free should be called when the req is released.
        req->do_free = DO_OPI_FREE;

        req->datatype = datatype;
    }

    //OPI was defined so that Give and Free clear the pointer, but it's
    // disabled to provide a slight performance edge.
    //*ptr = NULL;
    *request = req;
    FULL_PROFILE_STOP(OPI_Give);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int source, int tag, HMPI_Comm comm, HMPI_Request* request)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(OPI_Take);
    LOG_MPI_CALL("OPI_Take(ptr=%p, count=%d, datatype=%d, src=%d, tag=%d, comm=%p, request=%p)",
            ptr, count, datatype, source, tag, comm, request);

    //Freed when req completion is signaled back to the user.
    HMPI_Request req = *request = acquire_req();

    //If source is PROC_NULL, src_node_rank == MPI_UNDEFINED.
    //MPI will then handle PROC_NULL, so we don't need to check for it.
    int src_node_rank;
    HMPI_Comm_node_rank(comm, source, &src_node_rank);

    //int type_size;
    //MPI_Type_size(datatype, &type_size);

    //req->size = count * type_size;

    req->proc = source;
    req->tag = tag;
    req->context = comm->context;

    //update_reqstat() has a memory fence on BGQ, avoid it here.
    req->stat = HMPI_REQ_ACTIVE;

    req->datatype = datatype;

    /*if(unlikely(source == MPI_ANY_SOURCE)) {
        //Take ANY_SOURCE not supported right now
        //TODO - what would i have to do for this?
        // We could match a local give, or a remote send.
        abort();

        // test both layers and pick first 
        req->buf = ptr;
        req->type = OPI_TAKE_ANY_SOURCE;

        add_recv_req(req);
    } else*/ if(src_node_rank != MPI_UNDEFINED) {
        //Recv on-node, but not ANY_SOURCE
        req->buf = ptr;
        req->type = OPI_TAKE;

        add_recv_req(req);

        HMPI_Item* recv_reqs_head = &g_recv_reqs_head;
        HMPI_Request_list* local_list = &g_tl_send_reqs;
        HMPI_Request_list* shared_list = g_tl_my_send_reqs;

        HMPI_Progress(recv_reqs_head, local_list, shared_list);
    } else { //Recv off-node, but not ANY_SOURCE
        int type_size;
        MPI_Type_size(datatype, &type_size);

        OPI_Alloc(ptr, (uint64_t)type_size * (uint64_t)count);
        MPI_Irecv(*ptr, count, datatype,
                source, tag, comm->comm, &req->ir.req);
        req->buf = *ptr;
        req->type = MPI_RECV;
    }

    FULL_PROFILE_STOP(OPI_Take);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}
#endif //ENABLE_OPI

