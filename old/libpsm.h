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
#ifndef _LIBPSM_H_
#define _LIBPSM_H_
#include <stdint.h>
#include <stdlib.h>
#include <psm.h>
#include <psm_mq.h>
#include "lock.h"

#ifndef likely
#define likely(x)       __builtin_expect((x),1)
#endif

#ifndef unlikely
#define unlikely(x)     __builtin_expect((x),0)
#endif

//TODO - check out ORDERMASK flags, do I need full ordering?

#define TAGSEL_P2P 0xFFFFFFFFFFFFFFFFULL //Normal pt2pt messages
#define TAGSEL_ANY_SRC 0x00000000FFFFFFFFULL //MPI_ANY_SOURCE
#define TAGSEL_ANY_TAG 0xFFFFFFFFFF000000ULL //MPI_ANY_TAG

//64bits total for the PSM tag, split into 32 for the rank, 16 for tag, and 16 for comm.
//TODO - I need to identify both the src and dest here.
//Using 8 bits for src/dest thread plus the 32 for the MPI rank would work.
//TODO - make send/recv just take a tag to reduce the number of arguments.


//We can use the HMPI-level rank as the rank here, eliminating a src field.
//We send to a PSM-level rank though, so need to include a dst thread field.
//And of course we want as many bits as possible for the tag.
//So:
// 32 bit HMPI-level source rank
// 8 bits for a destination thread
// 24 bits for a tag (can fit in ids for MPI comms later too)

#define BUILD_TAG(src_rank, dst, tag) \
     (((uint64_t)(src_rank) << 32) | \
      ((uint64_t)((dst) & 0xFF) << 24) | \
      ((uint64_t)(tag) & 0xFFFFFF))

//Extract fields from an existing tag.
#define TAG_GET_RANK(tag) ((uint32_t)(tag >> 32))
#define TAG_GET_DST(tag) ((uint32_t)(tag >> 24) & 0xFF)
#define TAG_GET_TAG(tag) ((uint32_t)tag & 0xFFFFFF)


//Public data structures

#if 0
typedef struct libpsm_req_t
{
    psm_mq_req_t psm_req;
} libpsm_req_t;
#endif
typedef psm_mq_req_t libpsm_req_t;
typedef psm_mq_status_t libpsm_status_t;


//Private data structures

typedef enum peer_conn_state_t
{
    CONN_DISCONNECTED = 0,
    CONN_WAIT_INFO,
    CONN_CONNECTED
} peer_conn_state_t;

typedef struct peer_t
{
    peer_conn_state_t conn_state;
    psm_epid_t epid;
    psm_epaddr_t epaddr;
} peer_t;


//extern lock_t libpsm_lock;      //Used to protect ANY_SRC Iprobe/Recv sequence
extern mcs_lock_t libpsm_lock;
extern peer_t* libpsm_peers;
extern psm_ep_t libpsm_ep;
extern psm_mq_t libpsm_mq;
extern int libpsm_mpi_rank;
extern volatile int libpsm_poll;


void libpsm_init(void);

void libpsm_shutdown(void);

void libpsm_connect(peer_t* peer);  //Internal connect
void libpsm_connect2(uint32_t rank);     //External connect


static inline void post_recv(void* buf, uint32_t len, uint64_t tag, uint64_t tagsel, uint32_t rank, libpsm_req_t* req)
{
#if 0
    if(likely(rank != (uint32_t)-1)) {
        peer_t* peer = &libpsm_peers[rank];

        while(unlikely(peer->conn_state != CONN_CONNECTED)) {
            libpsm_connect(peer);
        }
    }
#endif

    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    psm_mq_irecv(libpsm_mq, tag, tagsel, 0, buf, len, NULL, req);
    MCS_LOCK_RELEASE(&libpsm_lock, &q);
}


//Rank is the MPI/PSM level rank to send to.
static inline void post_send(void* buf, uint32_t len, uint64_t tag, uint32_t rank, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[rank];
    psm_epaddr_t epaddr = peer->epaddr;

#if 0
    while(unlikely(peer->conn_state != CONN_CONNECTED)) {
        libpsm_connect(peer);
    }
#endif
    
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    psm_mq_isend(libpsm_mq, epaddr, 0, tag, buf, len, NULL, req);
    MCS_LOCK_RELEASE(&libpsm_lock, &q);
}


static inline void post_recv_nl(void* buf, uint32_t len, uint64_t tag, uint64_t tagsel, uint32_t rank, libpsm_req_t* req)
{
    psm_mq_irecv(libpsm_mq, tag, tagsel, 0, buf, len, NULL, req);
}


//Rank is the MPI/PSM level rank to send to.
static inline void post_send_nl(void* buf, uint32_t len, uint64_t tag, uint32_t rank, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[rank];

    psm_mq_isend(libpsm_mq, peer->epaddr, 0, tag, buf, len, NULL, req);
}


static inline int cancel(libpsm_req_t* req)
{
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    int ret = psm_mq_cancel(req);
    if(ret == PSM_OK) {
        psm_mq_test(req, NULL);
    }
    MCS_LOCK_RELEASE(&libpsm_lock, &q);
    return ret == PSM_OK;
}


static inline void wait(libpsm_req_t* req, libpsm_status_t* status)
{
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    psm_mq_wait(req, status);
    MCS_LOCK_RELEASE(&libpsm_lock, &q);
}


static inline int test(libpsm_req_t* req, libpsm_status_t* status)
{
    mcs_qnode_t q;
    MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
    int ret = psm_mq_test(req, status) == PSM_OK;
    MCS_LOCK_RELEASE(&libpsm_lock, &q);
    return ret;
}

static inline void poll(void)
{
    //TODO - this sucks, it results in an MPI_Iprobe.
    //Is there a better way to set up connections?
    //Can we use slurm OOB?

    //TODO - idea -- try the lock, if it is already acquired, skip polling.
    //libpsm_connect(NULL);

    //if(libpsm_poll == 0) {
    //    libpsm_poll = 1;
    //if(libpsm_lock == NULL) {
        mcs_qnode_t q;
        MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
        psm_poll(libpsm_ep);
        MCS_LOCK_RELEASE(&libpsm_lock, &q);
    //    libpsm_poll = 0;
    //}
}


void print_stats(void);

#endif

