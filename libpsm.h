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
#define TAGSEL_ANY_TAG 0xFFFFFFFFFF000000ULL //MPI_ANY_SOURCE

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


extern lock_t libpsm_lock;      //Used to protect ANY_SRC Iprobe/Recv sequence
extern peer_t* libpsm_peers;
extern psm_ep_t libpsm_ep;
extern psm_mq_t libpsm_mq;
extern int libpsm_mpi_rank;


void libpsm_init(void);

void libpsm_shutdown(void);

void libpsm_connect(peer_t* peer);  //Internal connect
void libpsm_connect2(uint32_t rank);     //External connect


static inline void post_recv(void* buf, uint32_t len, uint64_t tag, uint64_t tagsel, uint32_t rank, libpsm_req_t* req)
{
    if(likely(rank != (uint32_t)-1)) {
        peer_t* peer = &libpsm_peers[rank];

        while(unlikely(peer->conn_state != CONN_CONNECTED)) {
            libpsm_connect(peer);
        }
    }

    LOCK_SET(&libpsm_lock);
    psm_mq_irecv(libpsm_mq, tag, tagsel, 0, buf, len, NULL, req);
    LOCK_CLEAR(&libpsm_lock);
}


//Rank is the MPI/PSM level rank to send to.
static inline void post_send(void* buf, uint32_t len, uint64_t tag, uint32_t rank, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[rank];

    while(unlikely(peer->conn_state != CONN_CONNECTED)) {
        libpsm_connect(peer);
    }
    
    LOCK_SET(&libpsm_lock);
    psm_mq_isend(libpsm_mq, peer->epaddr, 0, tag, buf, len, NULL, req);
    LOCK_CLEAR(&libpsm_lock);
}


static inline int cancel(libpsm_req_t* req)
{
    LOCK_SET(&libpsm_lock);
    int ret = psm_mq_cancel(req);
    if(ret == PSM_OK) {
        psm_mq_test(req, NULL);
    }
    LOCK_CLEAR(&libpsm_lock);
    return ret == PSM_OK;
}


static inline void wait(libpsm_req_t* req, libpsm_status_t* status)
{
    LOCK_SET(&libpsm_lock);
    psm_mq_wait(req, status);
    LOCK_CLEAR(&libpsm_lock);
}


static inline int test(libpsm_req_t* req, libpsm_status_t* status)
{
    LOCK_SET(&libpsm_lock);
    int ret = psm_mq_test(req, status) == PSM_OK;
    LOCK_CLEAR(&libpsm_lock);
    return ret;
}

static inline void poll(void)
{
    //TODO - this sucks, it results in an MPI_Iprobe.
    //Is there a better way to set up connections?
    //Can we use slurm OOB?
    libpsm_connect(NULL);

    LOCK_SET(&libpsm_lock);
    psm_poll(libpsm_ep);
    LOCK_CLEAR(&libpsm_lock);
}


void print_stats(void);

#endif

