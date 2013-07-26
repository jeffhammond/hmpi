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
#include <mpi.h>
#include <assert.h>
#include "libpsm.h"


#ifdef MVAPICH2_VERSION
#error "HMPI-PSM won't work with MVAPICH2, try MPICH2 instead"
#endif

#define MPI_OOB_TAG 44648
#define TIMEOUT 10000000000


//#define LIBPSM_DEBUG 1

#define GET_PEER_RANK(peer) \
    (((uintptr_t)(peer) - (uintptr_t)libpsm_peers) / sizeof(peer_t))


#if 0
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
#endif

//lock_t libpsm_lock;      //Protects all PSM calls
mcs_lock_t libpsm_lock;      //Protects all PSM calls
lock_t libpsm_conn_lock; //Protects the connectino mechanism

int libpsm_mpi_rank;

peer_t* libpsm_peers = NULL;

psm_ep_t libpsm_ep;
static psm_epid_t libpsm_epid;
psm_mq_t libpsm_mq;
volatile int libpsm_poll = 0;


void libpsm_connect(peer_t* peer);


void libpsm_init(void)
{
    int size;
    int i;
    psm_error_t err;

    //LOCK_INIT(&libpsm_lock, 0);
    MCS_LOCK_INIT(&libpsm_lock);
    LOCK_INIT(&libpsm_conn_lock, 0);
    
    MPI_Comm_rank(MPI_COMM_WORLD, &libpsm_mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    libpsm_peers = (peer_t*)malloc(sizeof(peer_t) * size);

    for(i = 0; i < size; i++) {
        libpsm_peers[i].conn_state = CONN_DISCONNECTED;
    }

    {
        int ver_major = PSM_VERNO_MAJOR;
        int ver_minor = PSM_VERNO_MINOR;

        err = psm_init(&ver_major, &ver_minor);
        if(err != PSM_OK) {
            printf("%d ERROR psm init\n", libpsm_mpi_rank);
            exit(-1);
        }
    }

    {
        psm_uuid_t uuid;
        //struct psm_ep_open_opts opts;

        /* Setup MPI_LOCALRANKID and MPI_LOCALNRANKS so PSM can allocate hardware
         * contexts correctly.
         */
        //OMPI_COMM_WORLD_SIZE=4
        //OMPI_COMM_WORLD_LOCAL_SIZE=2
        //OMPI_COMM_WORLD_RANK=3
        //OMPI_COMM_WORLD_LOCAL_RANK=1

#if 0
        char* str;
        str = getenv("OMPI_COMM_WORLD_LOCAL_SIZE");
        setenv("MPI_LOCALNRANKS", str, 0);

        str = getenv("OMPI_COMM_WORLD_RANK");
        setenv("MPI_LOCALRANKID", str, 0);
#endif

#if 0
        char env_string[256];
        snprintf(env_string, 256, "%d", mpi_rank % 12);
        setenv("MPI_LOCALRANKID", env_string, 0);
        snprintf(env_string, 256, "%d", 12);
        setenv("MPI_LOCALNRANKS", env_string, 0);
#endif
   
#if 0 
        char* str;
        str = getenv("MPI_LOCALNRANKS");
        printf("MPI_LOCALNRANKS %s\n", str);
#endif

        if(libpsm_mpi_rank == 0) {
            psm_uuid_generate(uuid);
        }

        MPI_Bcast(&uuid, sizeof(psm_uuid_t), MPI_BYTE, 0, MPI_COMM_WORLD);

        //psm_ep_open_opts_get_defaults(&opts);

        //err = psm_ep_open(uuid, &opts, &libpsm_ep, &libpsm_epid);
        err = psm_ep_open(uuid, NULL, &libpsm_ep, &libpsm_epid);
        if(err != PSM_OK) {
            printf("%d ERROR psm_ep_open\n", libpsm_mpi_rank);
        }
    }

    err = psm_mq_init(libpsm_ep, PSM_MQ_ORDERMASK_NONE, NULL, 0, &libpsm_mq);
    if(err != PSM_OK) {
        printf("%d ERROR psm_mq_init\n", libpsm_mpi_rank);
        exit(-1);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    //Fully connect right now.
    for(i = 1; i < size; i++) {
        //printf("%d starting connect to %d\n", libpsm_mpi_rank, (libpsm_mpi_rank + i) % size);
        //fflush(stdout);
        libpsm_connect(&libpsm_peers[(libpsm_mpi_rank + i) % size]);
    }

    for(i = 1; i < size; i++) {
        //printf("%d waiting connect to %d\n", libpsm_mpi_rank, (libpsm_mpi_rank + i) % size);
        //fflush(stdout);
        peer_t* peer = &libpsm_peers[(libpsm_mpi_rank + i) % size];
        while(peer->conn_state != CONN_CONNECTED) {
            libpsm_connect(peer);
        }
    }



    //Connect to self
#if 0
    printf("%d connecting to self\n", mpi_rank);
    fflush(stdout);
    peer_t* peer = &libpsm_peers[mpi_rank];
    psm_error_t err2;

    err = psm_ep_connect(libpsm_ep, 1, &peer->epid, NULL, &err2,
            &peer->epaddr, 0);
    if(err != PSM_OK) {
        printf("%d ERROR psm_ep_connect %d %d\n", mpi_rank, err, err2);
        exit(-1);
    }

    peer->conn_state = CONN_CONNECTED;
    printf("%d self connected\n", mpi_rank);
    fflush(stdout);
#endif
}


void libpsm_shutdown(void)
{
}


static void libpsm_start_connect(peer_t* peer)
{
    MPI_Send(&libpsm_epid, sizeof(psm_epid_t), MPI_BYTE,
            GET_PEER_RANK(peer), MPI_OOB_TAG, MPI_COMM_WORLD);
    peer->conn_state = CONN_WAIT_INFO;
    //printf("%d sending conn req to %d\n", mpi_rank, GET_PEER_RANK(peer));
    //fflush(stdout);
}


void libpsm_connect(peer_t* peer)
{
    //Keep a receive posted all the time, or probe?
    //Need to exchange epid's, sizeof(psm_epid_t)
    MPI_Status st;
    int flag;

    LOCK_SET(&libpsm_conn_lock);

    //If this peer is disconnected, start the connection process.
    if(peer != NULL && peer->conn_state == CONN_DISCONNECTED) {
        //printf("%d peer not connected %d\n", mpi_rank, GET_PEER_RANK(peer));
        //fflush(stdout);
        libpsm_start_connect(peer);
    }

    //Poll the MPI connection receive request, and process.
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_OOB_TAG, MPI_COMM_WORLD, &flag, &st);
    if(!flag) {
        LOCK_CLEAR(&libpsm_conn_lock);
        return;
    }

    //Make sure we're dealing with the peer the message came from.
    peer = &libpsm_peers[st.MPI_SOURCE];
    //printf("%d received conn msg from %d state %d\n", mpi_rank, st.MPI_SOURCE, (int)peer->conn_state);
    //fflush(stdout);

    switch(peer->conn_state) {
    case CONN_DISCONNECTED:
        libpsm_start_connect(peer);
        //Fall through on purpose
    case CONN_WAIT_INFO:
    {
        psm_error_t err;
        psm_error_t err2;

        MPI_Recv(&peer->epid, sizeof(psm_epid_t), MPI_BYTE,
                st.MPI_SOURCE, MPI_OOB_TAG, MPI_COMM_WORLD, &st);

        //psm_error_register_handler(ep, PSM_ERRHANDLER_NOP);
        //LOCK_SET(&libpsm_lock);
        mcs_qnode_t q;
        MCS_LOCK_ACQUIRE(&libpsm_lock, &q);
        err = psm_ep_connect(libpsm_ep, 1, &peer->epid, NULL, &err2,
                &peer->epaddr, 0);
        //LOCK_CLEAR(&libpsm_lock);
        MCS_LOCK_RELEASE(&libpsm_lock, &q);

        if(err != PSM_OK) {
            printf("%d ERROR psm_ep_connect %d %d\n", libpsm_mpi_rank, err, err2);
            exit(-1);
        }

        peer->conn_state = CONN_CONNECTED;
        //printf("%d connected to %d\n", mpi_rank, GET_PEER_RANK(peer));
        //fflush(stdout);
    }
    default:
        break;
    }

    LOCK_CLEAR(&libpsm_conn_lock);
}


void libpsm_connect2(uint32_t rank)
{
    peer_t* peer = &libpsm_peers[rank];

    while(peer->conn_state != CONN_CONNECTED) {
        libpsm_connect(peer);
    }
}


#if 0
int post_recv(uint32_t src_rank, void* buf, uint32_t len, uint16_t tag, uint16_t comm, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[src_rank];

    while(unlikely(peer->conn_state != CONN_CONNECTED)) {
        libpsm_connect(peer);
    }
    
//psm_mq_irecv(psm_mq_t mq, uint64_t rtag, uint64_t rtagsel, uint32_t flags,
//	     void *buf, uint32_t len, void *context, psm_mq_req_t *req);

//    printf("%d psm post_recv src %d buf %p len %d tag %d req %p peer %p\n",
//            mpi_rank, src_rank, buf, len, tag, req, peer);
//    fflush(stdout);
    psm_mq_irecv(libpsm_mq, BUILD_TAG(src_rank, tag, comm), TAGSEL, 0,
            buf, len, NULL, &req->psm_req);
}


int post_send(uint32_t dst_rank, void* buf, uint32_t len, uint16_t tag, uint16_t comm, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[dst_rank];

    while(unlikely(peer->conn_state != CONN_CONNECTED)) {
        libpsm_connect(peer);
    }
    
//psm_mq_isend(psm_mq_t mq, psm_epaddr_t dest, uint32_t flags, uint64_t stag, 
//	     const void *buf, uint32_t len, void *context, psm_mq_req_t *req);

    //printf("%d psm post_send dst %d buf %p len %d tag %d %llx req %p peer %p\n",
    //        mpi_rank, dst_rank, buf, len, tag, BUILD_TAG(mpi_rank, tag), req, peer);
    //fflush(stdout);

    psm_mq_isend(libpsm_mq, peer->epaddr, 0, BUILD_TAG(mpi_rank, tag, comm),
            buf, len, NULL, &req->psm_req);
    //psm_mq_send(libpsm_mq, peer->epaddr, 0, BUILD_TAG(mpi_rank, tag), buf, len);

}
#endif


#if 0
void wait(libpsm_req_t* req)
{
//    printf("%d waiting on send\n", mpi_rank);
    psm_mq_status_t st;
    psm_mq_wait(&req->psm_req, &st);
//    while(psm_mq_test(&req->psm_req, &st) != PSM_OK) {
//        psm_poll(libpsm_ep);
//    }
}

int test(libpsm_req_t* req)
{
    return psm_mq_test(&req->psm_req, NULL) == PSM_OK;
}

void poll(void)
{
    psm_poll(libpsm_ep);
}
#endif

void print_stats(void)
{
    psm_mq_stats_t stats;

    psm_mq_get_stats(libpsm_mq, &stats);
    printf("%d PSM rx_user_bytes %lu\n", libpsm_mpi_rank, stats.rx_user_bytes);
    printf("%d PSM rx_user_num %lu\n", libpsm_mpi_rank, stats.rx_user_num);
    printf("%d PSM rx_sys_bytes %lu\n", libpsm_mpi_rank, stats.rx_sys_bytes);
    printf("%d PSM rx_sys_num %lu\n", libpsm_mpi_rank, stats.rx_sys_num);

    printf("%d PSM tx_num %lu\n", libpsm_mpi_rank, stats.tx_num);
    printf("%d PSM tx_eager_num %lu\n", libpsm_mpi_rank, stats.tx_eager_num);
    printf("%d PSM tx_eager_bytes %lu\n", libpsm_mpi_rank, stats.tx_eager_bytes);
    printf("%d PSM tx_rndv_num %lu\n", libpsm_mpi_rank, stats.tx_rndv_num);
    printf("%d PSM tx_rndv_bytes %lu\n", libpsm_mpi_rank, stats.tx_rndv_bytes);

    printf("%d PSM tx_shm_num %lu\n", libpsm_mpi_rank, stats.tx_shm_num);
    printf("%d PSM rx_shm_num %lu\n", libpsm_mpi_rank, stats.rx_shm_num);

    printf("%d PSM rx_sysbuf_num %lu\n", libpsm_mpi_rank, stats.rx_sysbuf_num);
    printf("%d PSM rx_sysbuf_bytes %lu\n", libpsm_mpi_rank, stats.rx_sysbuf_bytes);
}

