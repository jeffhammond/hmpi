#include <stdint.h>
#include <stdio.h>
#include <mpi.h>
#include <assert.h>
#include "libpsm.h"

#define MPI_OOB_TAG 44648
#define TIMEOUT 10000000000
#define TAGSEL 0xFFFFFFFFFFFFFFFFULL

//#define LIBPSM_DEBUG 1

#define BUILD_TAG(r, t, c) \
    (((uint64_t)(r) << 48) | ((uint64_t)((c) & 0xFFFF) << 32) | (uint64_t)((t) & 0xFFFFFFFF))

#define GET_PEER_RANK(peer) \
    (((uintptr_t)(peer) - (uintptr_t)libpsm_peers) / sizeof(peer_t))


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


static int mpi_rank;

static peer_t* libpsm_peers = NULL;

static psm_ep_t libpsm_ep;
static psm_epid_t libpsm_epid;
static psm_mq_t libpsm_mq;

static void libpsm_connect(peer_t* peer);

int libpsm_init(void)
{
    int size;
    int i;
    psm_error_t err;

    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
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
            printf("%d ERROR psm init\n", mpi_rank);
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

        if(mpi_rank == 0) {
            psm_uuid_generate(uuid);
        }

        MPI_Bcast(&uuid, sizeof(psm_uuid_t), MPI_BYTE, 0, MPI_COMM_WORLD);

        //psm_ep_open_opts_get_defaults(&opts);

        //err = psm_ep_open(uuid, &opts, &libpsm_ep, &libpsm_epid);
        err = psm_ep_open(uuid, NULL, &libpsm_ep, &libpsm_epid);
        if(err != PSM_OK) {
            printf("%d ERROR psm_ep_open\n", mpi_rank);
        }
    }

    err = psm_mq_init(libpsm_ep, PSM_MQ_ORDERMASK_NONE, NULL, 0, &libpsm_mq);
    if(err != PSM_OK) {
        printf("%d ERROR psm_mq_init\n", mpi_rank);
        exit(-1);
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


int libpsm_shutdown(void)
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


//TODO - connection setup
static void libpsm_connect(peer_t* peer)
{
    //Keep a receive posted all the time, or probe?
    //Need to exchange epid's, sizeof(psm_epid_t)
    MPI_Status st;
    int flag;

    //If this peer is disconnected, start the connection process.
    if(peer->conn_state == CONN_DISCONNECTED) {
        //printf("%d peer not connected %d\n", mpi_rank, GET_PEER_RANK(peer));
        //fflush(stdout);
        libpsm_start_connect(peer);
    }

    //Poll the MPI connection receive request, and process.
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_OOB_TAG, MPI_COMM_WORLD, &flag, &st);
    if(!flag) {
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
        err = psm_ep_connect(libpsm_ep, 1, &peer->epid, NULL, &err2,
                &peer->epaddr, 0);
        if(err != PSM_OK) {
            printf("%d ERROR psm_ep_connect %d %d\n", mpi_rank, err, err2);
            exit(-1);
        }

        peer->conn_state = CONN_CONNECTED;
        //printf("%d connected to %d\n", mpi_rank, GET_PEER_RANK(peer));
        //fflush(stdout);
    }
    }
}

void libpsm_connect2(int rank)
{
    peer_t* peer = &libpsm_peers[rank];

    while(peer->conn_state != CONN_CONNECTED) {
        libpsm_connect(peer);
    }
}


int post_recv(int src_rank, void* buf, uint32_t len, uint32_t tag, uint16_t comm, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[src_rank];

    while(peer->conn_state != CONN_CONNECTED) {
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


#if 0
int wait_recv(libpsm_req_t* req)
{
    psm_mq_status_t st;
//    printf("%d waiting on recv\n", mpi_rank);
    psm_mq_wait(&req->psm_req, &st);

//    while(psm_mq_test(&req->psm_req, &st) != PSM_OK) {
//        psm_poll(libpsm_ep);
//    }
}

int test_recv(libpsm_req_t* req)
{
    return psm_mq_test(&req->psm_req, NULL) == PSM_OK;
}
#endif

int post_send(int dst_rank, void* buf, uint32_t len, uint32_t tag, uint16_t comm, libpsm_req_t* req)
{
    peer_t* peer = &libpsm_peers[dst_rank];

    while(peer->conn_state != CONN_CONNECTED) {
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

int print_stats(void)
{
    psm_mq_stats_t stats;

    psm_mq_get_stats(libpsm_mq, &stats);
    printf("%d PSM rx_user_bytes %llu\n", mpi_rank, stats.rx_user_bytes);
    printf("%d PSM rx_user_num %llu\n", mpi_rank, stats.rx_user_num);
    printf("%d PSM rx_sys_bytes %llu\n", mpi_rank, stats.rx_sys_bytes);
    printf("%d PSM rx_sys_num %llu\n", mpi_rank, stats.rx_sys_num);

    printf("%d PSM tx_num %llu\n", mpi_rank, stats.tx_num);
    printf("%d PSM tx_eager_num %llu\n", mpi_rank, stats.tx_eager_num);
    printf("%d PSM tx_eager_bytes %llu\n", mpi_rank, stats.tx_eager_bytes);
    printf("%d PSM tx_rndv_num %llu\n", mpi_rank, stats.tx_rndv_num);
    printf("%d PSM tx_rndv_bytes %llu\n", mpi_rank, stats.tx_rndv_bytes);

    printf("%d PSM tx_shm_num %llu\n", mpi_rank, stats.tx_shm_num);
    printf("%d PSM rx_shm_num %llu\n", mpi_rank, stats.rx_shm_num);

    printf("%d PSM rx_sysbuf_num %llu\n", mpi_rank, stats.rx_sysbuf_num);
    printf("%d PSM rx_sysbuf_bytes %llu\n", mpi_rank, stats.rx_sysbuf_bytes);
}

