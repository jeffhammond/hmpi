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

#include "profile2.h"

#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "error.h"
#include "lock.h"
#ifdef __bg__
#include <spi/include/kernel/memory.h>
#include "mpix.h"
#endif

#ifdef USE_NUMA
#include <numa.h>
#endif


#ifdef FULL_PROFILE
PROFILE_DECLARE();
#define FULL_PROFILE_INIT() PROFILE_INIT()
#define FULL_PROFILE_VAR(v) PROFILE_VAR(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_RESET(v) PROFILE_RESET(v)
#define FULL_PROFILE_SHOW(v) PROFILE_SHOW(v)
#else
#define FULL_PROFILE_INIT()
#define FULL_PROFILE_VAR(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_RESET(v)
#define FULL_PROFILE_SHOW(v)
#endif

FULL_PROFILE_VAR(MPI_Other);
FULL_PROFILE_VAR(MPI_Isend);
FULL_PROFILE_VAR(MPI_Irecv);
FULL_PROFILE_VAR(MPI_Test);
FULL_PROFILE_VAR(MPI_Testall);
FULL_PROFILE_VAR(MPI_Wait);
FULL_PROFILE_VAR(MPI_Waitall);
FULL_PROFILE_VAR(MPI_Waitany);
FULL_PROFILE_VAR(MPI_Iprobe);

FULL_PROFILE_VAR(MPI_Barrier);
FULL_PROFILE_VAR(MPI_Reduce);
FULL_PROFILE_VAR(MPI_Allreduce);
FULL_PROFILE_VAR(MPI_Scan);
FULL_PROFILE_VAR(MPI_Bcast);
FULL_PROFILE_VAR(MPI_Scatter);
FULL_PROFILE_VAR(MPI_Gather);
FULL_PROFILE_VAR(MPI_Gatherv);
FULL_PROFILE_VAR(MPI_Allgather);
FULL_PROFILE_VAR(MPI_Allgatherv);
FULL_PROFILE_VAR(MPI_Alltoall);


#ifdef ENABLE_OPI
FULL_PROFILE_VAR(OPI_Alloc);
FULL_PROFILE_VAR(OPI_Free);
FULL_PROFILE_VAR(OPI_Give);
FULL_PROFILE_VAR(OPI_Take);

void OPI_Init(void);
void OPI_Finalize(void);
#endif


//Pointer to a shared context counter.  This counter is used to obtain new
// context ID's when communicators are created, so that every communicator
// used in a node has its own context.  The context is used in matching to
// differentiate communicators.
static int* g_comm_context = NULL;
                            
extern int g_rank;                      //HMPI world rank
extern int g_size;                      //HMPI world size
extern int g_node_rank;                 //HMPI node rank
extern int g_node_size;                 //HMPI node size
//extern int g_net_rank=-1;                  //HMPI net rank
//extern int g_net_size=-1;                  //HMPI net size
#if 0
extern int g_numa_node=-1;                 //HMPI numa node (compute-node scope)
extern int g_numa_root=-1;                 //HMPI root rank on same numa node
extern int g_numa_rank=-1;                 //HMPI rank within numa node
extern int g_numa_size=-1;                 //HMPI numa node size
#endif

extern HMPI_Comm HMPI_COMM_WORLD;


#ifdef HMPI_LOGCALLS
int g_log_fd = -1;

#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define LOG_MPI_CALL log_mpi_call
static void log_mpi_call(char* fmt, ...)
{
    va_list args;
    char str[1024];

    if(g_log_fd) {
        ERROR("Log file descriptor not initialized");
    }

    va_start(args, fmt);
    int len = vsnprintf(str, 1024, fmt, args);
    va_end(args);

    if(len >= 1024) {
        len = 1023;
    }

    strcat(str, "\n");
    write(g_log_fd, str, len + 1);
}
#else
#define LOG_MPI_CALL(fmt, ...)
#endif


// Internal global structures

//Each thread has a list of send and receive requests.
//The receive requests are managed privately by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them in match_recv().

extern HMPI_Item g_recv_reqs_head;
extern HMPI_Item* g_recv_reqs_tail;


#ifndef __bg__
extern mcs_qnode_t* g_lock_q;                   //Q node for lock
#endif
extern HMPI_Request_list* g_send_reqs;   //Shared: Senders add sends here
extern HMPI_Request_list* g_tl_my_send_reqs;    //Shortcut to my global send Q
extern HMPI_Request_list g_tl_send_reqs;        //Receiver-local send Q

//Pool of unused reqs to save malloc time.
extern HMPI_Item* g_free_reqs;


//#ifndef __bg__
#if 0
#include <numa.h>
#include <syscall.h>


void print_numa(void)
{
    if(numa_available() == -1) {
        ERROR("%d NUMA library not available", g_rank);
    }

    //printf("%d numa_max_node %d\n", g_rank, numa_max_node());
    //printf("%d numa_num_configured_nodes %d\n", g_rank, numa_num_configured_nodes());
    //unsigned long size, unsigned long maskp
#if 0
    struct bitmask* bm = numa_get_mems_allowed();
    printf("%d bm size %d\n", g_rank, bm->size);
    for(int i = 0; i < bm->size / sizeof(unsigned long); i++) {
        printf("%d numa_get_mems_allowed 0x%x\n", g_rank, bm->maskp[i]);
    }
#endif
#if 0
    numa_set_localalloc();
    int preferred = numa_preferred();
    long long freesize;
    long long totalsize = numa_node_size64(preferred, &freesize);

    printf("%d numa_preferred %d total %lld free %lld\n",
            g_rank, preferred, totalsize, freesize);
#endif



    //check some pages using move_pages and sm_lower.
    int pagesize = numa_pagesize();
    void* pages[4];
    int status[4];
    void* data = malloc(pagesize * 4);
    memset(data, 0, pagesize * 4);

    for(int i = 0; i < 4; i++) {
        pages[i] = (void*)((uintptr_t)data + (i * pagesize));
    }

    pages[0] = &pagesize;

    int ret = numa_move_pages(0, 4, pages, NULL, status, 0);
    if(ret > 0) {
        WARNING("%d move_pages couldn't move some pages %d", g_rank, ret);
        return;
    } else if(ret < 0) {
        //printf("%d ERROR move pages %d\n", g_rank, ret);
        WARNING("%d move_pages returned %d", g_rank, ret);
    }

    //for(int i = 0; i < 4; i++) {
    //    printf("%d page %p status %d %s\n", g_rank, pages[i], status[i], strerror(-status[i]));
    //}

    numa_set_preferred(status[0]);
    //printf("%d now preferred %d\n", g_rank, numa_preferred());
    free(data);
}
#endif


//Initialize a new communicator structure.
//Assumes the base MPI communicator (comm->comm) is already set to a valid
//MPI communicator.  All other values will be filled in based on the MPI comm.
void init_communicator(HMPI_Comm comm)
{

}


int HMPI_Init(int *argc, char ***argv)
{
    MPI_Init(argc, argv);
    FULL_PROFILE_INIT();

#ifdef __bg__
    //On BG/Q, we rely on BG_MAPCOMMONHEAP=1 to get shared memory.
    //Check that it is set before continuing.
    char* tmp = getenv("BG_MAPCOMMONHEAP");
    if(tmp == NULL || atoi(tmp) != 1) {
        ERROR("BG_MAPCOMMONHEAP not enabled");
    }
#endif

    //Set up communicators
    //TODO - this needs to be pulled out into its own routine.
    // A lot of the g_* variables should be moved into the comm struct.
    MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_size);

    if(g_rank == 0) {
        WARNING("sizeof req %d", sizeof(HMPI_Request_info));
        WARNING("sizeof comm %d", sizeof(HMPI_Comm_info));
    }
    
    HMPI_COMM_WORLD = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
    HMPI_COMM_WORLD->comm = MPI_COMM_WORLD;
    HMPI_COMM_WORLD->context = 0;

    //Split into comms containing ranks on the same nodes.
    {
#ifdef __bg__
        MPIX_Hardware_t hw;

        MPIX_Hardware(&hw);

        //printf("%d prank %d psize %d ppn %d coreID %d MHz %d memSize %d\n",
        //        g_rank, hw.prank, hw.psize, hw.ppn, hw.coreID, hw.clockMHz,
        //        hw.memSize);
        int color = 0;
        for(int i = 0; i < hw.torus_dimension; i++) {
            color = (color * hw.Size[i]) + hw.Coords[i];
        }

#else
        //Hash our processor name into a color for Comm_split()
        char proc_name[MPI_MAX_PROCESSOR_NAME];
        int proc_name_len;
        MPI_Get_processor_name(proc_name, &proc_name_len);

        int color = 0;
        for(char* s = proc_name; *s != '\0'; s++) {
            color = *s + 31 * color;
        }
#endif

        //MPI says color must be non-negative.
        color &= 0x7FFFFFFF;

        MPI_Comm_split(MPI_COMM_WORLD, color, g_rank,
                &HMPI_COMM_WORLD->node_comm);
    }

    //Used in HMPI_Comm_node_rank().
    MPI_Comm_size(HMPI_COMM_WORLD->node_comm, &HMPI_COMM_WORLD->node_size);

    {
        MPI_Group node_group;
        MPI_Group world_group;
        MPI_Comm_group(HMPI_COMM_WORLD->node_comm, &node_group);
        MPI_Comm_group(HMPI_COMM_WORLD->comm, &world_group);

        int base_rank = 0;
        MPI_Group_translate_ranks(node_group, 1,
                &base_rank, world_group, &HMPI_COMM_WORLD->node_root);

    }


    MPI_Comm_rank(HMPI_COMM_WORLD->node_comm, &g_node_rank);
    MPI_Comm_size(HMPI_COMM_WORLD->node_comm, &g_node_size);


    //Create a comm that goes across the nodes.
    //This will contain only the procs with node rank 0, or node rank 1, etc.
    MPI_Comm_split(MPI_COMM_WORLD,
            g_node_rank, g_rank, &HMPI_COMM_WORLD->net_comm);
#if 0
    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");

    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");
    asm("nop\nnop\nnop\nnop\n");
#endif


#if 0
    MPI_Comm_rank(HMPI_COMM_WORLD->net_comm, &g_net_rank);
    MPI_Comm_size(HMPI_COMM_WORLD->net_comm, &g_net_size);
#endif


#if 0
#ifdef USE_NUMA
    //Split the node comm into per-NUMA-domain (ie socket) comms.
    //Look up the NUMA node of a stack page -- this should be local.
    int ret = 0;
    void* page = &ret;

    ret = numa_move_pages(0, 1, &page, NULL, &g_numa_node, 0);
    if(ret != 0) {
        printf("ERROR numa_move_pages %s\n", strerror(ret));
        MPI_Abort(MPI_COMM_WORLD, 0);
    }
#else
    //Without a way to find the local NUMA node, assume one NUMA node.
    g_numa_node = 0;
#endif //USE_NUMA

    MPI_Comm_split(HMPI_COMM_WORLD->node_comm, g_numa_node, g_node_rank,
            &HMPI_COMM_WORLD->numa_comm);

    MPI_Comm_rank(HMPI_COMM_WORLD->numa_comm, &g_numa_rank);
    MPI_Comm_size(HMPI_COMM_WORLD->numa_comm, &g_numa_size);

    {
        MPI_Group numa_group;
        MPI_Group world_group;
        MPI_Comm_group(HMPI_COMM_WORLD->numa_comm, &numa_group);
        MPI_Comm_group(HMPI_COMM_WORLD->comm, &world_group);

        int base_rank = 0;
        MPI_Group_translate_ranks(numa_group, 1,
                &base_rank, world_group, &g_numa_root);
    }
#endif

#if 0
    printf("%5d rank=%3d size=%3d node_rank=%2d node_size=%2d node_root=%4d "
            "net_rank=%2d net_size=%2d numa_node=%d numa_root=%3d "
            "numa_rank=%2d\n",
            getpid(), g_rank, g_size, g_node_rank, g_node_size,
            HMPI_COMM_WORLD->node_root, g_net_rank, g_net_size, g_numa_node,
            g_numa_root, g_numa_rank);
#endif


    //Set up intra-node shared memory structures.
    if(g_node_rank == 0) {
        //One rank per node allocates shared send request lists.
        g_send_reqs = MALLOC(HMPI_Request_list, g_node_size);

        //One global context counter value.
        g_comm_context = MALLOC(int, 1);

        //HMPI_COMM_WORLD has context 0.
        *g_comm_context = 4;
    }

    MPI_Bcast(&g_send_reqs, 1, MPI_LONG, 0, HMPI_COMM_WORLD->node_comm);
    MPI_Bcast(&g_comm_context, 1, MPI_LONG, 0, HMPI_COMM_WORLD->node_comm);


    // Initialize request lists and lock
    g_recv_reqs_tail = &g_recv_reqs_head;

#ifndef __bg__
    //Except on BGQ, allocate a SHARED lock Q for use with MCS locks.
    //Used in Qing sends on the receiver and clearing that Q.
    g_lock_q = MALLOC(mcs_qnode_t, 1);
    memset(g_lock_q, 0, sizeof(mcs_qnode_t));
#endif

    g_send_reqs[g_node_rank].head.next = NULL;
    g_send_reqs[g_node_rank].tail = &g_send_reqs[g_node_rank].head;

    g_tl_my_send_reqs = &g_send_reqs[g_node_rank];
    LOCK_INIT(&g_send_reqs[g_node_rank].lock);

    g_tl_send_reqs.head.next = NULL;
    g_tl_send_reqs.tail = &g_tl_send_reqs.head;

    //print_numa();

#if 0
    hmpi_coll_t* coll = HMPI_COMM_WORLD->coll = MALLOC(hmpi_coll_t, 1);

    MPI_Bcast(&HMPI_COMM_WORLD->coll, 1, MPI_LONG, 0,
            HMPI_COMM_WORLD->node_comm);

    if(g_node_rank == 0) {
        coll->sbuf = MALLOC(volatile void*, g_node_size);
        coll->rbuf = MALLOC(volatile void*, g_node_size);
        coll->tmp = MALLOC(volatile void*, g_node_size);


        for(int i=0; i<PTOP; i++) {
            coll->ptop[i] = MALLOC(padptop, g_node_size);
        }

        // for(int i =0; i<g_node_size; i++)
        // {
        //   coll->ptop_0[i] = 0;
        //   coll->ptop_1[i] = 0;
        //   coll->ptop_2[i] = 0;
        //   coll->ptop_3[i] = 0;
        //   coll->ptop_4[i] = 0;
        // }

        for(int j = 0; j < PTOP; j++) {
            for(int i = 0; i < g_node_size; i++) {
                coll->ptop[j][i].ptopsense = 0;
            }
        }

        //for(int i =0; i<g_node_size; i++)
        //{
        //  coll->ptop_0[i].ptopsense = 0;
        //  coll->ptop_1[i].ptopsense = 0;
        //  coll->ptop_2[i].ptopsense = 0;
        //  coll->ptop_3[i].ptopsense = 0;
        //  coll->ptop_4[i].ptopsense = 0;
        //}

        //FANINEQUAL1(t_barrier_init_fanin1(&coll->t_barr, g_node_size);) 
        //PFANIN(t_barrier_init(&coll->t_barr, g_node_size););
    }
#endif


#ifdef ENABLE_OPI
    OPI_Init();
#endif

    //Set up debugging stuff
#ifdef HMPI_LOGCALLS
    {
        char filename[1024];
        snprintf(filename, 1024, "hmpi-%d.log", getpid());
        g_log_fd = open(filename, O_CREAT|O_DIRECT|O_TRUNC|O_WRONLY);
    }
#endif

    MPI_Barrier(MPI_COMM_WORLD);
    FULL_PROFILE_START(MPI_Other);
    return MPI_SUCCESS;
}


int HMPI_Finalize(void)
{
    FULL_PROFILE_STOP(MPI_Other);
    FULL_PROFILE_START(MPI_Other);
    FULL_PROFILE_SHOW(MPI_Isend);
    FULL_PROFILE_SHOW(MPI_Irecv);
    FULL_PROFILE_SHOW(MPI_Test);
    FULL_PROFILE_SHOW(MPI_Testall);
    FULL_PROFILE_SHOW(MPI_Wait);
    FULL_PROFILE_SHOW(MPI_Waitall);
    FULL_PROFILE_SHOW(MPI_Waitany);
    FULL_PROFILE_SHOW(MPI_Iprobe);

    FULL_PROFILE_SHOW(MPI_Barrier);
    FULL_PROFILE_SHOW(MPI_Reduce);
    FULL_PROFILE_SHOW(MPI_Allreduce);
    FULL_PROFILE_SHOW(MPI_Scan);
    FULL_PROFILE_SHOW(MPI_Bcast);
    FULL_PROFILE_SHOW(MPI_Scatter);
    FULL_PROFILE_SHOW(MPI_Gather);
    FULL_PROFILE_SHOW(MPI_Gatherv);
    FULL_PROFILE_SHOW(MPI_Allgather);
    FULL_PROFILE_SHOW(MPI_Allgatherv);
    FULL_PROFILE_SHOW(MPI_Alltoall);

    FULL_PROFILE_SHOW(MPI_Other);

#ifdef ENABLE_OPI
    OPI_Finalize();
#endif

    //Seems to prevent a segfault in MPI_Finalize()
    MPI_Barrier(HMPI_COMM_WORLD->comm);

    MPI_Finalize();
    return 0;
}

