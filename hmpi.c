/* Copyright (c) 2010-2013 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010-2013 Lawrence Livermore National Security, LLC.
 *                         All rights reserved. LLNL-CODE-642002
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
 * - Neither the Indiana University, LLNS/LLNL, nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
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
 * 
 * Additional BSD Notice
 * 
 * 1. This notice is required to be provided under our contract with the U.S.
 *    Department of Energy (DOE). This work was supported in part by the
 *    Department of Energy X-Stack program and the Early Career award program.
 *    It was partially performed under the auspices of the U.S. Department of
 *    Energy by Lawrence Livermore National Laboratory under Contract
 *    DE-AC52-07NA27344.
 * 
 * 2. Neither the United States Government nor Lawrence Livermore National
 *    Security, LLC nor any of their employees, makes any warranty, express
 *    or implied, or assumes any liability or responsibility for the accuracy,
 *    completeness, or usefulness of any information, apparatus, product, or
 *    process disclosed, or represents that its use would not infringe
 *    privately-owned rights.
 * 
 * 3. Also, reference herein to any specific commercial products, process, or
 *    services by trade name, trademark, manufacturer or otherwise does not
 *    necessarily constitute or imply its endorsement, recommendation, or
 *    favoring by the United States Government or Lawrence Livermore National
 *    Security, LLC. The views and opinions of authors expressed herein do not
 *    necessarily state or reflect those of the United States Government or
 *    Lawrence Livermore National Security, LLC, and shall not be used for
 *    advertising or product endorsement purposes.
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
#define FULL_PROFILE_TIMER(v) PROFILE_TIMER(v)
#define FULL_PROFILE_START(v) PROFILE_START(v)
#define FULL_PROFILE_STOP(v) PROFILE_STOP(v)
#define FULL_PROFILE_TIMER_RESET(v) PROFILE_TIMER_RESET(v)
#define FULL_PROFILE_TIMER_SHOW(v) PROFILE_TIMER_SHOW(v)
#else
#define FULL_PROFILE_INIT()
#define FULL_PROFILE_TIMER(v)
#define FULL_PROFILE_START(v)
#define FULL_PROFILE_STOP(v)
#define FULL_PROFILE_TIMER_RESET(v)
#define FULL_PROFILE_TIMER_SHOW(v)
#endif

FULL_PROFILE_TIMER(MPI_Other);
FULL_PROFILE_TIMER(MPI_Send);
FULL_PROFILE_TIMER(MPI_Recv);
FULL_PROFILE_TIMER(MPI_Isend);
FULL_PROFILE_TIMER(MPI_Irecv);
FULL_PROFILE_TIMER(MPI_Test);
FULL_PROFILE_TIMER(MPI_Testall);
FULL_PROFILE_TIMER(MPI_Wait);
FULL_PROFILE_TIMER(MPI_Waitall);
FULL_PROFILE_TIMER(MPI_Waitany);
FULL_PROFILE_TIMER(MPI_Iprobe);

FULL_PROFILE_TIMER(MPI_Barrier);
FULL_PROFILE_TIMER(MPI_Reduce);
FULL_PROFILE_TIMER(MPI_Allreduce);
FULL_PROFILE_TIMER(MPI_Scan);
FULL_PROFILE_TIMER(MPI_Bcast);
FULL_PROFILE_TIMER(MPI_Scatter);
FULL_PROFILE_TIMER(MPI_Gather);
FULL_PROFILE_TIMER(MPI_Gatherv);
FULL_PROFILE_TIMER(MPI_Allgather);
FULL_PROFILE_TIMER(MPI_Allgatherv);
FULL_PROFILE_TIMER(MPI_Alltoall);


#ifdef ENABLE_OPI
FULL_PROFILE_TIMER(OPI_Alloc);
FULL_PROFILE_TIMER(OPI_Free);
FULL_PROFILE_TIMER(OPI_Give);
FULL_PROFILE_TIMER(OPI_Take);

void OPI_Init(void);
void OPI_Finalize(void);
#endif


//Statistics on message size, counts.
#ifdef HMPI_STATS
PROFILE_DECLARE();
#define HMPI_STATS_INIT() PROFILE_INIT()
#define HMPI_STATS_COUNTER(v) PROFILE_COUNTER(v)
#define HMPI_STATS_COUNTER_EXTERN(v) HMPI_STATS_COUNTER_EXTERN(v)
#define HMPI_STATS_ACCUMULATE(v) PROFILE_ACCUMULATE(v, c)
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

HMPI_STATS_COUNTER(send_size);      //Send message size
HMPI_STATS_COUNTER(send_local);     //Local destination send
HMPI_STATS_COUNTER(send_remote);    //Remote destination send
HMPI_STATS_COUNTER(send_imm);       //Sender used immediate protocol
HMPI_STATS_COUNTER(send_syn);       //Sender helped synergistic protocol
HMPI_STATS_COUNTER(recv_syn);       //Receiver used synergistic protocol
HMPI_STATS_COUNTER(recv_mem);       //Receiver used memcpy
HMPI_STATS_COUNTER(recv_anysrc);    //Receive ANY_SRC


#ifdef HMPI_LOGCALLS
int g_log_fd = -1;

#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define LOG_MPI_CALL log_mpi_call
void log_mpi_call(char* fmt, ...)
{
    va_list args;
    char str[1024];

    if(g_log_fd == -1) {
        ERROR("Log file descriptor not initialized %d", g_log_fd);
    }

    va_start(args, fmt);
    int len = vsnprintf(str, 1024, fmt, args);
    va_end(args);

    if(len >= 1024) {
        len = 1023;
    }

    strcat(str, "\n");
    //write(g_log_fd, str, len + 1);
    write(stdout, str, len + 1);
}
#else
#define LOG_MPI_CALL(fmt, ...)
#endif


HMPI_Comm HMPI_COMM_WORLD;
HMPI_Comm HMPI_COMM_NODE;
HMPI_Comm HMPI_COMM_CACHE;
HMPI_Comm HMPI_COMM_NETWORK;


// Internal global structures

//Pointer to a shared context counter.  This counter is used to obtain new
// context ID's when communicators are created, so that every communicator
// used in a node has its own context.  The context is used in matching to
// differentiate communicators.
static int* g_comm_context = NULL;
                            

//Each thread has a list of send and receive requests.
//The receive requests are managed privately by the owning thread.
//The send requests list for a particular thread contains sends whose target is
// that thread.  Other threads place their send requests on this list, and the
// thread owning the list matches receives against them in match_recv().

extern HMPI_Item g_recv_reqs_head;
extern HMPI_Item* g_recv_reqs_tail;


#ifdef USE_MCS
extern mcs_qnode_t* g_lock_q;                   //Q node for lock
#endif
extern HMPI_Request_list* g_send_reqs;   //Shared: Senders add sends here
extern HMPI_Request_list* g_tl_my_send_reqs;    //Shortcut to my global send Q
extern HMPI_Request_list g_tl_send_reqs;        //Receiver-local send Q

//Pool of unused reqs to save malloc time.
extern HMPI_Item* g_free_reqs;


#ifdef __bg__
#include <spi/include/kernel/memory.h>

void print_bgq_mem(void)
{
    uint64_t shared, persist, heapavail, stackavail, stack, heap, guard, mmap;

    Kernel_GetMemorySize(KERNEL_MEMSIZE_SHARED, &shared);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_PERSIST, &persist);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_HEAPAVAIL, &heapavail);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_STACKAVAIL, &stackavail);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_STACK, &stack);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_HEAP, &heap);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_GUARD, &guard);
    Kernel_GetMemorySize(KERNEL_MEMSIZE_MMAP, &mmap);

    //if(heap >= heapavail >> 1) {
    //if(HMPI_COMM_WORLD->node_rank = 1) {
        printf("Allocated heap: %.2f MB, avail. heap: %.2f MB\n", (double)heap/(1024*1024), (double)heapavail/(1024*1024));
        printf("Allocated stack: %.2f MB, avail. stack: %.2f MB\n", (double)stack/(1024*1024), (double)stackavail/(1024*1024));
        printf("Memory: shared: %.2f MB, persist: %.2f MB, guard: %.2f MB, mmap: %.2f MB\n", (double)shared/(1024*1024), (double)persist/(1024*1024), (double)guard/(1024*1024), (double)mmap/(1024*1024));
        fflush(stdout);
    //}
}
#endif

//#ifndef __bg__
#if 0
#include <numa.h>
#include <syscall.h>


void print_numa(void)
{
    if(numa_available() == -1) {
        ERROR("%d NUMA library not available", HMPI_COMM_WORLD->comm_rank);
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
        WARNING("%d move_pages couldn't move some pages %d", HMPI_COMM_WORLD->comm_rank, ret);
        return;
    } else if(ret < 0) {
        //printf("%d ERROR move pages %d\n", g_rank, ret);
        WARNING("%d move_pages returned %d", HMPI_COMM_WORLD->comm_rank, ret);
    }

    //for(int i = 0; i < 4; i++) {
    //    printf("%d page %p status %d %s\n", g_rank, pages[i], status[i], strerror(-status[i]));
    //}

    numa_set_preferred(status[0]);
    //printf("%d now preferred %d\n", g_rank, numa_preferred());
    free(data);
}
#endif


#if 0
#include <hwloc.h>

void print_hwloc(void)
{
    hwloc_topology_t topology;

    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);

    int numcores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_CORE);
    int numpus = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    int numsockets = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_SOCKET);
    int numnumas = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_NODE);

    //WARNING("numcores %d numpus %d numsockets %d numnumas %d",
    //        numcores, numpus, numsockets, numnumas);

    char str[1024] = {0};
    hwloc_cpuset_t cpuset = hwloc_bitmap_alloc();

    //This gives the core this rank is pinned to.
    hwloc_get_cpubind(topology, cpuset, HWLOC_CPUBIND_PROCESS);
    int ret = hwloc_bitmap_snprintf(str, 1023, cpuset);

    WARNING("rank %d bitmap %s (%d)", HMPI_COMM_WORLD->comm_rank, str, ret);

    hwloc_obj_t obj;
#if 0
    hwloc_obj_t obj = hwloc_get_obj_covering_cpuset(topology, cpuset);

    while(obj != NULL) {

        memset(str, 1024, 0);
        hwloc_obj_type_snprintf(str, 1023, obj, 1);

        WARNING("rank %d obj type %s", HMPI_COMM_WORLD->comm_rank, str);


        memset(str, 1024, 0);
        hwloc_obj_attr_snprintf(str, 1024, obj, "@", 1);

        WARNING("rank %d obj attr %s", HMPI_COMM_WORLD->comm_rank, str);
        obj = hwloc_get_child_covering_cpuset(topology, cpuset, obj);
    }
#endif

    hwloc_topology_restrict(topology, cpuset, 0);

    obj = hwloc_get_next_obj_by_type(topology, HWLOC_OBJ_PU, NULL);

    if(obj == NULL) {
        WARNING("get next obj by type was NULL");
    } else {
        WARNING("%d PU obj name %s logical_index %d depth %d",
            HMPI_COMM_WORLD->comm_rank, obj->name, obj->logical_index, obj->depth);
    }

    //OK, should be able to use this to get a numa node ID.
    obj = hwloc_get_next_obj_by_type(topology, HWLOC_OBJ_NODE, NULL);

    if(obj == NULL) {
        WARNING("get next obj by type was NULL");
    } else {
        WARNING("%d NODE obj name %s logical_index %d os_index %d depth %d",
            HMPI_COMM_WORLD->comm_rank, obj->name, obj->logical_index, obj->os_index, obj->depth);
    }
}
#endif



//Initialize a new communicator structure.
//Assumes the base MPI communicator (comm->comm) is already set to a valid
//MPI communicator.  All other values will be filled in based on the MPI comm.
void init_communicator(HMPI_Comm comm)
{
    //Fill in the cached comm variables.
    MPI_Comm_rank(comm->comm, &comm->comm_rank);
    //MPI_Comm_size(comm, &comm->comm_size);


    //Split into comms containing ranks on the same nodes.
    //TODO - use MPI3 comm_split_type
    {
#ifdef __bg__
        MPIX_Hardware_t hw;

        MPIX_Hardware(&hw);

        //printf("%d prank %d psize %d ppn %d coreID %d MHz %d memSize %d\n",
        //        comm->comm_rank, hw.prank, hw.psize, hw.ppn, hw.coreID,
        //        hw.clockMHz, hw.memSize);
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

        MPI_Comm_split(comm->comm, color, comm->comm_rank,
                &comm->node_comm);
    }

    MPI_Comm_rank(comm->node_comm, &comm->node_rank);
    MPI_Comm_size(comm->node_comm, &comm->node_size);

    //Translate rank 0 in the node comm into its rank in the main comm.
    //Used by HMPI_Comm_node_rank().
    {
        MPI_Group node_group;
        MPI_Group comm_group;
        //MPI_Comm_group(comm->node_comm, &node_group);
        //Yes, this really should be HMPI_COMM_WORLD!
        //HMPI_Comm_node_rank uses node_root to translate a comm rank into a
        // node rank.  Regardless of how ranks are mapped in the source comm,
        // the node ranks do not change.
        MPI_Comm_group(HMPI_COMM_WORLD->node_comm, &node_group);
        MPI_Comm_group(comm->comm, &comm_group);

        int base_rank = 0;
        MPI_Group_translate_ranks(node_group, 1,
                &base_rank, comm_group, &comm->node_root);
    }

    //Create a comm that goes across the nodes.
    //This will contain only the procs with node rank 0, or node rank 1, etc.
    MPI_Comm_split(comm->comm,
            comm->node_rank, comm->comm_rank, &comm->net_comm);

    //MPI_Comm_rank(comm->net_comm, &comm->net_rank);
    //MPI_Comm_size(comm->net_comm, &comm->net_size);


#if 0
#ifdef USE_NUMA
    //Split the node comm into per-NUMA-domain (ie socket) comms.
    //Look up the NUMA node of a stack page -- this should be local.
    int ret = 0;
    void* page = &ret;

    ret = numa_move_pages(0, 1, &page, NULL, &g_numa_node, 0);
    if(ret != 0) {
        printf("ERROR numa_move_pages %s\n", strerror(ret));
        MPI_Abort(comm, 0);
    }
#else
    //Without a way to find the local NUMA node, assume one NUMA node.
    g_numa_node = 0;
#endif //USE_NUMA

    MPI_Comm_split(comm->node_comm, g_numa_node, comm->node_rank,
            &comm->numa_comm);

    MPI_Comm_rank(comm->numa_comm, &g_numa_rank);
    MPI_Comm_size(comm->numa_comm, &g_numa_size);

    {
        MPI_Group numa_group;
        MPI_Group world_group;
        MPI_Comm_group(comm->numa_comm, &numa_group);
        MPI_Comm_group(comm->comm, &world_group);

        int base_rank = 0;
        MPI_Group_translate_ranks(numa_group, 1,
                &base_rank, world_group, &g_numa_root);
    }
#endif

    //If g_comm_counter is NULL, initialize it using HMPI_COMM_WORLD directly.
    //If NULL and we're here, that means COMM_WORLD is set up, so we can use
    //the node comm.
    if(g_comm_context == NULL) {
        if(HMPI_COMM_WORLD->node_rank == 0) {
            //One global context counter value.
            g_comm_context = MALLOC(int, 1);
            *g_comm_context = 0;
        }

        MPI_Bcast(&g_comm_context, 1, MPI_LONG, 0, HMPI_COMM_WORLD->node_comm);
    }

    //Node rank 0 grabs a new context.  Even though communicator creation is
    // collective, it's still possible to split up the communicators and have
    // multiple creations occurring within a node at the same time. So use
    // FETCH_ADD32 to be safe.
    if(comm->node_rank == 0) {
        comm->context = FETCH_ADD32(g_comm_context, 1);
    }

    MPI_Bcast(&comm->context, 1, MPI_INT, 0, comm->node_comm);

    comm->coll = NULL;
#if 0
    hmpi_coll_t* coll = comm->coll = MALLOC(hmpi_coll_t, 1);

    MPI_Bcast(&comm->coll, 1, MPI_LONG, 0,
            comm->node_comm);

    if(comm->node_rank == 0) {
        coll->sbuf = MALLOC(volatile void*, comm->node_size);
        coll->rbuf = MALLOC(volatile void*, comm->node_size);
        coll->tmp = MALLOC(volatile void*, comm->node_size);


        for(int i=0; i<PTOP; i++) {
            coll->ptop[i] = MALLOC(padptop, comm->node_size);
        }

        // for(int i =0; i<comm->node_size; i++)
        // {
        //   coll->ptop_0[i] = 0;
        //   coll->ptop_1[i] = 0;
        //   coll->ptop_2[i] = 0;
        //   coll->ptop_3[i] = 0;
        //   coll->ptop_4[i] = 0;
        // }

        for(int j = 0; j < PTOP; j++) {
            for(int i = 0; i < comm->node_size; i++) {
                coll->ptop[j][i].ptopsense = 0;
            }
        }

        //for(int i =0; i<comm->node_size; i++)
        //{
        //  coll->ptop_0[i].ptopsense = 0;
        //  coll->ptop_1[i].ptopsense = 0;
        //  coll->ptop_2[i].ptopsense = 0;
        //  coll->ptop_3[i].ptopsense = 0;
        //  coll->ptop_4[i].ptopsense = 0;
        //}

        //FANINEQUAL1(t_barrier_init_fanin1(&coll->t_barr, comm->node_size);) 
        //PFANIN(t_barrier_init(&coll->t_barr, comm->node_size););
    }
#endif

}


int HMPI_Init(int *argc, char ***argv)
{
    MPI_Init(argc, argv);
    FULL_PROFILE_INIT();
    HMPI_STATS_INIT();

#ifdef __bg__
    //print_bgq_mem();
#endif

#ifdef __bg__
    //No longer using BG_MAPCOMMONHEAP, and it doesn't matter how it's set.
#if 0
    //On BG/Q, we rely on BG_MAPCOMMONHEAP=1 to get shared memory.
    //Check that it is set before continuing.
    char* tmp = getenv("BG_MAPCOMMONHEAP");
    if(tmp == NULL || atoi(tmp) != 1) {
        ERROR("BG_MAPCOMMONHEAP not enabled");
    }
#endif
#endif

    //Set up communicators.
    HMPI_COMM_WORLD = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
    HMPI_COMM_WORLD->comm = MPI_COMM_WORLD;
    init_communicator(HMPI_COMM_WORLD);

    //Borrow HMPI_COMM_WORLD's node_comm.
    HMPI_COMM_NODE = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
    //HMPI_COMM_NODE->comm = HMPI_COMM_WORLD->node_comm;
    MPI_Comm_dup(HMPI_COMM_WORLD->node_comm, &HMPI_COMM_NODE->comm);
    init_communicator(HMPI_COMM_NODE);
    
    //Borrow HMPI_COMM_WORLD's net_comm.
    HMPI_COMM_NETWORK = (HMPI_Comm_info*)MALLOC(HMPI_Comm_info, 1);
    //HMPI_COMM_NETWORK->comm = HMPI_COMM_WORLD->node_comm;
    MPI_Comm_dup(HMPI_COMM_WORLD->net_comm, &HMPI_COMM_NETWORK->comm);
    init_communicator(HMPI_COMM_NODE);


    //Set up intra-node shared memory structures.
    if(HMPI_COMM_WORLD->node_rank == 0) {
        //One rank per node allocates shared send request lists.
        g_send_reqs = MALLOC(HMPI_Request_list, HMPI_COMM_WORLD->node_size);
    }

    MPI_Bcast(&g_send_reqs, 1, MPI_LONG, 0, HMPI_COMM_WORLD->node_comm);


    // Initialize request lists and lock
    g_recv_reqs_tail = &g_recv_reqs_head;

#ifdef USE_MCS
    //Except on BGQ, allocate a SHARED lock Q for use with MCS locks.
    //Used in Qing sends on the receiver and clearing that Q.
    g_lock_q = MALLOC(mcs_qnode_t, 1);
    memset(g_lock_q, 0, sizeof(mcs_qnode_t));
#endif

    g_send_reqs[HMPI_COMM_WORLD->node_rank].head.next = NULL;
    g_send_reqs[HMPI_COMM_WORLD->node_rank].tail = &g_send_reqs[HMPI_COMM_WORLD->node_rank].head;

    g_tl_my_send_reqs = &g_send_reqs[HMPI_COMM_WORLD->node_rank];
    LOCK_INIT(&g_send_reqs[HMPI_COMM_WORLD->node_rank].lock);

    g_tl_send_reqs.head.next = NULL;
    g_tl_send_reqs.tail = &g_tl_send_reqs.head;

    //print_numa();


#ifdef ENABLE_OPI
    OPI_Init();
#endif

    //Set up debugging stuff
#ifdef HMPI_LOGCALLS
    {
        char filename[1024];
        snprintf(filename, 1024, "hmpi-%d.log", getpid());
        g_log_fd = open(filename, O_CREAT|O_SYNC|O_TRUNC,S_IRUSR|S_IWUSR);
        if(g_log_fd == -1) {
            ERROR("Opening log file failed %d %s", errno, strerror(errno));
        }
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
    FULL_PROFILE_TIMER_SHOW(MPI_Send);
    FULL_PROFILE_TIMER_SHOW(MPI_Recv);
    FULL_PROFILE_TIMER_SHOW(MPI_Isend);
    FULL_PROFILE_TIMER_SHOW(MPI_Irecv);
    FULL_PROFILE_TIMER_SHOW(MPI_Test);
    FULL_PROFILE_TIMER_SHOW(MPI_Testall);
    FULL_PROFILE_TIMER_SHOW(MPI_Wait);
    FULL_PROFILE_TIMER_SHOW(MPI_Waitall);
    FULL_PROFILE_TIMER_SHOW(MPI_Waitany);
    FULL_PROFILE_TIMER_SHOW(MPI_Iprobe);

    FULL_PROFILE_TIMER_SHOW(MPI_Barrier);
    FULL_PROFILE_TIMER_SHOW(MPI_Reduce);
    FULL_PROFILE_TIMER_SHOW(MPI_Allreduce);
    FULL_PROFILE_TIMER_SHOW(MPI_Scan);
    FULL_PROFILE_TIMER_SHOW(MPI_Bcast);
    FULL_PROFILE_TIMER_SHOW(MPI_Scatter);
    FULL_PROFILE_TIMER_SHOW(MPI_Gather);
    FULL_PROFILE_TIMER_SHOW(MPI_Gatherv);
    FULL_PROFILE_TIMER_SHOW(MPI_Allgather);
    FULL_PROFILE_TIMER_SHOW(MPI_Allgatherv);
    FULL_PROFILE_TIMER_SHOW(MPI_Alltoall);

    FULL_PROFILE_TIMER_SHOW(MPI_Other);

    HMPI_STATS_COUNTER_SHOW(send_size);
    HMPI_STATS_COUNTER_SHOW(send_local);
    HMPI_STATS_COUNTER_SHOW(send_remote);
    HMPI_STATS_COUNTER_SHOW(send_imm);
    HMPI_STATS_COUNTER_SHOW(send_syn);
    HMPI_STATS_COUNTER_SHOW(recv_syn);
    HMPI_STATS_COUNTER_SHOW(recv_mem);
    HMPI_STATS_COUNTER_SHOW(recv_anysrc);

#ifdef ENABLE_OPI
    //Doesn't do anything now.
//    OPI_Finalize();
#endif

    //Seems to prevent a segfault in MPI_Finalize()
    MPI_Barrier(HMPI_COMM_WORLD->comm);

    MPI_Finalize();
    return 0;
}



int HMPI_Cart_create(HMPI_Comm comm_old, int ndims, int* dims, int* periods,
        int reorder, HMPI_Comm* comm_cart)
{
    //Allocate a new HMPI communicator.
    HMPI_Comm c = MALLOC(HMPI_Comm_info, 1);

    //Create an MPI comm.
    MPI_Cart_create(comm_old->comm, ndims, dims, periods, reorder, &c->comm);

    //Initialize the rest of the HMPI comm.
    init_communicator(c);

    *comm_cart = c;
    return MPI_SUCCESS;
}

int HMPI_Cart_sub(HMPI_Comm comm, int* remain_dims, HMPI_Comm* newcomm)
{
    //Allocate a new HMPI communicator.
    HMPI_Comm c = MALLOC(HMPI_Comm_info, 1);

    //Create an MPI comm.
    MPI_Cart_sub(comm->comm, remain_dims, &c->comm);

    //Initialize the rest of the HMPI comm.
    init_communicator(c);

    *newcomm = c;
    return MPI_SUCCESS;
}

int HMPI_Comm_create(HMPI_Comm comm, MPI_Group group, HMPI_Comm* newcomm)
{
    //Allocate a new HMPI communicator.
    HMPI_Comm c = MALLOC(HMPI_Comm_info, 1);

    //Create an MPI comm from the group.
    MPI_Comm_create(comm->comm, group, &c->comm);

    //Initialize the rest of the HMPI comm.
    init_communicator(c);

    *newcomm = c;
    return MPI_SUCCESS;
}


int HMPI_Comm_dup(HMPI_Comm comm, HMPI_Comm* newcomm)
{
    //Allocate a new HMPI communicator.
    HMPI_Comm c = MALLOC(HMPI_Comm_info, 1);

    //Duplicate the old comm's MPI comm into the new HMPI comm.
    MPI_Comm_dup(comm->comm, &c->comm);

    //Initialize the rest of the HMPI comm.
    init_communicator(c);

    *newcomm = c;
    return MPI_SUCCESS;
}


int HMPI_Comm_free(HMPI_Comm* comm)
{
    HMPI_Comm c = *comm;

    //Free malloc'd resources on the comm.

    //Free all the MPI communicators (main, node, net, numa).
    MPI_Comm_free(&c->net_comm);
    MPI_Comm_free(&c->node_comm);
    MPI_Comm_free(&c->comm);

    //Free the comm structure itself.
    free(c);
    *comm = HMPI_COMM_NULL;

    return MPI_SUCCESS;
}


int HMPI_Comm_split(HMPI_Comm comm, int color, int key, HMPI_Comm* newcomm)
{
    HMPI_Comm c = MALLOC(HMPI_Comm_info, 1);

    //Split the old comm's MPI comm into the new HMPI comm.
    MPI_Comm_split(comm->comm, color, key, &c->comm);

    //Initialize the rest of the HMPI comm.
    init_communicator(c);

    *newcomm = c;
    return MPI_SUCCESS;
}

