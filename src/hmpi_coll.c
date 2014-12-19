#define _GNU_SOURCE

#define HMPI_INTERNAL   //Disables HMPI->MPI renaming defines
#ifndef HMPIH
#define HMPIH
#include "hmpi.h"
#endif
//#define _PROFILE 1
//#define _PROFILE_HMPI 1
//#define _PROFILE_PAPI_EVENTS 1
#include "profile2.h"
//PROFILE_DECLARE();
//PROFILE_TIMER(allreduce);
#include <malloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "lock.h"
#include "barrier.h"

int32_t sense=1;
int32_t hcsense=1;
//int32_t test=0; //AWF - unused?

//#define FANOUT 2
//#define FANIN 4
//#define CACHE_LINE_SIZE 64

#ifdef DEBUG_LEVEL1
#define DEBUG1(stats) stats
#else
#define DEBUG1(stats)
#endif

#ifdef DEBUG_LEVEL2
#define DEBUG2(stats) stats
#else
#define DEBUG2(stats)
#endif


//barrier_record nodes[g_node_rank];

int NBC_Operation(char *buf3, char *buf1, char *buf2, MPI_Op op, MPI_Datatype type, int count);

//Callback used in barriers to cause MPI library progress while waiting
static inline void barrier_iprobe(void)
{
    int flag;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
}


//
// Collectives
//


/*========================================================================
 * Implementation of a global barrier (tree-based barrier intra-node) with callback function in case
 * of deadlock.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_Barrier(HMPI_Comm comm) {
#ifdef DEBUG
    printf("in HMPI_Barrier\n"); fflush(stdout);
#endif
   // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
    t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);
    if(g_net_size > 1) {
        // all root-threads perform MPI_Barrier 
        if(g_node_rank == 0) {
            MPI_Barrier(comm->comm);
        }

        //barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
        //t_barrier(comm->coll->t_barr, g_node_rank);
        t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);
    }
    return MPI_SUCCESS;
}

/*
 int HMPI_Barrier(HMPI_Comm comm) {
    int i;
//	int sktid = tid/g_numa_size;
    barrier_record * nodes = comm->coll->t_barr->nodes;
    sbarrier_record * skts = comm->coll->t_barr->skts;
//DEBUG1(printf("barrier tid=%d, %d, %d, %d, %d\n",tid,nodes[tid].cnotready[0],nodes[tid].cnotready[1],nodes[tid].cnotready[2],nodes[tid].cnotready[3]);)
    for(i=0; i<FANIN; i++)
      while(nodes[g_node_rank].cnotready[i].cnotready);
    for(i=0; i<FANIN; i++)
       nodes[g_node_rank].cnotready[i].cnotready = nodes[g_node_rank].havechild[i];
    *nodes[g_node_rank].parentpointer = 0;

    if(g_node_rank == g_numa_root)
	{
      for(i=0; i<SFANIN; i++)
        while(skts[g_numa_node].cnotready[i].cnotready);
	  for(i=0; i<SFANIN; i++)
	    skts[g_numa_node].cnotready[i].cnotready = skts[g_numa_node].havechild[i];
      *skts[g_numa_node].parentpointer = 0;
	}

    if(g_net_size > 1&&g_node_rank == 0) {
            MPI_Barrier(comm->comm);
        }

	if(SFANOUT>0)
    {
     if(g_node_rank == g_numa_root && g_node_rank != 0)
     	{
  	     while(skts[g_numa_node].wsense.wsense != sense);
  	       barrier_iprobe();
      	}
  
  	  if(g_node_rank == g_numa_root)
  	    for(i=0; i<SFANOUT; i++)
          *skts[g_numa_node].childpointer[i] = sense;
  	
      if(g_node_rank != g_numa_root)
       {  
  	    while(nodes[g_node_rank].wsense.wsense != sense);
  	  //     barrier_iprobe();
       }
  	  for(i=0; i<FANOUT; i++)
        *nodes[g_node_rank].childpointer[i] = sense;
      //STORE_FENCE();
      sense = sense == 0? 1: 0;
	}

	if(SFANOUT == 0)
	{
      if(g_node_rank != 0)
      {  
        while(nodes[g_node_rank].wsense.wsense != sense);
  	   //    barrier_iprobe();
      }
      for(i=0; i<FANOUT; i++)
        *nodes[g_node_rank].childpointer[i] = sense;
      //STORE_FENCE();
      sense = sense == 0? 1: 0;
	}
    return MPI_SUCCESS;
}*/

int HMPI_AllreduceTestFanout1(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    //PROFILE_INIT(g_node_rank);
    //PROFILE_START(allreduce);
    
    double begin; double elapse;
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;

    unsigned long long i;
    unsigned long long *r, *s;
    r=(unsigned long long *)recvbuf;
    s=(unsigned long long *)sendbuf;

    t_barrier(comm->coll->t_barr, g_node_rank);

    begin = MPI_Wtime();
	if(g_node_rank != 0)
    {
	    for(i=0; i<98304; i++)
        {
          r[i] =  *(((unsigned long long *)(comm->coll->sbuf[0]))+i);
        }
	}
    
    t_barrier(comm->coll->t_barr, g_node_rank);
   	  elapse = (MPI_Wtime() - begin);
	  printf("intra-node fanout thread = %d, datasize = %d, time = %12.8f\n", g_node_rank,count,elapse);
    return MPI_SUCCESS;
}

int HMPI_AllreduceTestFanout2(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    //PROFILE_INIT(g_node_rank);
    //PROFILE_START(allreduce);
    
    double begin; double elapse;
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;

    unsigned long long i;
    unsigned long long *r, *s;
    r=(unsigned long long *)recvbuf;
    s=(unsigned long long *)sendbuf;

    t_barrier(comm->coll->t_barr, g_node_rank);

    begin = MPI_Wtime();
	    for(i=0; i<98304; i++)
        {
          r[i] =  *(((unsigned long long *)(comm->coll->sbuf[g_numa_root]))+i);
        }
    
    t_barrier(comm->coll->t_barr, g_node_rank);
   	  elapse = (MPI_Wtime() - begin);
	  printf("intra-socket fanout thread = %d, datasize = %d, time = %12.8f\n", g_node_rank,count,elapse);
    return MPI_SUCCESS;
}


/*========================================================================
 * Implement allreduce as tiled-reduce and broadcast to have good cache performance.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AllreduceTiledRed(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    
    int i,j,size;
    MPI_Type_size(datatype, &size);
    //to simplify, now only support threads number is the fold of sockets number
	//number of threads per socket
    //int g_numa_size = g_node_size/sockets;
	//int g_numa_node = g_node_rank/g_numa_size;

	//root thread in each socket
    //int g_numa_root = g_node_rank/g_numa_size*g_numa_size;

	// dimensions of sockets in hypercube
	int k=(int)sqrt((double)sockets);

    //to utilize L3 cache(each socket share 12MB L3 cache), 
    //first fetch total 4.5MB data in send buffer into each L3 cache
    //so the step is approximately 128KB when there are 6 threads on each socket
    //calculate the block size ofter partition
    int stride1,stride2,chunksize1,chunksize2,chunks,remain1,remain2;
    void *tmp;
    stride1 = 128*1024/size;
	//when data size larger than one chunk
    if(count >= stride1*g_numa_size)
    {
      chunksize1 = stride1*g_numa_size;
	  remain1 =0;
      chunks = count/chunksize1;
      chunksize2 = count-chunksize1*chunks;
	  if(chunksize2 > 0)
	  {
       stride2 = chunksize2/g_numa_size;
	   remain2 = chunksize2 - stride2*g_numa_size;
	   }
    }
	//when data size smaller than one chunk
    else if( count>= g_node_size )
    {
     stride1 = count/g_numa_size;
	 remain1 = count - stride1*g_numa_size;
     chunks = 1;
     chunksize1 = count;
     chunksize2 = 0;
    }
	//for small data size, actually can use tree algorithm instead
	else
	{

      if(g_node_rank%g_numa_size == 0)
      {  
        tmp = (void *)malloc(size*count);
        comm->coll->tmp[g_node_rank] = tmp;
      }

      comm->coll->sbuf[g_node_rank]=sendbuf;
      comm->coll->rbuf[g_node_rank]=recvbuf;
      t_barrier(comm->coll->t_barr, g_node_rank);
	   if(g_node_rank%g_numa_size == 0)
	   for(j=g_node_rank; j<g_node_rank+g_numa_size; j++)
	   {
	    if(j == g_node_rank)
	       NBC_Operation((char *)comm->coll->tmp[g_node_rank],
		            (char *)comm->coll->sbuf[j], 
		            (char *)comm->coll->sbuf[j+1], op, datatype, count);
	    else if(j == g_node_rank+1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_node_rank], 
		            (char *)comm->coll->tmp[g_node_rank], 
	                    (char *)comm->coll->sbuf[j], op, datatype, count);
       }

     t_barrier(comm->coll->t_barr, g_node_rank);

     if(g_net_size > 1)
	 {
		if(k == 0)
          if(g_node_rank==0)
		  {
            MPI_Allreduce((char *)tmp, (char *)recvbuf, count, datatype, op, comm->comm);
		  }
//	    else if(k == 1)
//          if(g_node_rank==0)
//	      {  
//           NBC_Operation((char *)comm->coll->rbuf[g_node_rank], (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, count);
//           MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
//	      }
	    if(k > 0)
			{ 
			  int l;
			//  if(g_node_rank%g_numa_size == 0)
			//  {
			    for( l=0; l<k; l++ )
				{
				    //use receive buffer and tmp buffer interchangeably
			     if(g_node_rank%g_numa_size == 0 && (g_numa_node%(int)pow(2,l)) == 0)
				 {
					if(l%2 == 0)
			      	  NBC_Operation((char *)recvbuf, (char*)comm->coll->tmp[g_node_rank], (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l)))], op, datatype, count);
					else
			      	  NBC_Operation((char *)tmp, (char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l)))], op, datatype, count);
			     }
                    t_barrier(comm->coll->t_barr, g_node_rank);
				}
			   //l%2 == 0, final results stored in tmp buffer, else in receive buffer
			   //if(g_node_rank%g_numa_size == 0)
			   if(g_node_rank == 0)
			   {
			     if(l%2 == 0)
			     {
                   MPI_Allreduce((char *)tmp, (char *)recvbuf, count, datatype, op, comm->comm);
			     }
			     else
                   MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf, count, datatype, op, comm->comm);
			   }
			//  }
			}
       t_barrier(comm->coll->t_barr, g_node_rank);

       //if(g_node_rank%g_numa_size!=0)
       if(g_node_rank!=0)
         //memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[g_numa_root], count*size);
         memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[0], count*size);
	 }
     else
     {
		if(k == 0)
			memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->tmp[k], count*size);
		else if(k == 1)
    		NBC_Operation((char *)comm->coll->rbuf[g_node_rank], (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, count);
		else
			{ 
			  int l;
			  //allreduce among sockets, using hypercube
			  //can also be changed to reduce and then broadcast, by setting g_numa_node equal to power of 2
			    for( l=0; l<k; l++ )
				{
				    //use receive buffer and tmp buffer interchangeably
			      if(g_node_rank%g_numa_size == 0)
				  {
				   if(l%2 == 0)
			      	  NBC_Operation(recvbuf, (char*)comm->coll->tmp[g_node_rank], (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l)))], op, datatype, count);
					else
			      	  NBC_Operation(tmp, (char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l)))], op, datatype, count);
				   }
                    t_barrier(comm->coll->t_barr, g_node_rank);
				}
			   //l%2 == 0, final results stored in tmp buffer, else in receive buffer
			   if(l%2 == 0)
				 memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->tmp[g_numa_root], count*size);
			   else
			     if(g_node_rank%g_numa_size != 0)
				 memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[g_numa_root], count*size);
			}
	 }
     t_barrier(comm->coll->t_barr, g_node_rank);

     if(g_node_rank%g_numa_size == 0)
     free(tmp);
     return MPI_SUCCESS;
	}


	//printf("stride1=%d,stride2=%d,chunksize1=%d,chunksize2=%d,chunks=%d,remain1=%d,remain2=%d \n",stride1,stride2,chunksize1,chunksize2,chunks,remain1,remain2);

    //printf("g_numa_size=%d, threads=%d, stride=%d, chunksize=%d, chunks=%d, remain=%d\n",g_numa_size, g_node_size,stride, chunksize, chunks, remain);
    //MPI_Type_get_extent(datatype, &lb, &extent);
    //MPI_Type_extent(datatype, &extent);

#if 0
    if(extent != size) {
        printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
        fflush(stdout);
        MPI_Abort(comm->comm, 0);
    }
#endif
//printf("stride1=%d,stride2=%d,chunksize1=%d,chunksize2=%d,chunks=%d,remain1=%d,remain2=%d,g_numa_rank=%d,g_numa_size=%d,g_numa_root=%d, g_node_rank=%d \n",stride1,stride2,chunksize1,chunksize2,chunks,remain1,remain2,g_numa_rank,g_numa_size,g_numa_root,g_node_rank);
	DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)
    //collect the addresses of send buffer and recieve buffer

    if(g_node_rank%g_numa_size == 0)
    {  
      tmp = (void *)malloc(size*chunksize1);
      comm->coll->tmp[g_node_rank] = tmp;
    }
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;

    t_barrier(comm->coll->t_barr, g_node_rank);
    //barrier(&comm->barr, g_node_rank);
	
    for(i=0; i<chunks; i++)
    {

	   if(g_node_rank%g_numa_size == g_numa_size-1)
	   for(j=0; j<g_numa_size; j++)
	   {
	    if(j == 0)
	       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size,
		            (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size,
					(char *)comm->coll->sbuf[j+g_numa_root+1]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
	    else if(j == 1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, 
		            (char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, 
                    (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
       }
	   else
	   for(j=0; j<g_numa_size; j++)
	   {
	    if(j == 0)
	       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size,
		            (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size,
					(char *)comm->coll->sbuf[j+g_numa_root+1]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
	    else if(j == 1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, 
		            (char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, 
                    (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
       }
    //barrier(&comm->barr, g_node_rank);
     t_barrier(comm->coll->t_barr, g_node_rank);
     
     if(g_net_size > 1)
	 {
       if(k == 0)
         if(g_node_rank==0)
         {
               MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
         }
	  // if(k == 1)
	  // {
      //   if(g_node_rank<g_numa_size)
	  //    if(g_node_rank == g_numa_size-1)
	  //     NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride1*size+chunksize1*i*size,
	  //                  (char *)comm->coll->tmp[0]+g_node_rank*stride1*size,
	  //  	            (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride1*size, op, datatype, stride1+remain1);
	  //    else
	  //    NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride1*size+chunksize1*i*size,
	  //                  (char *)comm->coll->tmp[0]+g_node_rank*stride1*size,
	  //                  (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride1*size, op, datatype, stride1);
      //  t_barrier(comm->coll->t_barr, g_node_rank);
	  //  if(g_node_rank==0)
	  //  {  
      //   MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	  //  }
	  // }
	  if(k > 0)
	   {
		 int l;
		   for( l=0; l<k; l++ )
		   {
			//do a inter-socket reduce in hypercube, use receive buffer and tmp buffer interchangeably
			if( g_numa_node%((int)pow(2,l)) == 0 )
			{
			  if(l%2 == 0)
			  {
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1+remain1);
                else
			    NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1);
			  }
			  else
			  {
			    
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
                else
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
			  
			  }
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }
	   if(g_node_rank == 0)
	 	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
			
//			t_barrier(comm->coll->t_barr, g_node_rank);
//	   if(g_node_rank == g_numa_size)
//	 	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	 //  if(g_node_rank%g_numa_size == 0)
	 //  if(l%2 == 0)
	 //       MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	 //  else
	 // 	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
       
	   }

	   t_barrier(comm->coll->t_barr, g_node_rank);
       if(g_node_rank != 0)
       memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize1*size);
      // if(g_node_rank != 0)
      // memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize1*size);
	 }

     else
	 {
       if(k == 0)
         memcpy((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], chunksize1*size);
	   //else if(k == 1)
       // NBC_Operation((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, chunksize1);
	   else
	   {
		   int l;
		   for( l=0; l<k;l++ )
		   {
			//do a inter-socket allreduce in hypercube, use receive buffer and tmp buffer interchangeably
			if(l%2 == 0)
			{
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1+remain1);
              else
			  NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1);
			}
			else
			{
			  
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
              else
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
			
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }
		if(l%2 == 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root], chunksize1*size);
		else
          if(g_node_rank%g_numa_size != 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_root]+chunksize1*i*size, chunksize1*size);
	   } 
	 }

     t_barrier(comm->coll->t_barr, g_node_rank);
    
    }
  

    if(chunksize2 >0)
    {
	   if(g_node_rank%g_numa_size == g_numa_size-1)
	   for(j=0; j<g_numa_size; j++)
	   {
	    if(j == 0)
	       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size,
		            (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size,
					(char *)comm->coll->sbuf[j+g_numa_root+1]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
	    else if(j == 1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, 
		            (char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, 
                    (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
       }
	   else
	   for(j=0; j<g_numa_size; j++)
	   {
	    if(j == 0)
	       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size,
		            (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size,
					(char *)comm->coll->sbuf[j+g_numa_root+1]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
	    else if(j == 1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, 
		            (char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, 
                    (char *)comm->coll->sbuf[j+g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
       }
    //barrier(&comm->barr, g_node_rank);
     t_barrier(comm->coll->t_barr, g_node_rank);
     
     if(g_net_size > 1)
	 {
       if(k == 0)
         if(g_node_rank==0)
         {
               MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
         }
	  //  if(k == 1)
	  // {
      //   if(g_node_rank<g_numa_size)
	  //    if(g_node_rank == g_numa_size-1)
	  //     NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride2*size+chunksize1*i*size,
	  //                  (char *)comm->coll->tmp[0]+g_node_rank*stride2*size,
	  //  	            (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride2*size, op, datatype, stride2+remain2);
	  //    else
	  //    NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride2*size+chunksize1*i*size,
	  //                  (char *)comm->coll->tmp[0]+g_node_rank*stride2*size,
	  //                  (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride2*size, op, datatype, stride2);
      //  t_barrier(comm->coll->t_barr, g_node_rank);
	  //  if(g_node_rank==0)
	  //  {  
      //   MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	  //  }
	  // }
	  if(k > 0)
	   {
		 int l;
		   for( l=0; l<k; l++ )
		   {
			//do a inter-socket reduce in hypercube, use receive buffer and tmp buffer interchangeably
			if(l%2 == 0)
			{
			  if( g_numa_node%((int)pow(2,l)) == 0 )
			  {
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2+remain2);
                else
			    NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2);
			  }
			}
			else
			{
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
              else
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }

	   //t_barrier(comm->coll->t_barr, g_node_rank);
	   if(g_node_rank == 0)
	  	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);

	 //  t_barrier(comm->coll->t_barr, g_node_rank);
	 //  if(g_node_rank == g_numa_size)
	 // 	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	  // if(g_node_rank%g_numa_size == 0)
	  // if(l%2 == 0)
	  //      MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	  // else
	  //	    MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
       
	   }

	   t_barrier(comm->coll->t_barr, g_node_rank);
      // if(g_node_rank!= 0)
      // memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize2*size);
       if(g_node_rank != 0)
       memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize2*size);
	 }

     else
	 {
       if(k == 0)
         memcpy((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], chunksize2*size);
	//   if(k == 1)
    //	NBC_Operation((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, chunksize2);
	  if(k>0)
	   {
		   int l;
		   for( l=0; l<k;l++ )
		   {
			//do a inter-socket allreduce in hypercube, use receive buffer and tmp buffer interchangeably
			if(l%2 == 0)
			{
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2+remain2);
              else
			  NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2);
			}
			else
			{
			  
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
              else
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*(g_numa_node^((int)pow(2,l )))]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
			
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }
		if(l%2 == 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root], chunksize2*size);
		else
          if(g_node_rank%g_numa_size != 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_root]+chunksize1*i*size, chunksize2*size);
	   } 
	 }

     t_barrier(comm->coll->t_barr, g_node_rank);

    } 

   if(g_node_rank%g_numa_size == 0)
   free(tmp);

   DEBUG3(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    SHOW(g_node_rank, g_numa_size);
    PROFILE_SHOW_REDUCE(allreduce);
	)
    return MPI_SUCCESS;
}


//only for Xeon X5650
/*
int HMPI_AllreduceHyperCube(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    int size;
    MPI_Type_size(datatype, &size);
    void *tmp = (void *)malloc(size*count);
    DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)

    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
    comm->coll->tmp[g_node_rank]=tmp;
    t_barrier(comm->coll->t_barr, g_node_rank);
    NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[g_node_rank^1], op, datatype, count);
    t_barrier(comm->coll->t_barr, g_node_rank);
    switch(g_node_rank%6){
    case 0:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank+4], op, datatype, count);
    break;
    case 1:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank+4], op, datatype, count);
    break;
    case 2:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    break;
    case 3:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    break;
    case 4:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-4], op, datatype, count);
    break;
    case 5:
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-4], op, datatype, count);
    break;
    }

    t_barrier(comm->coll->t_barr, g_node_rank);
    if(g_node_rank<6)
    NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[g_node_rank+6], op, datatype, count);
    else
    NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[g_node_rank-6], op, datatype, count);
    t_barrier(comm->coll->t_barr, g_node_rank);
    free (tmp);
    DEBUG3(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    PROFILE_SHOW_REDUCE(allreduce);
	)
    return MPI_SUCCESS;
}*/
/*========================================================================
 * Implement allreduce as Hypercube. To have good cache performance, allreduce is done chunk by chunk.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AllreduceHypercubeChunks(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    int i,j,size;
    MPI_Type_size(datatype, &size);
    
    //to simplify, now only support threads number is the fold of sockets numberi
	//number of threads per socket
    //int g_numa_size = g_node_size/sockets;
	// g_numa_node
	//int g_numa_node = g_node_rank/g_numa_size;
	//intra-socket thread id
    //int g_numa_rank = g_node_rank%g_numa_size;

	//root thread in each socket
    //int g_numa_root = g_node_rank/g_numa_size*g_numa_size;

	// dimensions of sockets
	int k=(int)(log((double)sockets)/log(2.0));
    // dimentions of intra-socket threads
	int t=(int)(log((double)g_numa_size)/log(2.0));

    //to utilize L3 cache(each socket share 12MB L3 cache), 
    //first fetch total 4.5MB data in send buffer into each L3 cache
    //so the step is approximately 128KB when there are 6 threads on each socket
    //calculate the block size ofter partition
    int stride1=0;
	int stride2=0;
	int chunksize1=0;
	int chunksize2=0;
	int chunks=0;
	int remain1=0;
	int remain2=0;
    void *tmp;
    stride1 = 128*1024/size;
	//when data size larger than one chunk
    if(count >= stride1*g_numa_size)
    {
      chunksize1 = stride1*g_numa_size;
	  remain1 =0;
      chunks = count/chunksize1;
	  chunksize2 = count-chunksize1*chunks;
	  if(chunksize2 > 0)
	  {
       stride2 = chunksize2/g_numa_size;
	   remain2 = chunksize2 - stride2*g_numa_size;
	   }
    }
	//when data size smaller than one chunk
    else if( count>= g_node_size )
    {
     stride1 = count/g_numa_size;
     remain1 = count - stride1*g_numa_size;
	 chunks = 1;
     chunksize1 = count;
     chunksize2 = 0;
	 
    }
	//for small data size, actually can use tree algorithm instead
	else
	{

      if(g_node_rank%g_numa_size == 0)
      {  
        tmp = (void *)malloc(size*count);
        comm->coll->tmp[g_node_rank] = tmp;
      }

      comm->coll->sbuf[g_node_rank]=sendbuf;
      comm->coll->rbuf[g_node_rank]=recvbuf;

      t_barrier(comm->coll->t_barr, g_node_rank);

	   if(g_node_rank%g_numa_size == 0)
	   for(j=g_node_rank; j<g_node_rank+g_numa_size; j++)
	   {
	    if(j == g_node_rank)
	       NBC_Operation((char *)comm->coll->tmp[g_node_rank],
		            (char *)comm->coll->sbuf[j], 
		            (char *)comm->coll->sbuf[j+1], op, datatype, count);
	    else if(j == g_node_rank+1) continue;
	    else 
	      NBC_Operation((char *)comm->coll->tmp[g_node_rank], 
		            (char *)comm->coll->tmp[g_node_rank], 
	                    (char *)comm->coll->sbuf[j], op, datatype, count);
       }

     t_barrier(comm->coll->t_barr, g_node_rank);

     if(g_net_size > 1)
	 {
		if(k == 0)
          if(g_node_rank==0)
		  {
            MPI_Allreduce((char *)tmp, (char *)recvbuf, count, datatype, op, comm->comm);
		  }
//	    else if(k == 1)
//          if(g_node_rank==0)
//	      {  
//           NBC_Operation((char *)comm->coll->rbuf[g_node_rank], (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, count);
//           MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
//	      }
		if(k > 0)
			{ 
			  int l, n;
			  //inter-socket, root threads do allreduce using dissemination
			    for( l=0; l<k; l++ )
				{
				    //use receive buffer and tmp buffer interchangeably
					n = g_numa_node - (int)pow(2,l);
					if(n < 0)
					  n += sockets;
					
			     if(g_node_rank%g_numa_size == 0 && (g_numa_node%(int)pow(2,l)) == 0)
				 {
					if(l%2 == 0)
			      	  NBC_Operation((char *)recvbuf, (char*)comm->coll->tmp[g_node_rank], (char*)comm->coll->tmp[g_numa_size*n], op, datatype, count);
					else
			      	  NBC_Operation((char *)tmp, (char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[g_numa_size*n], op, datatype, count);
			     }
                    t_barrier(comm->coll->t_barr, g_node_rank);
				}
			   //if l%2 == 0, final results stored in tmp buffer, else in receive buffer
			   if(g_node_rank == 0)
			   {
			     if(l%2 == 0)
                   MPI_Allreduce(tmp, recvbuf, count, datatype, op, comm->comm);
			     else
                   MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
			   }
			}
       t_barrier(comm->coll->t_barr, g_node_rank);

       if(g_node_rank!=0)
         memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[0], count*size);
	 }
     else
     {
		if(k == 0)
			memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->tmp[k], count*size);
		else if(k == 1)
    		NBC_Operation((char *)comm->coll->rbuf[g_node_rank], (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, count);
		else
			{ 
			   
			  int l, n;
			  //inter-socket, root threads do allreduce using dissemination
			    for( l=0; l<k; l++ )
				{
				    //use receive buffer and tmp buffer interchangeably
			        if(g_node_rank%g_numa_size == 0)
				  {
					n = g_numa_node - (int)pow(2,l);
					if(n < 0)
					  n += sockets;

					if(l%2 == 0)
			      	  NBC_Operation(recvbuf, (char*)comm->coll->tmp[g_node_rank], (char*)comm->coll->tmp[g_numa_size*n], op, datatype, count);
					else
			      	  NBC_Operation(tmp, (char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[g_numa_size*n], op, datatype, count);               }
                    t_barrier(comm->coll->t_barr, g_node_rank);
				}
			   
			   //l%2 == 0, final results stored in tmp buffer of root thread, else in receive buffer of root thread
			   if(l%2 == 0)
				 memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->tmp[g_numa_root], count*size);
			   else
			     if(g_node_rank%g_numa_size != 0)
				 memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[g_numa_root], count*size);
			}
	 }
     t_barrier(comm->coll->t_barr, g_node_rank);

     if(g_node_rank%g_numa_size == 0)
     free(tmp);
     return MPI_SUCCESS;
	}

#if 0
    if(extent != size) {
        printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
        fflush(stdout);
        MPI_Abort(comm->comm, 0);
    }
#endif
//printf("stride1=%d,stride2=%d,chunksize1=%d,chunksize2=%d,chunks=%d,remain1=%d,remain2=%d,g_numa_rank=%d,g_numa_size=%d,g_numa_root=%d, g_node_rank=%d, t=%d, k=%d,count =%d \n",stride1,stride2,chunksize1,chunksize2,chunks,remain1,remain2,g_numa_rank,g_numa_size,g_numa_root,g_node_rank,t,k,count);
	DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)
    //collect the addresses of send buffer and recieve buffer

    if(g_node_rank%g_numa_size == 0)
    {  
      tmp = (void *)malloc(size*chunksize1);
      comm->coll->tmp[g_node_rank] = tmp;
    }
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;

    t_barrier(comm->coll->t_barr, g_node_rank);
    //barrier(&comm->barr, g_node_rank);
	
	int l, n;
	int offset = 0;
	int bcount = 0;
    for(i=0; i<chunks; i++)
    {
	  //intra-socket dissemination
	    offset = 0;
		for( l=0; l<t; l++ )
		{
		    //use receive buffer and tmp buffer interchangeably

			//size of each block
			bcount = (g_numa_size/(int)pow(2, l+1))*stride1;

			//offset
	        offset += (g_numa_rank / (int)pow(2, l) + 1)%2*bcount;

			//handle the remainder
			if(g_numa_rank%(int)pow(2, l+1) == 0)
			  bcount += remain1;

			n = g_numa_rank - (int)pow(2,l);
			if(n < 0)
			  n += g_numa_size;

			if(l == 0)
	      	  NBC_Operation((char *)recvbuf+chunksize1*i*size+offset*size, (char *)comm->coll->sbuf[g_node_rank]+chunksize1*i*size+offset*size, (char *)comm->coll->sbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
			else if(l == t-1)
	      	  NBC_Operation((char *)comm->coll->tmp[g_numa_root]+offset*size, (char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
			else
	      	  NBC_Operation((char *)recvbuf+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
            t_barrier(comm->coll->t_barr, g_node_rank);
		}
    //barrier(&comm->barr, g_node_rank);
     t_barrier(comm->coll->t_barr, g_node_rank);
     
     if(g_net_size > 1)
	 {
       if(k == 0)
         if(g_node_rank==0)
         {
               MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
         }
	   //else if(k == 1)
	   //{
       //  if(g_node_rank<g_numa_size)
	   //   if(g_node_rank == g_numa_size-1)
	   //    NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride1*size+chunksize1*i*size,
	   //                 (char *)comm->coll->tmp[0]+g_node_rank*stride1*size,
	   // 	            (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride1*size, op, datatype, stride1+remain1);
	   //   else
	   //   NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride1*size+chunksize1*i*size,
	   //                 (char *)comm->coll->tmp[0]+g_node_rank*stride1*size,
	   //                 (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride1*size, op, datatype, stride1);
       // t_barrier(comm->coll->t_barr, g_node_rank);
	   // if(g_node_rank==0)
	   // {  
       //  MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	   // }
	   //}
	   if (k > 0)
	   {
		 int l, n;
		   for( l=0; l<k; l++ )
		   {
			//do a inter-socket allreduce in dissemination, use receive buffer and tmp buffer interchangeably
			n = g_numa_node - (int)pow(2,l);
			if(n < 0)
			  n += sockets;
			if( g_numa_node%((int)pow(2,l)) == 0 )
			{
			  if(l%2 == 0)
			  {
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1+remain1);
                else
			    NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1);
			  }
			  else
			  {
			    
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
                else
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
			  
			  }
		    }
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }

	   if(g_node_rank == 0)
	   {  
	     if(l%2 == 0)
	          MPI_Allreduce(tmp, recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	     else
	    	  MPI_Allreduce(MPI_IN_PLACE, recvbuf+chunksize1*i*size, chunksize1, datatype, op, comm->comm);
	   }
	  }

	   t_barrier(comm->coll->t_barr, g_node_rank);
       if(g_node_rank != 0)
       memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize1*size);
	 }

     else
	 {
       if(k == 0)
         memcpy((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], chunksize1*size);
	   else if(k == 1)
    	NBC_Operation((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, chunksize1);
	   else
	   {
		   int l, n;
		   for( l=0; l<k;l++ )
		   {
			//do a inter-socket allreduce in dissemination, use receive buffer and tmp buffer interchangeably
			n = g_numa_node - (int)pow(2,l);
			if(n < 0)
			  n += sockets;
			if(l%2 == 0)
			{
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1+remain1);
              else
			  NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size, op, datatype, stride1);
			}
			else
			{
			  
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1+remain1);
              else
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride1*size+chunksize1*i*size, op, datatype, stride1);
			
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }
		if(l%2 == 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root], chunksize1*size);
		else
          if(g_node_rank%g_numa_size != 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_root]+chunksize1*i*size, chunksize1*size);
	   } 
	 }

     t_barrier(comm->coll->t_barr, g_node_rank);
    
    }
  

    if(chunksize2 >0)
    {
	    offset = 0;
		for( l=0; l<t; l++ )
		{
		    //use receive buffer and tmp buffer interchangeably

			//size of each block
			bcount = (g_numa_size/(int)pow(2, l+1))*stride2;

			//offset
	        offset += (g_numa_rank / (int)pow(2, l) + 1)%2*bcount;

			//handle the remainder
			if(g_numa_rank%(int)pow(2, l+1) == 0)
			  bcount += remain2;

			n = g_numa_rank - (int)pow(2,l);
			if(n < 0)
			  n += g_numa_size;

			if(l == 0)
	      	  NBC_Operation((char *)recvbuf+chunksize1*i*size+offset*size, (char *)comm->coll->sbuf[g_node_rank]+chunksize1*i*size+offset*size, (char *)comm->coll->sbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
			else if(l == t-1)
	      	  NBC_Operation((char *)comm->coll->tmp[g_numa_root]+offset*size, (char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
			else
	      	  NBC_Operation((char *)recvbuf+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size+offset*size, (char*)comm->coll->rbuf[n]+chunksize1*i*size+offset*size, op, datatype, bcount);
            t_barrier(comm->coll->t_barr, g_node_rank);
		}
    //barrier(&comm->barr, g_node_rank);
    // t_barrier(comm->coll->t_barr, g_node_rank);
     
     if(g_net_size > 1)
	 {
       if(k == 0)
         if(g_node_rank==0)
         {
               MPI_Allreduce((char *)tmp, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
         }
	   //else if(k == 1)
	   //{
       //  if(g_node_rank<g_numa_size)
	   //   if(g_node_rank == g_numa_size-1)
	   //    NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride2*size+chunksize1*i*size,
	   //                 (char *)comm->coll->tmp[0]+g_node_rank*stride2*size,
	   // 	            (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride2*size, op, datatype, stride2+remain2);
	   //   else
	   //   NBC_Operation((char *)comm->coll->rbuf[0]+g_node_rank*stride2*size+chunksize1*i*size,
	   //                 (char *)comm->coll->tmp[0]+g_node_rank*stride2*size,
	   //                 (char *)comm->coll->tmp[g_numa_size]+g_node_rank*stride2*size, op, datatype, stride2);
       // t_barrier(comm->coll->t_barr, g_node_rank);
	   // if(g_node_rank==0)
	   // {  
       //  MPI_Allreduce(MPI_IN_PLACE, (char *)recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	   // }
	   //}
	   if(k > 0)
	   {
		 int l, n;
		   for( l=0; l<k; l++ )
		   {
			//do a inter-socket allreduce in dissemination, use receive buffer and tmp buffer interchangeably
			n = g_numa_node - (int)pow(2,l);
			if(n < 0)
			  n += sockets;
			if( g_numa_node%((int)pow(2,l)) == 0 )
			{  
			  if(l%2 == 0)
			  {
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2+remain2);
                else
			    NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2);
			  }
			  else
			  {
                if(g_node_rank%g_numa_size == g_numa_size-1)
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
                else
			       NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
			  }
		   }
			t_barrier(comm->coll->t_barr, g_node_rank);
		 }

	   if(g_node_rank == 0)
	   {
	     if(l%2 == 0)
	          MPI_Allreduce(tmp, recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	     else
	    	  MPI_Allreduce(MPI_IN_PLACE, recvbuf+chunksize1*i*size, chunksize2, datatype, op, comm->comm);
	   }
       
	   }

	   t_barrier(comm->coll->t_barr, g_node_rank);
       if(g_node_rank != 0)
       memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[0]+chunksize1*i*size, chunksize2*size);
	 }

     else
	 {
       if(k == 0)
         memcpy((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], chunksize2*size);
	   else if(k == 1)
    	NBC_Operation((char *)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char *)comm->coll->tmp[0], (char *)comm->coll->tmp[g_numa_size], op, datatype, chunksize2);
	   else
	   {
		   int l, n;
		   for( l=0; l<k;l++ )
		   {
			//do a inter-socket allreduce in dissemination, use receive buffer and tmp buffer interchangeably
			n = g_numa_node - (int)pow(2,l);
			if(n < 0)
			  n += sockets;
			if(l%2 == 0)
			{
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2+remain2);
              else
			  NBC_Operation((char *)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->tmp[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size, op, datatype, stride2);
			}
			else
			{
			  
              if(g_node_rank%g_numa_size == g_numa_size-1)
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2+remain2);
              else
			     NBC_Operation((char *)comm->coll->tmp[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size, (char*)comm->coll->rbuf[g_numa_root]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_size*n]+(g_node_rank%g_numa_size)*stride2*size+chunksize1*i*size, op, datatype, stride2);
			
			}
			t_barrier(comm->coll->t_barr, g_node_rank);
		   }
		if(l%2 == 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->tmp[g_numa_root], chunksize2*size);
		else
          if(g_node_rank%g_numa_size != 0)
          memcpy((char*)comm->coll->rbuf[g_node_rank]+chunksize1*i*size, (char*)comm->coll->rbuf[g_numa_root]+chunksize1*i*size, chunksize2*size);
	   } 
	 }

     t_barrier(comm->coll->t_barr, g_node_rank);

    } 

   if(g_node_rank%g_numa_size == 0)
   free(tmp);

   DEBUG3(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    SHOW(g_node_rank, g_numa_size);
    PROFILE_SHOW_REDUCE(allreduce);
	)
    return MPI_SUCCESS;

}


int HMPI_ReduceTree_sub(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,int root, HMPI_Comm comm)
{
    int i;
    int size;
	int tag = 0;
    barrier_record * nodes = comm->coll->t_barr->nodes;
    sbarrier_record * skts = comm->coll->t_barr->skts;
   
    MPI_Type_size(datatype, &size);
    DEBUG2(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)
   
    DEBUG3( double begin; double elapse; )

    //collect the addresses of send buffer and recieve buffer
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
   if(!nodes[g_node_rank].leafbool) 
       recvbuf=(uint64_t *)malloc(sizeof(uint64_t)*count);
    //corresponding to the number of FANIN, after all the child nodes arriving, parent node begins to do the following computation
    DEBUG3(
    //if(g_node_rank == 0)
    begin = MPI_Wtime();
   )
    for(i=FANIN-1; i>=0; i--)
    {
	  while(nodes[g_node_rank].cnotready[i].cnotready);
      if(nodes[g_node_rank].havechild[i]>0)
       {
		 tag++;
         nodes[g_node_rank].cnotready[i].cnotready = nodes[g_node_rank].havechild[i];
	     if(tag==1)
    	   //distinguish leaf nodes and other nodes to reduce extra memcpy overhead
	       if(nodes[nodes[g_node_rank].havechild[i]].leafbool) 
             NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
           else
	         NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
	     else
	       if(nodes[nodes[g_node_rank].havechild[i]].leafbool) 
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
	       else  
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
       }
    }
    *nodes[g_node_rank].parentpointer = 0;

	if( g_node_rank == g_numa_root)
	{
	  tag = 0;
      for(i=SFANIN-1; i>=0; i--)
      {
	    while(skts[g_numa_node].cnotready[i].cnotready);
        if(skts[g_numa_node].havechild[i]>0)
        {
		 tag++;
         skts[g_numa_node].cnotready[i].cnotready = skts[g_numa_node].havechild[i];

	      if(g_node_size <= sockets)
	      {
	       if(tag==1)
	         if(skts[skts[g_numa_node].havechild[i]].leafbool) 
               NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
             else
	           NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	       else
	         if(skts[skts[g_numa_node].havechild[i]].leafbool) 
	           NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->sbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	         else  
	           NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	       }
	      else
	          NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
       }
      }
      *skts[g_numa_node].parentpointer = 0;
	}
    DEBUG3(
     if(g_node_rank == 0)
	 {
    	elapse = (MPI_Wtime() - begin);
	    printf("thread = %d, step1 time = %12.8f\n", g_node_rank, elapse);
	 }
    )

    //do mpi_allreduce if there are more than one MPI processes
     if(g_net_size > 1 && g_node_rank==0)
       MPI_Reduce(recvbuf, sendbuf, count, datatype, op,root, comm->comm);

    // if(g_node_rank==1 && g_net_size > 1)
    //   MPI_Allreduce(MPI_IN_PLACE, sendbuf, count, datatype, op, comm->comm);

   // DEBUG3(
    //begin = MPI_Wtime();
   //)
   if(SFANOUT>0)
   { //inter-socket FANOUT
	if(g_node_rank == g_numa_root && g_node_rank != 0)
	{
	  while(skts[g_numa_node].wsense.wsense != sense);
      //memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[skts[g_numa_node].parentindx*g_numa_size], count*size);
	}
	if(g_node_rank == g_numa_root)
	  for(i=0; i<SFANOUT; i++)
        *skts[g_numa_node].childpointer[i] = sense;

    //intra-scoket FANOUT	
    if(g_node_rank != g_numa_root)
    {  
	  while(nodes[g_node_rank].wsense.wsense != sense);
      //memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[nodes[g_node_rank].parentindx], count*size);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[g_node_rank].childpointer[i] = sense;

    //printf("tid = %d, child0 = %d, child1 = %d\n", g_node_rank, nodes[g_node_rank].childindx[0], nodes[g_node_rank].childindx[1]);
    sense = sense == 0? 1: 0;
   }

   if(SFANOUT==0)
   {
    if(g_node_rank != 0)
    {  
	  while(nodes[g_node_rank].wsense.wsense != sense);
      //memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[nodes[g_node_rank].parentindx], count*size);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[g_node_rank].childpointer[i] = sense;

    //printf("tid = %d, child0 = %d, child1 = %d\n", g_node_rank, nodes[g_node_rank].childindx[0], nodes[g_node_rank].childindx[1]);
    sense = sense == 0? 1: 0;

   } 
    //t_barrier(comm->coll->t_barr, g_node_rank);
    DEBUG3(
    	elapse = (MPI_Wtime() - begin);
	printf("thread = %d, step2 time = %12.8f\n", g_node_rank,elapse);
    )

    DEBUG2(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    PROFILE_SHOW_REDUCE(allreduce);)
   //if(g_node_rank!=0)
   //     memcpy(comm->coll->rbuf[g_node_rank], comm->coll->rbuf[0], count*size);
    return MPI_SUCCESS;
}


/*========================================================================
 * Implement reduce as a tree. To have good cache performance, reduce is done chunk by chunk.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_ReduceTree(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,int root, HMPI_Comm comm)
{
  int chunksize = 196608;
    if(count<=chunksize)
	HMPI_ReduceTree_sub(sendbuf, recvbuf, count, datatype, op, root, comm);
	 else 
	  {
	   int remain, chunks, i,size;
	    MPI_Type_size(datatype, &size);
	     chunks = count/chunksize;
         remain = count%chunksize;
	    for(i=0; i<chunks; i++) 
	    {    
	       HMPI_ReduceTree_sub((char *)sendbuf+chunksize*i*size,(char *)recvbuf+chunksize*i*size, chunksize, datatype, op, root, comm);
	      t_barrier(comm->coll->t_barr, g_node_rank);
          }    
	        if(remain>0)
	       HMPI_ReduceTree_sub((char *)sendbuf+chunksize*i*size,(char *)recvbuf+chunksize*i*size, remain, datatype, op, root, comm);
	     }
	   return MPI_SUCCESS;
}

/*========================================================================
 * Implement allreduce as a tree.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AllreduceTree(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    int i;
    int size;
	int tag = 0;
    barrier_record * nodes = comm->coll->t_barr->nodes;
    sbarrier_record * skts = comm->coll->t_barr->skts;
   
    MPI_Type_size(datatype, &size);
    DEBUG2(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)
   
    DEBUG3( double begin; double elapse; )

    //collect the addresses of send buffer and recieve buffer
    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
    
    //corresponding to the number of FANIN, after all the child nodes arriving, parent node begins to do the following computation
    DEBUG3(
    //if(g_node_rank == 0)
    begin = MPI_Wtime();
   )
    for(i=FANIN-1; i>=0; i--)
    {
	  while(nodes[g_node_rank].cnotready[i].cnotready);
      if(nodes[g_node_rank].havechild[i]>0)
       {
		 tag++;
         nodes[g_node_rank].cnotready[i].cnotready = nodes[g_node_rank].havechild[i];
	     if(tag==1)
    	   //distinguish leaf nodes and other nodes to reduce extra memcpy overhead
	       if(nodes[nodes[g_node_rank].havechild[i]].leafbool) 
             NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
           else
	         NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
	     else
	       if(nodes[nodes[g_node_rank].havechild[i]].leafbool) 
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
	       else  
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[i]], op, datatype, count);
       }
    }
    *nodes[g_node_rank].parentpointer = 0;

	if( g_node_rank == g_numa_root)
	{
	  tag = 0;
      for(i=SFANIN-1; i>=0; i--)
      {
	    while(skts[g_numa_node].cnotready[i].cnotready);
        if(skts[g_numa_node].havechild[i]>0)
        {
		 tag++;
         skts[g_numa_node].cnotready[i].cnotready = skts[g_numa_node].havechild[i];

	      if(g_node_size <= sockets)
	      {
	       if(tag==1)
	         if(skts[skts[g_numa_node].havechild[i]].leafbool) 
               NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
             else
	           NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	       else
	         if(skts[skts[g_numa_node].havechild[i]].leafbool) 
	           NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->sbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	         else  
	           NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
	       }
	      else
	          NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[skts[g_numa_node].havechild[i]*g_numa_size], op, datatype, count);
       }
      }
      *skts[g_numa_node].parentpointer = 0;
	}
    DEBUG3(
     if(g_node_rank == 0)
	 {
    	elapse = (MPI_Wtime() - begin);
	    printf("thread = %d, step1 time = %12.8f\n", g_node_rank, elapse);
	 }
    )

    //do mpi_allreduce if there are more than one MPI processes
     if(g_net_size > 1 && g_node_rank==0)
       MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);

    // if(g_node_rank==1 && g_net_size > 1)
    //   MPI_Allreduce(MPI_IN_PLACE, sendbuf, count, datatype, op, comm->comm);

   // DEBUG3(
    //begin = MPI_Wtime();
   //)
   if(SFANOUT>0)
   { //inter-socket FANOUT
	if(g_node_rank == g_numa_root && g_node_rank != 0)
	{
	  while(skts[g_numa_node].wsense.wsense != sense);
      memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[skts[g_numa_node].parentindx*g_numa_size], count*size);
	}
	if(g_node_rank == g_numa_root)
	  for(i=0; i<SFANOUT; i++)
        *skts[g_numa_node].childpointer[i] = sense;

    //intra-scoket FANOUT	
    if(g_node_rank != g_numa_root)
    {  
	  while(nodes[g_node_rank].wsense.wsense != sense);
      memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[nodes[g_node_rank].parentindx], count*size);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[g_node_rank].childpointer[i] = sense;

    //printf("tid = %d, child0 = %d, child1 = %d\n", g_node_rank, nodes[g_node_rank].childindx[0], nodes[g_node_rank].childindx[1]);
    sense = sense == 0? 1: 0;
   }

   if(SFANOUT==0)
   {
    if(g_node_rank != 0)
    {  
	  while(nodes[g_node_rank].wsense.wsense != sense);
      memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[nodes[g_node_rank].parentindx], count*size);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[g_node_rank].childpointer[i] = sense;

    //printf("tid = %d, child0 = %d, child1 = %d\n", g_node_rank, nodes[g_node_rank].childindx[0], nodes[g_node_rank].childindx[1]);
    sense = sense == 0? 1: 0;

   } 
    //t_barrier(comm->coll->t_barr, g_node_rank);
    DEBUG3(
    	elapse = (MPI_Wtime() - begin);
	printf("thread = %d, step2 time = %12.8f\n", g_node_rank,elapse);
    )

    DEBUG2(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    PROFILE_SHOW_REDUCE(allreduce);)
   //if(g_node_rank!=0)
   //     memcpy(comm->coll->rbuf[g_node_rank], comm->coll->rbuf[0], count*size);
    return MPI_SUCCESS;
}

/*========================================================================
 * Implement allreduce as 1D dissemination.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AllreduceDis1D(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    int i,n;
    int size;
    MPI_Type_size(datatype, &size);
    void *tmp = (void *)malloc(size*count);
    
    DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)

	// dimensions of threads intra-node
	int k=(int)ceil(log((double)g_node_size)/log(2.0));

    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
    comm->coll->tmp[g_node_rank]=tmp;
    t_barrier(comm->coll->t_barr, g_node_rank);
    for( i=0; i<k; i++)
	{
      n = g_node_rank - (int)pow(2,i);
      if(n < 0)
    	n += g_node_size;
      if(i>=1)
	  	while(comm->coll->ptop[i-1][n].ptopsense!=hcsense);
	  if(i == 0)
	   NBC_Operation((char *)recvbuf, (char *)sendbuf, (char *)comm->coll->sbuf[n], op, datatype, count);
	  else if(i%2 == 1)
	   NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[n], op, datatype, count);
	  else
	   NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[n], op, datatype, count);
    //t_barrier(comm->coll->t_barr, g_node_rank);
	   if(i<k-1)
       comm->coll->ptop[i][g_node_rank].ptopsense=hcsense;
	}
	if(g_net_size > 1)
	{
	      if(g_node_rank == 0)
	  	  {
		     if(i%2 == 0)
	  	       MPI_Allreduce(tmp, recvbuf, count, datatype, op, comm->comm);
	         else
	  	       MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
	      }
        t_barrier(comm->coll->t_barr, g_node_rank);
		  if(g_node_rank != 0)
		     memcpy((char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[0], count*size);
    }
	else
	{
	   if(i%2 == 0)
           memcpy(recvbuf, tmp, count*size);	
	}
	t_barrier(comm->coll->t_barr, g_node_rank);
      free (tmp);
	  hcsense = hcsense == 0? 1: 0;
    return MPI_SUCCESS;
}

/*========================================================================
 * Implement allreduce as 2D dissemination.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AllreduceDis2D(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    int i,j,n;
    int size;
    MPI_Type_size(datatype, &size);
    void *tmp = (void *)malloc(size*count);
    
    DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)

    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
    comm->coll->tmp[g_node_rank]=tmp;

	// dimensions of sockets
	int k=(int)(log((double)sockets)/log(2.0));
    // dimentions of intra-socket threads
	int t=(int)(log((double)g_numa_size)/log(2.0));

    t_barrier(comm->coll->t_barr, g_node_rank);
    
    for( i=0; i<t; i++)
	{
      n = g_numa_rank - (int)pow(2,i);
      if(n < 0)
    	n += g_numa_size;
	  if(i == 0)
	   NBC_Operation((char *)recvbuf, (char *)sendbuf, (char *)comm->coll->sbuf[g_numa_root+n], op, datatype, count);
	  else if(i%2 == 1)
	   NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[g_numa_root+n], op, datatype, count);
	  else
	   NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[g_numa_root+n], op, datatype, count);
      t_barrier(comm->coll->t_barr, g_node_rank);
	}

	if(g_net_size > 1)
	{
	  for(j=0; j<k; j++)
	  {
       if( g_numa_node%((int)pow(2,j)) == 0 )
       { 
	    n = g_numa_node - (int)pow(2,j);
        if(n < 0)
      	n += sockets;
		if(i%2 == 0)
		  if(j%2 == 0)
    	     NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[n*g_numa_size], op, datatype, count);
		  else
    	     NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[n*g_numa_size], op, datatype, count);
		else
		  if(j%2 == 0)
    	       NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[n*g_numa_size], op, datatype, count);
		  else
    	       NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[n*g_numa_size], op, datatype, count);
	    }
        t_barrier(comm->coll->t_barr, g_node_rank);
	   }
	   if(g_node_rank == 0)
	   {
	      if((i%2 == 0 && j%2 == 0) || (i%2 == 1 && j%2 == 1))
	  	     MPI_Allreduce(tmp, recvbuf, count, datatype, op, comm->comm);
	      else
	  	     MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
	   }
        t_barrier(comm->coll->t_barr, g_node_rank);

		  if(g_node_rank != 0)
		     memcpy((char*)comm->coll->rbuf[g_node_rank], (char*)comm->coll->rbuf[0], count*size);
    }
	else
	{
	  for(j=0; j<k; j++)
	  {
        n = g_numa_node - (int)pow(2,j);
        if(n < 0)
      	n += sockets;
		if(i%2 == 0)
		  if(j%2 == 0)
    	     NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[n*g_numa_size], op, datatype, count);
		  else
    	     NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[n*g_numa_size], op, datatype, count);
		else
		  if(j%2 == 0)
    	       NBC_Operation((char *)tmp, (char *)recvbuf, (char *)comm->coll->rbuf[n*g_numa_size], op, datatype, count);
		  else
    	       NBC_Operation((char *)recvbuf, (char *)tmp, (char *)comm->coll->tmp[n*g_numa_size], op, datatype, count);
        t_barrier(comm->coll->t_barr, g_node_rank);
	   }
	   if((i%2 == 0 && j%2 == 0) || (i%2 == 1 && j%2 == 1))
           memcpy(recvbuf, tmp, count*size);	
	}
      free (tmp);
    return MPI_SUCCESS;
}


/*
int HMPI_AllreduceHyperCube(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)// with data reuse
{
    int j;
    int size;
    MPI_Type_size(datatype, &size);
    void *tmp = (void *)malloc(size*count);
    DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)

    comm->coll->sbuf[g_node_rank]=sendbuf;
    comm->coll->rbuf[g_node_rank]=recvbuf;
    comm->coll->tmp[g_node_rank]=tmp;
    t_barrier(comm->coll->t_barr, g_node_rank);
    
    //STORE_FENCE();
    //comm->coll->ptop_0[g_node_rank].ptopsense=hcsense;
    //STORE_FENCE();
    //while(comm->coll->ptop_0[g_node_rank^1].ptopsense != hcsense) ;

    NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[g_node_rank^1], op, datatype, count);

    //t_barrier(comm->coll->t_barr, g_node_rank);
    //STORE_FENCE();
    comm->coll->ptop_1[g_node_rank].ptopsense=hcsense;
    //STORE_FENCE();
    
    switch(g_node_rank%6){
    case 0:
    while(comm->coll->ptop_1[g_node_rank+2].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank+4].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank+4], op, datatype, count);
    break;
    case 1:
    while(comm->coll->ptop_1[g_node_rank+1].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank+3].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+1], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank+3], op, datatype, count);
    break;
    case 2:
    while(comm->coll->ptop_1[g_node_rank+2].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank-2].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    break;
    case 3:
    while(comm->coll->ptop_1[g_node_rank+1].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank-3].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank+1], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-3], op, datatype, count);
    break;
    case 4:
    while(comm->coll->ptop_1[g_node_rank-2].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank-4].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank-2], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-4], op, datatype, count);
    break;
    case 5:
    while(comm->coll->ptop_1[g_node_rank-3].ptopsense!=hcsense || comm->coll->ptop_1[g_node_rank-5].ptopsense!=hcsense) ;
    NBC_Operation(tmp, recvbuf, (void*)comm->coll->rbuf[g_node_rank-3], op, datatype, count);
    NBC_Operation(tmp, tmp, (void*)comm->coll->rbuf[g_node_rank-5], op, datatype, count);
    break;
    }

    //STORE_FENCE();
    comm->coll->ptop_2[g_node_rank].ptopsense=hcsense;
    //STORE_FENCE();
    //t_barrier(comm->coll->t_barr, g_node_rank);
    if(0<g_node_rank && g_node_rank<6)
    {
      while(comm->coll->ptop_2[6].ptopsense!=hcsense) ;
      NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[6], op, datatype, count);

      //STORE_FENCE();
      comm->coll->ptop_3[g_node_rank].ptopsense=hcsense;
      //STORE_FENCE();
      //while(comm->coll->ptop_3[6].ptopsense!=hcsense || comm->coll->ptop_3[0].ptopsense!=hcsense) ;
      free (tmp);
      hcsense = hcsense == 0? 1: 0;
    }
    
    if(6<g_node_rank && g_node_rank<12)
    {
      while(comm->coll->ptop_2[0].ptopsense!=hcsense) ;
      NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[0], op, datatype, count);

      //STORE_FENCE();
      comm->coll->ptop_3[g_node_rank].ptopsense=hcsense;
      //STORE_FENCE();
      //while(comm->coll->ptop_3[6].ptopsense!=hcsense || comm->coll->ptop_3[0].ptopsense!=hcsense) ;
      free (tmp);
      hcsense = hcsense == 0? 1: 0;
    }
    
    if(g_node_rank == 0) 
    {
      while(comm->coll->ptop_2[6].ptopsense!=hcsense) ;
      NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[6], op, datatype, count);

      //STORE_FENCE();
      comm->coll->ptop_3[g_node_rank].ptopsense=hcsense;

      //STORE_FENCE();
      while(comm->coll->ptop_3[6].ptopsense!=hcsense || comm->coll->ptop_3[7].ptopsense!=hcsense || comm->coll->ptop_3[8].ptopsense!=hcsense || comm->coll->ptop_3[9].ptopsense!=hcsense || comm->coll->ptop_3[10].ptopsense!=hcsense || comm->coll->ptop_3[11].ptopsense!=hcsense) ;
      //STORE_FENCE();
      free (tmp);
      hcsense = hcsense == 0? 1: 0;
      }

    if(g_node_rank == 6) 
    {
      while(comm->coll->ptop_2[0].ptopsense!=hcsense) ;
      NBC_Operation(recvbuf, tmp, (void*)comm->coll->tmp[0], op, datatype, count);

      //STORE_FENCE();
      comm->coll->ptop_3[g_node_rank].ptopsense=hcsense;

      //STORE_FENCE();
      while(comm->coll->ptop_3[0].ptopsense!=hcsense || comm->coll->ptop_3[1].ptopsense!=hcsense || comm->coll->ptop_3[2].ptopsense!=hcsense || comm->coll->ptop_3[3].ptopsense!=hcsense || comm->coll->ptop_3[4].ptopsense!=hcsense || comm->coll->ptop_3[5].ptopsense!=hcsense) ;
      //STORE_FENCE();
      free (tmp);
      hcsense = hcsense == 0? 1: 0;
      }

    DEBUG3(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    PROFILE_SHOW_REDUCE(allreduce);
	)
    return MPI_SUCCESS;
}*/



/*
int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm)
{
    int size;
    int i;

    MPI_Type_size(datatype, &size);

#ifdef DEBUG
    if(g_node_rank == 0) {
    printf("[%i %i] HMPI_Reduce(%p, %p, %i, %p, %p, %d, %p)\n", g_rank, g_node_rank, sendbuf, recvbuf, count, datatype, op, root, comm);
    fflush(stdout);
    }
#endif

    if(g_node_rank == root % g_node_size) {
        //The true root uses its recv buf; others alloc a temp buf.
        void* localbuf;
        if(g_rank == root) {
            localbuf = recvbuf;
	//|| nodes[g_node_rank].cnotready[2].cnotready || nodes[g_node_rank].cnotready[3].cnotready || nodes[g_node_rank].cnotready[4].cnotready || nodes[g_node_rank].cnotready[5].cnotready); 
    
    //distinguish leaf nodes and other nodes to reduce extra memcpy overhead
    for(j=0; j<FANIN; j++)
      if(nodes[g_node_rank].havechild[j]>0)
       {
         nodes[g_node_rank].cnotready[j].cnotready = nodes[g_node_rank].havechild[j];
	     if(j==0)
	       if(nodes[nodes[g_node_rank].havechild[j]].leafbool) 
             NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[j]], op, datatype, count);
           else
	         NBC_Operation(recvbuf, sendbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[j]], op, datatype, count);
	     else
	       if(nodes[nodes[g_node_rank].havechild[j]].leafbool) 
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->sbuf[nodes[g_node_rank].havechild[j]], op, datatype, count);
	       else  
	         NBC_Operation(recvbuf, recvbuf, (void*)comm->coll->rbuf[nodes[g_node_rank].havechild[j]], op, datatype, count);
       }

    *nodes[g_node_rank].parentpointer = 0;
    //DEBUG3(
     //if(g_node_rank == 0)
     //{
    	//elapse = (MPI_Wtime() - begin);
	//printf("thread = %d, step1 time = %12.8f\n", g_node_rank, elapse);
     //}
    //)
    //do mpi_allreduce if there are more than one MPI processes
     if(g_node_rank==0 && g_net_size > 1)
       MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);

    // if(g_node_rank==1 && g_net_size > 1)
    //   MPI_Allreduce(MPI_IN_PLACE, sendbuf, count, datatype, op, comm->comm);

   // DEBUG3(
    //begin = MPI_Wtime();
   //)
    //all other threads waiting for root thread to get the final result
    if(g_node_rank != 0)
    { 
      while(nodes[g_node_rank].wsense.wsense!=sense);
      memcpy((void*)comm->coll->rbuf[g_node_rank], (void*)comm->coll->rbuf[nodes[g_node_rank].parentindx], count*size);
    }
    //printf("tid = %d, child0 = %d, child1 = %d\n", g_node_rank, nodes[g_node_rank].childindx[0], nodes[g_node_rank].childindx[1]);


    //final result is disseminated as a tree structure, lower contention compared to root thread broadcast
    
    for(j=0; j<FANOUT; j++)
    if(nodes[g_node_rank].childindx[j]>0)
    {
   //   memcpy((void*)comm->coll->rbuf[nodes[g_node_rank].childindx[j]], (void*)comm->coll->rbuf[g_node_rank], count*size);
      *nodes[g_node_rank].childpointer[j] = sense;
    }
    
    sense = sense == 0? 1: 0;
    
    //t_barrier(comm->coll->t_barr, g_node_rank);
    DEBUG3(
    	elapse = (MPI_Wtime() - begin);
	printf("thread = %d, step2 time = %12.8f\n", g_node_rank,elapse);
    )

    DEBUG2(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    PROFILE_SHOW_REDUCE(allreduce);)
   //if(g_node_rank!=0)
   //     memcpy(comm->coll->rbuf[g_node_rank], comm->coll->rbuf[0], count*size);

    return MPI_SUCCESS;
}*/




int HMPI_Reduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, HMPI_Comm comm)
{
    int size;
    int i;

    MPI_Type_size(datatype, &size);

#ifdef DEBUG
    if(g_node_rank == 0) {
    printf("[%i %i] HMPI_Reduce(%p, %p, %i, %p, %p, %d, %p)\n", g_rank, g_node_rank, sendbuf, recvbuf, count, datatype, op, root, comm);
    fflush(stdout);
    }
#endif

    if(g_node_rank == root % g_node_size) {
        //The true root uses its recv buf; others alloc a temp buf.
        void* localbuf;
        if(g_rank == root) {
            localbuf = recvbuf;
        } else {
           // localbuf = memalign(4096, size * count);
        }

        //TODO eliminate this memcpy by folding into a reduce call?
        memcpy(localbuf, sendbuf, size * count);

        //barrier_cb(&comm->barr, 0, barrier_iprobe);
        t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);

        for(i=0; i<g_node_size; ++i) {
            if(i == g_node_rank) continue;
            NBC_Operation(localbuf,
                    localbuf, (void*)comm->coll->sbuf[i], op, datatype, count);
        }

        //Other local ranks are free to go.
        //barrier(&comm->barr, 0);
        t_barrier(comm->coll->t_barr, g_node_rank);

        if(g_net_size > 1) {
            MPI_Reduce(MPI_IN_PLACE,
                    localbuf, count, datatype, op, root / g_node_size, comm->comm);
        }

        if(g_rank != root) {
        //    free(localbuf);
        }
    } else {
        //First barrier signals to root that all buffers are ready.
        comm->coll->sbuf[g_node_rank] = sendbuf;
        //barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
        t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);

        //Wait for root to copy our data; were free when it's done.
        //barrier(&comm->barr, g_node_rank);
        t_barrier(comm->coll->t_barr, g_node_rank);
    }

        t_barrier(comm->coll->t_barr, g_node_rank);
    return MPI_SUCCESS;
}


int HMPI_Allreduce(void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, HMPI_Comm comm)
{
    //MPI_Aint extent, lb;
    int size;
    int i;
    MPI_Type_size(datatype, &size);
    //MPI_Type_get_extent(datatype, &lb, &extent);
    //MPI_Type_extent(datatype, &extent);

#if 0
    if(extent != size) {
        printf("allreduce non-contiguous derived datatypes are not supported yet!\n");
        fflush(stdout);
        MPI_Abort(comm->comm, 0);
    }
#endif
#ifdef DEBUG
    if(g_node_rank == 0) {
    printf("[%i %i] HMPI_Allreduce(%p, %p, %i, %p, %p, %p)\n", g_rank, g_node_rank, sendbuf, recvbuf,  count, datatype, op, comm);
    fflush(stdout);
    }
#endif

    DEBUG3(
    PROFILE_INIT(g_node_rank);
    PROFILE_START(allreduce);)
    // Do the MPI allreduce, then each rank can copy out.
    if(g_node_rank == 0) {
        comm->coll->rbuf[0] = recvbuf;
        //barrier_cb(&comm->barr, 0, barrier_iprobe);
	t_barrier_cb(comm->coll->t_barr, 0, barrier_iprobe);
   // DEBUG1(printf("t%d: step1\n", g_node_rank);)
        //TODO eliminate this memcpy by folding into a reduce call?
        memcpy(recvbuf, sendbuf, size * count);

        for(i=1; i<g_node_size; ++i) {
            NBC_Operation(recvbuf, recvbuf,
                    (void*)comm->coll->sbuf[i], op, datatype, count);
        }

        if(g_net_size > 1) {
            MPI_Allreduce(MPI_IN_PLACE, recvbuf, count, datatype, op, comm->comm);
        }

      DEBUG1(printf("t%d: step1\n", g_node_rank);)
        t_barrier(comm->coll->t_barr, 0);
   //      t_barrier_cb(comm->coll->t_barr, 0, barrier_iprobe);
     DEBUG1(printf("t%d: step2\n", g_node_rank);)
    } else {
        //Put up our send buffer for the root thread to reduce from.
        comm->coll->sbuf[g_node_rank] = sendbuf;
          t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);
     
		DEBUG1(printf("t%d: step1_others\n", g_node_rank);)
        //Wait for the root rank to do its thing.
	  t_barrier(comm->coll->t_barr, g_node_rank);
	  //t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);
         DEBUG1(printf("t%d: step2_others\n", g_node_rank); )
        //Copy reduced data to our own buffer.
        memcpy(recvbuf, (void*)comm->coll->rbuf[0], count*size);
    }

    //Potential optimization -- 0 can't leave until all threads arrive.. all
    //others can go
    t_barrier(comm->coll->t_barr, g_node_rank);

    DEBUG3(
    PROFILE_STOP(allreduce);
    PROFILE_SHOW(allreduce);
    //PROFILE_SHOW_REDUCE(allreduce);
    )
    //t_barrier_cb(comm->coll->t_barr, g_node_rank, barrier_iprobe);
    return MPI_SUCCESS;
}



#define HMPI_ALLTOALL_TAG 7546347
/*========================================================================
 * Implement alltoall as pair-wise message exchange, which has the best performance for large message size.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AlltoallPairWise(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
  //void* rbuf;
  int32_t send_size;
  uint64_t size;
  //MPI_Request* send_reqs = NULL;
  //MPI_Request* recv_reqs = NULL;
  MPI_Request send_reqs;
  MPI_Request recv_reqs;
  MPI_Status send_stat;
  MPI_Status recv_stat;
  MPI_Datatype dt_send;
  MPI_Datatype dt_recv;

  MPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
  int32_t recv_size;
  MPI_Aint send_extent, recv_extent, lb;

  MPI_Type_size(recvtype, &recv_size);

  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->comm, 0);
  }

  if(send_size * sendcount != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->comm, 0);
  }
#endif

#ifdef DEBUG
  printf("[%i] HMPI_Alltoall(%p, %i, %p, %p, %i, %p, %p)\n", g_net_rank*g_node_size+g_node_rank, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
  fflush(stdout);
#endif

  //uint64_t comm_size = g_node_size * g_net_size;
  uint64_t comm_size = g_node_size;
  //uint64_t comm_size = g_node_size;
  uint64_t data_size = send_size * sendcount;

  comm->coll->sbuf[g_node_rank] = sendbuf;

  if(g_node_rank == 0) {
      //Alloc temp send/recv buffers
      comm->coll->mpi_sbuf = memalign(4096, data_size * g_node_size * comm_size);
      comm->coll->mpi_rbuf = memalign(4096, data_size * g_node_size * comm_size);

     // send_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);
     // recv_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);

      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have g_node_size per process
      // though, so we multiply by that.
      MPI_Type_contiguous(sendcount * g_node_size * g_node_size, sendtype, &dt_send);
      MPI_Type_commit(&dt_send);

      MPI_Type_contiguous(recvcount * g_node_size * g_node_size, recvtype, &dt_recv);
      MPI_Type_commit(&dt_recv);

      //Post receives from each other rank, except self.
      //int len = data_size * g_node_size * g_node_size;
      //for(int i = 0; i < g_net_size; i++) {
      //    if(i != g_net_rank) {
      //        MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf + (len * i)), 1,
      //                dt_recv, i, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs[i]);
      //    }
      //}
    //  for(int i = 0; i < g_net_size; i++) {
    //      if(i != g_net_rank) {
    //          MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf), 1,
    //                  dt_recv, i, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs);
    //      }
    //  }
      //recv_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

      for(int i = 0; i < g_net_size; i++) {
          
		  int source = (g_net_rank - i - 1 + g_net_size) % g_net_size;
          if(i != (g_net_size-1) && g_node_rank == 0) {
              MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf), 1,
                      dt_recv, source, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs);
          }
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  //Copy into the shared send buffer on a stride by g_node_size
  //This way our temp buffer has all the data going to proc 0, then proc 1, etc
  uintptr_t offset = g_node_rank * data_size;
  uintptr_t scale = data_size * g_node_size;
  
  //Verified from (now missing) prints, this is correct
  //TODO - try staggering
  // Data is pushed here -- remote thread can't read it
  //for(uintptr_t i = 0; i < comm_size; i++) {
  for(uintptr_t j = 0; j < g_node_size; j++) {
      int node_rank = MPI_UNDEFINED;
      HMPI_Comm_node_rank(comm, i, &node_rank);
      //if(!HMPI_Comm_local(comm, i)) {
      if(node_rank != MPI_UNDEFINED) {
          //Copy to send buffer to go out over network
          memcpy((void*)((uintptr_t)(comm->coll->mpi_sbuf) + (scale * j) + offset),
                  (void*)((uintptr_t)sendbuf + data_size * (i*g_node_size+j)), data_size);
      }
  }

  //Start sends to each other rank
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  int dest = (g_net_rank + i + 1) % g_net_size;
  if(g_node_rank == 0) {
  //    int len = data_size * g_node_size * g_node_size;
  //    for(int i = 1; i < g_net_size; i++) {

          if( i != (g_net_size-1)) {
              MPI_Isend((void*)((uintptr_t)comm->coll->mpi_sbuf), 1,
                      dt_send, dest, HMPI_ALLTOALL_TAG, comm->comm, &send_reqs);
          }
   //   }

      //send_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

  //Pull local data from other threads' send buffers.
  //For each thread, memcpy from their send buffer into my receive buffer.
  int r = g_net_rank * g_node_size; //Base rank
  if( i == 0 )
  for(uintptr_t thr = 0; thr < g_node_size; thr++) {
      //Note careful use of addition by r to get the right offsets
      int t = (g_node_rank + thr) % g_node_size;
      memcpy((void*)((uintptr_t)recvbuf + ((r + t) * data_size)),
             (void*)((uintptr_t)comm->coll->sbuf[t] + ((r + g_node_rank) * data_size)),
             data_size);
  }

  //Wait on sends and receives to complete
  if(g_node_rank == 0 && i != (g_net_size-1)) {
      //MPI_Waitall(g_net_size, recv_reqs, MPI_STATUSES_IGNORE);
      //MPI_Waitall(g_net_size, send_reqs, MPI_STATUSES_IGNORE);
      MPI_Wait(&recv_reqs, &recv_stat);
      MPI_Wait(&send_reqs, &send_stat);
  }

 t_barrier(comm->coll->t_barr, g_node_rank);
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);

  //Need to do g_net_size memcpy's -- one block of data per MPI process.
  // We copy g_node_size * data_size at a time.
  offset = g_node_rank * data_size * g_node_size;
  //scale = data_size * g_node_size * g_node_size;
  size = g_node_size * data_size;

//  for(uint64_t i = 0; i < g_net_size; i++) {
      if(i != g_net_rank) {
          memcpy((void*)((uintptr_t)recvbuf + size * source),
                  (void*)((uintptr_t)comm->coll->mpi_rbuf + offset),
                  size);
      }
//  }
}
//  barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
t_barrier(comm->coll->t_barr, g_node_rank);

  if(g_node_rank == 0) {
      free((void*)comm->coll->mpi_sbuf);
      free((void*)comm->coll->mpi_rbuf);
      //free(send_reqs);
      //free(recv_reqs);
      MPI_Type_free(&dt_send);
      MPI_Type_free(&dt_recv);
  }

  return MPI_SUCCESS;
}

/*========================================================================
 * Implement alltoall as bulk synchronous parallelism, namely launch non-blocking recieves and sends following a waitall. It gets the best performance for mediate message size.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_AlltoallBulk(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
  //void* rbuf;
  int32_t send_size;
  uint64_t size;
  MPI_Request* send_reqs = NULL;
  MPI_Request* recv_reqs = NULL;
  MPI_Datatype dt_send;
  MPI_Datatype dt_recv;

  MPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
  int32_t recv_size;
  MPI_Aint send_extent, recv_extent, lb;

  MPI_Type_size(recvtype, &recv_size);

  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->comm, 0);
  }

  if(send_size * sendcount != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->comm, 0);
  }
#endif

#ifdef DEBUG
  printf("[%i] HMPI_Alltoall(%p, %i, %p, %p, %i, %p, %p)\n", g_net_rank*g_node_size+g_node_rank, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
  fflush(stdout);
#endif

  uint64_t comm_size = g_node_size * g_net_size;
  uint64_t data_size = send_size * sendcount;

  comm->coll->sbuf[g_node_rank] = sendbuf;

  if(g_node_rank == 0) {
      //Alloc temp send/recv buffers
      comm->coll->mpi_sbuf = memalign(4096, data_size * g_node_size * comm_size);
      comm->coll->mpi_rbuf = memalign(4096, data_size * g_node_size * comm_size);

      send_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);
      recv_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);

      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have g_node_size per process
      // though, so we multiply by that.
      MPI_Type_contiguous(sendcount * g_node_size * g_node_size, sendtype, &dt_send);
      MPI_Type_commit(&dt_send);

      MPI_Type_contiguous(recvcount * g_node_size * g_node_size, recvtype, &dt_recv);
      MPI_Type_commit(&dt_recv);

      //Post receives from each other rank, except self.
      int len = data_size * g_node_size * g_node_size;
      for(int i = 0; i < g_net_size; i++) {
          if(i != g_net_rank) {
              MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf + (len * i)), 1,
                      dt_recv, i, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs[i]);
          }
      }
      recv_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  //Copy into the shared send buffer on a stride by g_node_size
  //This way our temp buffer has all the data going to proc 0, then proc 1, etc
  uintptr_t offset = g_node_rank * data_size;
  uintptr_t scale = data_size * g_node_size;

  //Verified from (now missing) prints, this is correct
  //TODO - try staggering
  // Data is pushed here -- remote thread can't read it
  for(uintptr_t i = 0; i < comm_size; i++) {
      int node_rank = MPI_UNDEFINED;
      HMPI_Comm_node_rank(comm, i, &node_rank);
      //if(!HMPI_Comm_local(comm, i)) {
      if(node_rank != MPI_UNDEFINED) {
          //Copy to send buffer to go out over network
          memcpy((void*)((uintptr_t)(comm->coll->mpi_sbuf) + (scale * i) + offset),
                  (void*)((uintptr_t)sendbuf + data_size * i), data_size);
      }
  }

  //Start sends to each other rank
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  if(g_node_rank == 0) {
      int len = data_size * g_node_size * g_node_size;
      for(int i = 1; i < g_net_size; i++) {
          int r = (g_net_rank + i) % g_net_size;
          if(r != g_net_rank) {
              MPI_Isend((void*)((uintptr_t)comm->coll->mpi_sbuf + (len * r)), 1,
                      dt_send, r, HMPI_ALLTOALL_TAG, comm->comm, &send_reqs[r]);
          }
      }

      send_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

  //Pull local data from other threads' send buffers.
  //For each thread, memcpy from their send buffer into my receive buffer.
  int r = g_net_rank * g_node_size; //Base rank
  for(uintptr_t thr = 0; thr < g_node_size; thr++) {
      //Note careful use of addition by r to get the right offsets
      int t = (g_node_rank + thr) % g_node_size;
      memcpy((void*)((uintptr_t)recvbuf + ((r + t) * data_size)),
             (void*)((uintptr_t)comm->coll->sbuf[t] + ((r + g_node_rank) * data_size)),
             data_size);
  }

  //Wait on sends and receives to complete
  if(g_node_rank == 0) {
      MPI_Waitall(g_net_size, recv_reqs, MPI_STATUSES_IGNORE);
      MPI_Waitall(g_net_size, send_reqs, MPI_STATUSES_IGNORE);
      MPI_Type_free(&dt_send);
      MPI_Type_free(&dt_recv);
  }

 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  //Need to do g_net_size memcpy's -- one block of data per MPI process.
  // We copy g_node_size * data_size at a time.
  offset = g_node_rank * data_size * g_node_size;
  scale = data_size * g_node_size * g_node_size;
  size = g_node_size * data_size;

  for(uint64_t i = 0; i < g_net_size; i++) {
      if(i != g_net_rank) {
          memcpy((void*)((uintptr_t)recvbuf + size * i),
                  (void*)((uintptr_t)comm->coll->mpi_rbuf + (scale * i) + offset),
                  size);
      }
  }

//  barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
t_barrier(comm->coll->t_barr, g_node_rank);

  if(g_node_rank == 0) {
      free((void*)comm->coll->mpi_sbuf);
      free((void*)comm->coll->mpi_rbuf);
      free(send_reqs);
      free(recv_reqs);
  }

  return MPI_SUCCESS;
}
/*========================================================================
 * Implement alltoall as Bruck algorithm, which has the best performance for small message size.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
int HMPI_Alltoall(void* sendbuf, int sendcount, MPI_Datatype sendtype, void* recvbuf, int recvcount, MPI_Datatype recvtype, HMPI_Comm comm) 
{
  //void* rbuf;
  int32_t send_size;
  uint64_t size;
  int steps=(int)ceil(log((double)g_net_size)/log(2.0));
  MPI_Request send_reqs;
  MPI_Request recv_reqs;
  MPI_Status send_stat;
  MPI_Status recv_stat;
 // MPI_Datatype dt_send;
 // MPI_Datatype dt_recv;

  MPI_Type_size(sendtype, &send_size);

#ifdef HMPI_SAFE
  int32_t recv_size;
  MPI_Aint send_extent, recv_extent, lb;

  MPI_Type_size(recvtype, &recv_size);

  //MPI_Type_extent(sendtype, &send_extent);
  //MPI_Type_extent(recvtype, &recv_extent);
  MPI_Type_get_extent(sendtype, &lb, &send_extent);
  MPI_Type_get_extent(recvtype, &lb, &recv_extent);

  if(send_extent != send_size || recv_extent != recv_size) {
    printf("alltoall non-contiguous derived datatypes are not supported yet!\n");
    MPI_Abort(comm->comm, 0);
  }

  if(send_size * sendcount != recv_size * recvcount) {
    printf("different send and receive size is not supported!\n");
    MPI_Abort(comm->comm, 0);
  }
#endif

#ifdef DEBUG
  printf("[%i] HMPI_Alltoall(%p, %i, %p, %p, %i, %p, %p)\n", g_net_rank*g_node_size+g_node_rank, sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
  fflush(stdout);
#endif
  uint64_t comm_size = g_node_size * g_net_size;
  uint64_t data_size = send_size * sendcount;
  uint64_t parcel_size;
  comm->coll->sbuf[g_node_rank] = sendbuf;

  if(g_node_rank == 0) {
      //Alloc temp send/recv buffers
      comm->coll->mpi_tmp = memalign(4096, data_size * g_node_size * comm_size);
      comm->coll->mpi_sbuf = memalign(4096, data_size * g_node_size * g_node_size * g_net_size / 2);
      comm->coll->mpi_rbuf = memalign(4096, data_size * g_node_size * g_node_size * g_net_size / 2);

      //send_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);
      //recv_reqs = (MPI_Request*)malloc(sizeof(MPI_Request) * g_net_size);

      //Seems like we should multiply by comm_size, but alltoall already
      // assumes one element per process.  We do have g_node_size per process
      // though, so we multiply by that.
      //MPI_Type_contiguous(sendcount * g_node_size * g_node_size, sendtype, &dt_send);
      //MPI_Type_commit(&dt_send);

      //MPI_Type_contiguous(recvcount * g_node_size * g_node_size, recvtype, &dt_recv);
      //MPI_Type_commit(&dt_recv);

      //Post receives from each other rank, except self.
      //int len = data_size * g_node_size * g_node_size;
      //for(int i = 0; i < g_net_size; i++) {
      //    if(i != g_net_rank) {
      //        MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf + (len * i)), 1,
      //                dt_recv, i, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs[i]);
      //    }
      //}
      //recv_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

 t_barrier(comm->coll->t_barr, g_node_rank);
 uintptr_t offset = g_node_rank * data_size;
 uintptr_t scale = data_size * g_node_size;

  for(uintptr_t i = 0; i < comm_size; i++) {
      int node_rank = MPI_UNDEFINED;
      HMPI_Comm_node_rank(comm, i, &node_rank);
      //if(!HMPI_Comm_local(comm, i)) {
      if(node_rank != MPI_UNDEFINED) {
          //Copy to send buffer to go out over network
          memcpy((void*)((uintptr_t)(comm->coll->mpi_tmp) + (scale * (((g_net_size-g_net_rank)*g_node_size+i)%comm_size)) + offset),
                  (void*)((uintptr_t)sendbuf + data_size * i), data_size);
      }
  }

 for(int s=0; s < steps; s++)
 {
  int t = (int)ceil(g_net_size/(int)pow(2,s));
  if(t%2 == 1)
    parcel_size = t/2*2*(int)pow(2,s)*data_size * g_node_size * g_node_size;
  else
  {
    if(g_net_size/(int)pow(2,s)==0)
	  parcel_size = g_net_size/2*data_size * g_node_size * g_node_size;
	else
	  parcel_size = (g_net_size-(int)pow(2,s)*t/2)*data_size * g_node_size * g_node_size;
  }

  if(g_node_rank == 0) {
      //int len = data_size * g_node_size * g_node_size;
      //for(int i = 0; i < g_net_size; i++) {
          //if(i != g_net_rank) {
              MPI_Irecv((void*)((uintptr_t)comm->coll->mpi_rbuf), parcel_size/send_size,
			  sendtype, (g_net_rank-(int)pow(2,s)+g_net_size)%g_net_size, HMPI_ALLTOALL_TAG, comm->comm, &recv_reqs);
          //}
      //}
      //recv_reqs[g_net_rank] = MPI_REQUEST_NULL;
	  }
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
// t_barrier(comm->coll->t_barr, g_node_rank);

  //Copy into the shared send buffer on a stride by g_node_size
  //This way our temp buffer has all the data going to proc 0, then proc 1, etc
  //uintptr_t offset = g_node_rank * data_size;
  //uintptr_t scale = data_size * g_node_size;

  //Verified from (now missing) prints, this is correct
  //TODO - try staggering
  // Data is pushed here -- remote thread can't read it
  //for(uintptr_t i = 0; i < comm_size; i++) {
  //    if(!HMPI_Comm_local(comm, i)) {
  //        //Copy to send buffer to go out over network
  //        memcpy((void*)((uintptr_t)(comm->coll->mpi_tmp) + (scale * (((g_net_size-g_net_rank)*g_node_size+i)%comm_size)) + offset),
  //                (void*)((uintptr_t)sendbuf + data_size * i), data_size);
  //    }
  //}

  //Start sends to each other rank
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 
 //Package parcel to send buffer
 if(g_node_rank == 0) {
 uintptr_t offst = 0;
 uintptr_t offst1 = 0;
 uintptr_t sz = 0;
 for(uintptr_t i = 0; i < (int)ceil(g_net_size/(int)pow(2,s)); i++)
 {
   if(i%2 == 1)
   {
     offst1 += sz;
       offst = i*(int)pow(2,s)*data_size * g_node_size * g_node_size;
     if((g_net_size-i*(int)pow(2,s))<(int)pow(2,s))
       sz = (g_net_size-i*(int)pow(2,s))*data_size * g_node_size * g_node_size;
     else sz =(int)pow(2,s)*data_size * g_node_size * g_node_size;
     
	 memcpy((void*)((uintptr_t)(comm->coll->mpi_sbuf) + offst1),
                  (void*)((uintptr_t)(comm->coll->mpi_tmp) + offst), sz);
   }
 
 }
 }
  //for(uintptr_t i = 0; i < comm_size; i++) {
  //    if(!HMPI_Comm_local(comm, i)) {
  //        //Copy to send buffer to go out over network
  //        memcpy((void*)((uintptr_t)(comm->coll->mpi_tmp) + (scale * (((g_net_size-g_net_rank)*g_node_size+i)%comm_size)) + offset),
  //                (void*)((uintptr_t)sendbuf + data_size * i), data_size);
  //    }
  //}
 t_barrier(comm->coll->t_barr, g_node_rank);

  if(g_node_rank == 0) {
     // int len = data_size * g_node_size * g_node_size;
     // for(int i = 1; i < g_net_size; i++) {
     //     int r = (g_net_rank + i) % g_net_size;
          //if(r != g_net_rank) {
              MPI_Isend((void*)((uintptr_t)comm->coll->mpi_sbuf), parcel_size/send_size,
                      sendtype, (g_net_rank+(int)pow(2,s))%g_net_size, HMPI_ALLTOALL_TAG, comm->comm, &send_reqs);
          //}
      //}

      //send_reqs[g_net_rank] = MPI_REQUEST_NULL;
  }

  if(s == 0)
  {
    //Pull local data from other threads' send buffers.
    //For each thread, memcpy from their send buffer into my receive buffer.
    int r = g_net_rank * g_node_size; //Base rank
    for(uintptr_t thr = 0; thr < g_node_size; thr++) {
        //Note careful use of addition by r to get the right offsets
        int t = (g_node_rank + thr) % g_node_size;
        memcpy((void*)((uintptr_t)recvbuf + ((r + t) * data_size)),
               (void*)((uintptr_t)comm->coll->sbuf[t] + ((r + g_node_rank) * data_size)),
               data_size);
    }
   }

  //Wait on sends and receives to complete
  if(g_node_rank == 0) {
     // MPI_Waitall(g_net_size, recv_reqs, MPI_STATUSES_IGNORE);
     // MPI_Waitall(g_net_size, send_reqs, MPI_STATUSES_IGNORE);
      MPI_Wait(&recv_reqs, &recv_stat);
      MPI_Wait(&send_reqs, &send_stat);
   //   MPI_Type_free(&dt_send);
   //   MPI_Type_free(&dt_recv);
  }

  //Copy to mpi_tmp buffer
  
 if(g_node_rank == 0) {
 uintptr_t offst = 0;
 uintptr_t offst1 = 0;
 uintptr_t sz = 0;
 for(uintptr_t i = 0; i < (int)ceil(g_net_size/(int)pow(2,s)); i++)
 {
   if(i%2 == 1)
   {
     offst1 += sz;
       offst = i*(int)pow(2,s)*data_size * g_node_size * g_node_size;
     if((g_net_size-i*(int)pow(2,s))<(int)pow(2,s))
       sz = (g_net_size-i*(int)pow(2,s))*data_size * g_node_size * g_node_size;
     else sz =(int)pow(2,s)*data_size * g_node_size * g_node_size;
     
	 memcpy((void*)((uintptr_t)(comm->coll->mpi_tmp) + offst),
                  (void*)((uintptr_t)(comm->coll->mpi_rbuf) + offst1), sz);
   }
 
 }
 }


}
 // barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
 t_barrier(comm->coll->t_barr, g_node_rank);

  //Need to do g_net_size memcpy's -- one block of data per MPI process.
  // We copy g_node_size * data_size at a time.
  offset = g_node_rank * data_size * g_node_size;
  scale = data_size * g_node_size * g_node_size;
  size = g_node_size * data_size;

  for(uint64_t i = 0; i < g_net_size; i++) {
      if(i != g_net_rank) {
          memcpy((void*)((uintptr_t)recvbuf + size * i),
                  (void*)((uintptr_t)comm->coll->mpi_tmp + (scale * ((g_net_rank-i+g_net_size)%g_net_size)) + offset),
                  size);
      }
  }

//  barrier_cb(&comm->barr, g_node_rank, barrier_iprobe);
t_barrier(comm->coll->t_barr, g_node_rank);

  if(g_node_rank == 0) {
      free((void*)comm->coll->mpi_sbuf);
      free((void*)comm->coll->mpi_rbuf);
      free((void*)comm->coll->mpi_tmp);
      //free(send_reqs);
      //free(recv_reqs);
  }

  return MPI_SUCCESS;
}

