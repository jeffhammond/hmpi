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
#ifndef BARRIER_H
#define BARRIER_H
//#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>

#include "lock.h"

//#define CACHE_LINE 64
//#define CACHE_LINE 1

extern int g_rank;                      //HMPI world rank
extern int g_size;                      //HMPI world size
extern int g_node_rank;                 //HMPI node rank
extern int g_node_size;                 //HMPI node size
extern int g_net_rank;                  //HMPI net rank
extern int g_net_size;                  //HMPI net size
extern int g_numa_node;                 //HMPI numa node (compute-node scope)
extern int g_numa_root;                 //HMPI root rank on same numa node
extern int g_numa_rank;                 //HMPI rank within numa node

//intra-socket
#define FANOUT 11
#define FANIN 3
#define CACHE_LINE_SIZE 64

//inter-socket
#define SFANOUT 0  //if SFANOUT=0, than there is no hierarchical fanout, namely all other threads read data from root thread
#define SFANIN 2

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

#ifdef DEBUG_LEVEL3
#define DEBUG3(stats) stats
#else
#define DEBUG3(stats)
#endif

#ifdef DEBUG_LEVEL4
#define DEBUG4(stats) stats
#else
#define DEBUG4(stats)
#endif

#ifdef FANIN_1
#define FANINEQUAL1(init) init
#else
#define FANINEQUAL1(init)
#endif 

#ifndef FANIN_1
#define PFANIN(init) init
#else
#define PFANIN(init)
#endif
typedef struct {
volatile int32_t cnotready;
int32_t padding[15];
} padcnotready;
typedef struct {
volatile int32_t wsense;
int32_t padding[15];
} padwense;
//typedef struct {
// padwense wsense __attribute__((aligned(64)));
// volatile int32_t* parentpointer;
// volatile int32_t* childpointer[FANOUT];
// volatile int32_t havechild[FANIN];
// padcnotready cnotready[FANIN] __attribute__((aligned(64)));
// volatile int32_t dummy;
//} barrier_lite;



typedef struct {
 padwense wsense __attribute__((aligned(64)));
 volatile int32_t* parentpointer;
 volatile int32_t* childpointer[FANOUT];
 volatile int32_t havechild[FANIN];
 padcnotready cnotready[FANIN] __attribute__((aligned(64)));
 volatile int32_t dummy;
 volatile int32_t parentindx;
 //volatile int32_t childindx[FANOUT];
 int leafbool;
} barrier_record;

typedef struct {
 padwense wsense __attribute__((aligned(64)));
 volatile int32_t* parentpointer;
 volatile int32_t* childpointer[SFANOUT];
 volatile int32_t havechild[SFANIN];
 padcnotready cnotready[SFANIN] __attribute__((aligned(64)));
 volatile int32_t dummy;
 volatile int32_t parentindx;
 //volatile int32_t childindx[SFANOUT];
 int leafbool;
} sbarrier_record;

typedef struct{
 barrier_record * nodes;
 sbarrier_record * skts;
} hbarrier_record;

//TODO
extern int32_t sense;
extern int sockets;
//extern __thread int numpst;

//barrier_record nodes[g_node_rank];


/*========================================================================
 * Initialize a hierarchical binomial tree for the barrier.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
static int t_barrier_init_fanin1(hbarrier_record **hbarrier, int nthreads)
{
    int tid,i,j,k,sid;
    int threads = nthreads/sockets;
	int id  = -1;
   
    int t=(int)ceil( (log((double)threads)/log(2.0)) );
    hbarrier_record *hbarr;
    hbarr = *hbarrier = (hbarrier_record *)valloc(sizeof(hbarrier_record));
    barrier_record *barr;
    sbarrier_record *sbarr;
    barr = hbarr->nodes = (barrier_record *)valloc(sizeof(barrier_record)*nthreads);
    sbarr = hbarr->skts = (sbarrier_record *)valloc(sizeof(sbarrier_record)*sockets);

   //initialization for threads in each socket
    for (i=0; i<sockets; i++)
    for (id=0; id<threads; id++)
	{
	  tid = i*threads + id;

	 if(SFANOUT>0)
	 {
	   for(j=0; j<FANOUT; j++)
	   {
	    if(FANOUT*id+j+1 < threads)
        {
	      barr[tid].childpointer[j] = &(barr[FANOUT*id+j+1+i*threads].wsense.wsense);
	      //barr[tid].childindx[j] = FANOUT*id+j+1+i*threads;
	    }
        else
        {  
	      barr[tid].childpointer[j] = &(barr[tid].dummy);
	      //barr[tid].childindx[j] = 0;
	    }
	   }
	  
	    if(id == 0)
	  	barr[tid].parentindx = -1;
 	    else
	  	barr[tid].parentindx = (id-1)/FANOUT+i*threads;
        barr[tid].wsense.wsense = 0;
	 }
    }

   //initialization for root thread in each socket, using hypercube

    for (sid=0; sid<sockets; sid++)
	{
//	  if(SFANIN > 1)
//	  {
        for(j=0; j<SFANIN; j++)
        { 
         // if(sid==0&&j==0)
          //  sbarr[sid].havechild[j] = 0;
          //else 
  	      if(SFANIN*sid+1>=sockets)
  	        sbarr[sid].leafbool=1;
  	      else
  	        sbarr[sid].leafbool=0;
            sbarr[sid].havechild[j] = (SFANIN*sid+j+1) < sockets? SFANIN*sid+j+1: 0;
        }
  
        sbarr[sid].dummy = 1024;
        if(sid == 0)
          sbarr[sid].parentpointer = &(sbarr[sid].dummy);
        else
          sbarr[sid].parentpointer = &(sbarr[(int)(floor((sid-1)/SFANIN))].cnotready[(sid-1)%SFANIN].cnotready);
  
  
        for(j=0; j<SFANIN; j++)
        {    
          sbarr[sid].cnotready[j].cnotready = sbarr[sid].havechild[j];
        //  printf("sid = %d, cnotready = %d\n", sid,  sbarr[sid].cnotready[j].cnotready);
        }
      // }
        if(SFANOUT>0)
		{
		  int sfanout = SFANOUT;
          //printf("sid = %d, parent = %d\n", sid,  *(sbarr[sid].parentpointer));
  	      for(j=0; j<SFANOUT; j++)
  	      {
  	       if(SFANOUT*sid+j+1 < sockets)
           {
  	          sbarr[sid].childpointer[j] = &(sbarr[SFANOUT*sid+j+1].wsense.wsense);
  	          //sbarr[sid].childindx[j] = SFANOUT*sid+j+1;
  	       }
           else
           {  
  	          sbarr[sid].childpointer[j] = &(sbarr[sid].dummy);
  	          //sbarr[sid].childindx[j] = 0;
  	       }
  	      }
  	
      	  if(sid == 0)
      	  	sbarr[sid].parentindx = -1;
      	  else
      	  	sbarr[sid].parentindx = (sid-1)/sfanout;
          
    	  sbarr[sid].wsense.wsense = 0;
		}
    }

//	if(SFANIN == 1)
//	{
//	
//	
//	}
	// abandon hierarchical fanout
	if(SFANOUT == 0)
	{
	 for(tid=0; tid<nthreads; tid++)
	 {
	 for(j=0; j<FANOUT; j++)
	 {
	    if(FANOUT*tid+j+1 < nthreads)
        {
	      barr[tid].childpointer[j] = &(barr[FANOUT*tid+j+1].wsense.wsense);
	      //barr[tid].childindx[j] = FANOUT*tid+j+1;
	    }
        else
        {  
	      barr[tid].childpointer[j] = &(barr[tid].dummy);
	      //barr[tid].childindx[j] = 0;
	    }
	   }
	  
	  if(tid == 0)
	  	barr[tid].parentindx = -1;
	  else
	  	barr[tid].parentindx = (tid-1)/FANOUT;
        barr[tid].wsense.wsense = 0;
	 }
	}


	if(FANIN == t)
	{
     for (i=0; i<sockets; i++)
	    for (id=0; id<threads; id++)
		  {
		   tid = i*threads+id;
	        for(k=0; k<t; k++)
		    {
               if(k == 0)
			   {
			     if(id%(int)pow(2, k+1)==1)
				    barr[tid].leafbool=1;
				 else
				    barr[tid].leafbool=0;
			   }

               if(id%(int)pow(2, k+1) == 0)
			   {
			     barr[tid].havechild[FANIN-k-1]=(id+(int)pow(2, k))<threads? id+(int)pow(2, k)+i*threads: 0;

				 if(id+(int)pow(2, k) < threads)
				 barr[id+(int)pow(2,k)+i*threads].parentpointer = &(barr[tid].cnotready[FANIN-k-1].cnotready);
				 }
			   else
			     barr[tid].havechild[FANIN-k-1] = 0;
			}

           barr[tid].dummy = 1024;
		   if(id == 0) 
		     barr[tid].parentpointer = &(barr[tid].dummy); 
		   for(j=0; j<FANIN; j++)
		     barr[tid].cnotready[j].cnotready = barr[tid].havechild[j];  
	
	      }
	}
	else
    {
	  printf("error! if you open FANINEQUAL1, the macro FANIN should be equal to %d in  barrier.h \n",t);
	  exit(-1);
	}
    return 0;
}

/*========================================================================
 * Initialize a hierarchical n-ary tree for the barrier.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
static int t_barrier_init(hbarrier_record **hbarrier, int nthreads)
{
    int tid,i,j,sid;
    int threads = nthreads/sockets;
	int id  = -1;
   
    hbarrier_record *hbarr;
    hbarr = *hbarrier = (hbarrier_record *)valloc(sizeof(hbarrier_record));
    barrier_record *barr;
    sbarrier_record *sbarr;
    barr = hbarr->nodes = (barrier_record *)valloc(sizeof(barrier_record)*nthreads);
    sbarr = hbarr->skts = (sbarrier_record *)valloc(sizeof(sbarrier_record)*sockets);

   //initialization for threads in each socket
    for (i=0; i<sockets; i++)
    for (id=0; id<threads; id++)
	{
	  tid = i*threads + id;
      for(j=0; j<FANIN; j++)
      { 
       // if(tid==0&&j==0)
        //  barr[tid].havechild[j] = 0;
        //else 
	    if(FANIN*id+1>=threads)
	      barr[tid].leafbool=1;
	    else
	      barr[tid].leafbool=0;

        barr[tid].havechild[j] = (FANIN*id+j+1) < threads? FANIN*id+j+1+i*threads: 0;
      }

      barr[tid].dummy = 1024;
      if(id == 0)
        barr[tid].parentpointer = &(barr[tid].dummy);
      else
        barr[tid].parentpointer = &(barr[(int)(floor((id-1)/FANIN)) + i*threads].cnotready[(id-1)%FANIN].cnotready);

      
      for(j=0; j<FANIN; j++)
      {    
        barr[tid].cnotready[j].cnotready = barr[tid].havechild[j];
      //  printf("tid = %d, cnotready = %d\n", tid,  barr[tid].cnotready[j].cnotready);
      }
      //printf("tid = %d, parent = %d\n", tid,  *(barr[tid].parentpointer));

	 if(SFANOUT>0)
	 {
	   for(j=0; j<FANOUT; j++)
	   {
	    if(FANOUT*id+j+1 < threads)
        {
	      barr[tid].childpointer[j] = &(barr[FANOUT*id+j+1+i*threads].wsense.wsense);
	      //barr[tid].childindx[j] = FANOUT*id+j+1+i*threads;
	    }
        else
        {  
	      barr[tid].childpointer[j] = &(barr[tid].dummy);
	      //barr[tid].childindx[j] = 0;
	    }
	   }
	  
	    if(id == 0)
	  	barr[tid].parentindx = -1;
 	    else
	  	barr[tid].parentindx = (id-1)/FANOUT+i*threads;
        barr[tid].wsense.wsense = 0;
	 }
    }

   //initialization for root thread in each socket, using hypercube

    for (sid=0; sid<sockets; sid++)
	{
//	  if(SFANIN > 1)
//	  {
        for(j=0; j<SFANIN; j++)
        { 
         // if(sid==0&&j==0)
          //  sbarr[sid].havechild[j] = 0;
          //else 
  	      if(SFANIN*sid+1>=sockets)
  	        sbarr[sid].leafbool=1;
  	      else
  	        sbarr[sid].leafbool=0;
            sbarr[sid].havechild[j] = (SFANIN*sid+j+1) < sockets? SFANIN*sid+j+1: 0;
        }
  
        sbarr[sid].dummy = 1024;
        if(sid == 0)
          sbarr[sid].parentpointer = &(sbarr[sid].dummy);
        else
          sbarr[sid].parentpointer = &(sbarr[(int)(floor((sid-1)/SFANIN))].cnotready[(sid-1)%SFANIN].cnotready);
  
  
        for(j=0; j<SFANIN; j++)
        {    
          sbarr[sid].cnotready[j].cnotready = sbarr[sid].havechild[j];
        //  printf("sid = %d, cnotready = %d\n", sid,  sbarr[sid].cnotready[j].cnotready);
        }
      // }
        if(SFANOUT>0)
		{
		  int sfanout = SFANOUT;
          //printf("sid = %d, parent = %d\n", sid,  *(sbarr[sid].parentpointer));
  	      for(j=0; j<SFANOUT; j++)
  	      {
  	       if(SFANOUT*sid+j+1 < sockets)
           {
  	          sbarr[sid].childpointer[j] = &(sbarr[SFANOUT*sid+j+1].wsense.wsense);
  	          //sbarr[sid].childindx[j] = SFANOUT*sid+j+1;
  	       }
           else
           {  
  	          sbarr[sid].childpointer[j] = &(sbarr[sid].dummy);
  	          //sbarr[sid].childindx[j] = 0;
  	       }
  	      }
  	
      	  if(sid == 0)
      	  	sbarr[sid].parentindx = -1;
      	  else
      	  	sbarr[sid].parentindx = (sid-1)/sfanout;
          
    	  sbarr[sid].wsense.wsense = 0;
		}
    }

//	if(SFANIN == 1)
//	{
//	
//	
//	}
	// abandon hierarchical fanout
	if(SFANOUT == 0)
	{
	 for(tid=0; tid<nthreads; tid++)
	 {
	 for(j=0; j<FANOUT; j++)
	 {
	    if(FANOUT*tid+j+1 < nthreads)
        {
	      barr[tid].childpointer[j] = &(barr[FANOUT*tid+j+1].wsense.wsense);
	      //barr[tid].childindx[j] = FANOUT*tid+j+1;
	    }
        else
        {  
	      barr[tid].childpointer[j] = &(barr[tid].dummy);
	      //barr[tid].childindx[j] = 0;
	    }
	   }
	  
	  if(tid == 0)
	  	barr[tid].parentindx = -1;
	  else
	  	barr[tid].parentindx = (tid-1)/FANOUT;
        barr[tid].wsense.wsense = 0;
	 }
	}
    return 0;
}

/*========================================================================
 * Implementation of a tree-based barrier.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
static inline void t_barrier(hbarrier_record *package, int tid)
{
	int i;
    barrier_record * nodes = package->nodes;
    sbarrier_record * skts = package->skts;
    
	//intra-socket FANIN
	for(i=0; i<FANIN; i++)
      while(nodes[tid].cnotready[i].cnotready);
    for(i=0; i<FANIN; i++)
       nodes[tid].cnotready[i].cnotready = nodes[tid].havechild[i];
    *nodes[tid].parentpointer = 0;
    
	//inter-socket FANIN
	if(tid == g_numa_root)
	{
      for(i=0; i<SFANIN; i++)
        while(skts[g_numa_node].cnotready[i].cnotready);
	
	  for(i=0; i<SFANIN; i++)
	    skts[g_numa_node].cnotready[i].cnotready = skts[g_numa_node].havechild[i];
      *skts[g_numa_node].parentpointer = 0;
	}

   if(SFANOUT>0)
   {
    //inter-socket FANOUT
	if(tid == g_numa_root && tid != 0)
	{
	  while(skts[g_numa_node].wsense.wsense != sense);
	}
	if(tid == g_numa_root)
	  for(i=0; i<SFANOUT; i++)
        *skts[g_numa_node].childpointer[i] = sense;

    //intra-scoket FANOUT	
    if(tid != g_numa_root)
    {  
	  while(nodes[tid].wsense.wsense != sense);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[tid].childpointer[i] = sense;
    //STORE_FENCE();
    sense = sense == 0? 1: 0;
   }

	// abandon hierarchical fanout
   if(SFANOUT ==0)
   {
    if(tid != 0)
    {  
	  while(nodes[tid].wsense.wsense != sense);
    }
	for(i=0; i<FANOUT; i++)
      *nodes[tid].childpointer[i] = sense;
    //STORE_FENCE();
    sense = sense == 0? 1: 0;
   }
    return;
}

/*========================================================================
 * Implementation of a tree-based barrier with callback function in case
 * of deadlock.
 * Shigang Li
 * April 03, 2013
 =========================================================================*/
static inline void t_barrier_cb(hbarrier_record *package, int tid, void (*callbackfn)(void))
{
    int i;
//	int sktid = tid/numpst;
    barrier_record * nodes = package->nodes;
    sbarrier_record * skts = package->skts;
    for(i=0; i<FANIN; i++)
      while(nodes[tid].cnotready[i].cnotready);
    for(i=0; i<FANIN; i++)
       nodes[tid].cnotready[i].cnotready = nodes[tid].havechild[i];
    *nodes[tid].parentpointer = 0;

    if(tid == g_numa_root)
	{
      for(i=0; i<SFANIN; i++)
        while(skts[g_numa_node].cnotready[i].cnotready);
	  for(i=0; i<SFANIN; i++)
	    skts[g_numa_node].cnotready[i].cnotready = skts[g_numa_node].havechild[i];
      *skts[g_numa_node].parentpointer = 0;
	}
	if(SFANOUT>0)
    {
     if(tid == g_numa_root && tid != 0)
     	{
  	     while(skts[g_numa_node].wsense.wsense != sense);
  	       callbackfn();
      	}
  
  	  if(tid == g_numa_root)
  	    for(i=0; i<SFANOUT; i++)
          *skts[g_numa_node].childpointer[i] = sense;
  	
      if(tid != g_numa_root)
       {  
  	    while(nodes[tid].wsense.wsense != sense);
  	       callbackfn();
       }
  	  for(i=0; i<FANOUT; i++)
        *nodes[tid].childpointer[i] = sense;
      //STORE_FENCE();
      sense = sense == 0? 1: 0;
	}

	if(SFANOUT == 0)
	{
      if(tid != 0)
      {  
        while(nodes[tid].wsense.wsense != sense);
  	       callbackfn();
      }
      for(i=0; i<FANOUT; i++)
        *nodes[tid].childpointer[i] = sense;
      //STORE_FENCE();
      sense = sense == 0? 1: 0;
	}
    return;
}


typedef struct {
  //Centralized barrier
  int32_t* local_sense;
  volatile int32_t global_sense;
  volatile int32_t count;
  int32_t threads;
} barrier_t;


static int barrier_init(barrier_t *barrier, int threads) {
  barrier->local_sense = (int32_t*)calloc(sizeof(int32_t) /** CACHE_LINE*/, threads);
  barrier->global_sense = 0;
  barrier->count = threads;
  barrier->threads = threads;
  return 0;
}


static int barrier_destroy(barrier_t *barrier) {
  free(barrier->local_sense);
  return 0;
}


//Standard barrier
static inline void barrier(barrier_t *barrier, int tid) {
  int32_t local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  //int64_t val = __sync_fetch_and_sub(&barrier->count, (int64_t)1);
  int32_t val = FETCH_ADD32((int*)&barrier->count, (int32_t)-1);

  if(val == 1) {
      barrier->count = barrier->threads;
      STORE_FENCE();
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense);
  return;
}


//Barrier with a callback -- usually used to poll MPI to prevent deadlock
static inline void barrier_cb(barrier_t *barrier, int tid, void (*cbfn)(void)) {
  int32_t local_sense = barrier->local_sense[tid] = ~barrier->local_sense[tid];

  //if(__sync_fetch_and_sub(&barrier->count, (int)1) == 1) {
  //int32_t val = __sync_fetch_and_sub(&barrier->count, (int32_t)1);
  int32_t val = FETCH_ADD32((int*)&barrier->count, (int32_t)-1);

  if(val == 1) {
      barrier->count = barrier->threads;
      barrier->global_sense = local_sense;
      return;
  }

  //while(barrier->global_sense != barrier->local_sense[tid]) {
  while(barrier->global_sense != local_sense) {
      cbfn();
  }
  return;
}


#endif
