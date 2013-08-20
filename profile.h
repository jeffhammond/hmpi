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
#ifndef _AWF_PROFILE_HEADER
#define _AWF_PROFILE_HEADER

//Note: assumes that either mpi.h or hmpi.h is included before this header.

#ifndef _PROFILE
#define _PROFILE 0
#endif

#ifndef _PROFILE_PAPI_EVENTS
#define _PROFILE_PAPI_EVENTS 0
#endif

#ifndef _PROFILE_PAPI_FILE
#define _PROFILE_PAPI_FILE 0
#endif

//#define _PROFILE_MAX_MIN 1

#if _PROFILE == 1

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "error.h"


#ifdef __cplusplus
extern "C" {
#endif

extern uint64_t _profile_overhead;

#if _PROFILE_PAPI_EVENTS == 1
#include <papi.h>

//Statically reserve space to handle 16 counters at once.
#define MAX_EVENTS 16

extern int _profile_eventset;
extern int _profile_num_events;
extern int _profile_event_codes[MAX_EVENTS];

#if _PROFILE_PAPI_FILE == 1

extern FILE* _profile_fd;

#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \
  FILE* _profile_fd; \
  int _profile_eventset = PAPI_NULL; \
  int _profile_num_events = 0; \
  int _profile_event_codes[MAX_EVENTS];

#else //_PROFILE_PAPI_FILE != 1
#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \
  int _profile_eventset = PAPI_NULL; \
  int _profile_num_events = 0; \
  int _profile_event_codes[MAX_EVENTS];
#endif //_PROFILE_PAPI_FILE != 1

#else //_PROFILE_PAPI_EVENTS != 1

#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \

#endif //_PROFILE_PAPI_EVENTS != 1


#ifdef __cplusplus
}
#endif


typedef struct profile_timer_t {
    uint64_t count;
    uint64_t time;  //Measured in nanoseconds
    uint64_t start; //Measured in nanoseconds
#if _PROFILE_MAX_MIN
    uint64_t min;
    uint64_t max;
#endif
#if _PROFILE_PAPI_EVENTS == 1
    uint64_t tmp_ctrs[MAX_EVENTS];  //Used during timing regions
    uint64_t ctrs[MAX_EVENTS];
    uint64_t ctr_min[MAX_EVENTS];
    uint64_t ctr_max[MAX_EVENTS];
#endif
} profile_timer_t;


typedef struct profile_timer_results_t
{
    uint64_t count;
    double total; //Measured in microseconds
    double avg;   //Measured in microseconds

#if _PROFILE_MAX_MIN
    double max;
    double min;
#endif

#if _PROFILE_PAPI_EVENTS == 1
    uint64_t total_ctrs[MAX_EVENTS];
    double avg_ctrs[MAX_EVENTS];
#if _PROFILE_MAX_MIN
    uint64_t max_ctrs[MAX_EVENTS];
    uint64_t min_ctrs[MAX_EVENTS];
#endif
#endif
} profile_timer_results_t;


typedef struct profile_counter_t {
    uint64_t count;
    uint64_t total;
#if _PROFILE_MAX_MIN
    uint64_t min;
    uint64_t max;
#endif
} profile_counter_t;


typedef struct profile_counter_results_t
{
    uint64_t count;
    uint64_t total;
    double avg;

#if _PROFILE_MAX_MIN
    uint64_t min;
    uint64_t max;
#endif
} profile_counter_results_t;


#define PROFILE_TIMER(v) \
    profile_timer_t _profile_timer_ ## v = {0}

#define PROFILE_TIMER_EXTERN(v) \
    extern profile_timer_t _profile_timer_ ## v

#define PROFILE_TIMER_RESET(v) \
    memset(&_profile_timer_ ## v, 0, sizeof(profile_timer_t))


#define PROFILE_COUNTER(v) \
    profile_counter_t _profile_counter_ ## v = {0}

#define PROFILE_COUNTER_EXTERN(v) \
    extern profile_counter_t _profile_counter_ ## v

#define PROFILE_COUNTER_RESET(v) \
    memset(&_profile_counter_ ## v, 0, sizeof(profile_counter_t))


static inline void __PROFILE_START(struct profile_timer_t* v);
static inline void __PROFILE_STOP(const char* name, struct profile_timer_t* v);

#ifndef UINT64_MAX
#define UINT64_MAX (uint64_t)-1
#endif

static void PROFILE_CALIBRATE(void)
{
    struct profile_timer_t v = {0};
    uint64_t min = UINT64_MAX;
    uint64_t old_time = 0;
    uint64_t time;
    int i;

    for(i = 0; i < 100000; i++) {
        old_time = v.time;

        __PROFILE_START(&v);
        __PROFILE_STOP("calibrate", &v);

        time = v.time - old_time;
        if(time < min) {
            min = time;
        }
    }

    _profile_overhead = min;
}


static __attribute__((unused)) void PROFILE_INIT()
{
    //Prevent multiple initializations.
    if(_profile_overhead != 0) {
        return;
    }

#if _PROFILE_PAPI_EVENTS == 1
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if(ret < 0) {
        ERROR("PAPI init failure %s", PAPI_strerror(ret));
    }

    PAPI_thread_init((long unsigned int (*)())pthread_self);


    _profile_eventset = PAPI_NULL;
    ret = PAPI_create_eventset(&_profile_eventset);
    if(ret != PAPI_OK) {
        ERROR("PAPI_create_eventset %s", PAPI_strerror(ret));
    }

    //Check for an environment variable and parse it.
    //Expect PROFILE_PAPI_EVENTS to be a space or comma separated list.
    char* env_papi_events = getenv("PROFILE_PAPI_EVENTS");
    char* sep = env_papi_events;

    for(_profile_num_events = 0;
            sep != NULL && _profile_num_events < MAX_EVENTS; 
            _profile_num_events++) {
        char* event_str = strsep(&sep, " \n\t,");

        ret = PAPI_event_name_to_code(event_str,
                &_profile_event_codes[_profile_num_events]);
        if(ret != PAPI_OK) {
            WARNING("PAPI_event_name_to_code(%s) %s",
                    event_str, PAPI_strerror(ret));
        }

        ret = PAPI_add_event(_profile_eventset,
                _profile_event_codes[_profile_num_events]);
        if(ret != PAPI_OK) {
            ERROR("PAPI_add_event %s %s", event_str, PAPI_strerror(ret));
        }
    }

    if(_profile_num_events == 0) {
        ERROR("Must specify events via PROFILE_PAPI_EVENTS environment variable");
    }

    if(_profile_num_events == MAX_EVENTS && sep != NULL) {
        ERROR("Maximum of %d events are supported", MAX_EVENTS);
    }

    int num_hwcntrs = PAPI_num_counters();
    if(num_hwcntrs < _profile_num_events) {
        ERROR("PAPI reported %d events available, %d events specified",
                num_hwcntrs, _profile_num_events);
    }


#if _PROFILE_PAPI_FILE == 1
    char filename[PAPI_MAX_STR_LEN] = {0};

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    snprintf(filename, PAPI_MAX_STR_LEN, "profile-%d-%d.out", tid, rank);

    _profile_fd = fopen(filename, "w+");
    if(_profile_fd == NULL) {
        ERROR("Unable to open profile data file for writing");
    }

    fprintf(_profile_fd, "VAR TIME");
    for(i = 0; i < _profile_num_events; i++) {
        PAPI_event_info_t info;

        ret = PAPI_get_event_info(_profile_events[i], &info);
        if(ret != PAPI_OK) {
            ERROR("PAPI_get_event_info %d %s", i, PAPI_strerror(ret));
        }

        fprintf(_profile_fd, " %s", info.symbol);
    }
    fprintf(_profile_fd, "\n");
#endif //_PROFILE_PAPI_FILE == 1

    //Start the events now.  We leave them counting all the time, and use
    //PAPI_Read() to get the values.
    ret = PAPI_start(_profile_eventset);
    if(ret != PAPI_OK) {
        ERROR("PAPI_start %s", PAPI_strerror(ret));
    }

#endif //_PROFILE_PAPI_EVENTS == 1

    PROFILE_CALIBRATE();
}


static inline void PROFILE_FINALIZE()
{
#if _PROFILE_PAPI_EVENTS == 1
#if _PROFILE_PAPI_FILE == 1
    fclose(_profile_fd);
#endif
#endif
}


#ifdef __bg__
#ifdef __IBMCPP__
#include <builtins.h>
#endif
#include <hwi/include/bqc/A2_core.h>
//#define BGQ_NS_PER_CYCLE ((double)1e9/(double)1.6e9)
#define BGQ_NS_PER_CYCLE ((double)0.625)

// 64-bit read of BGQ Cycle counter register.
static inline uint64_t get_bgq_cycles()
{
    uint64_t dest;
    asm volatile ("mfspr %0,%1" : "=&r" (dest) : "i" (SPRN_TBRO));
    //return dest * BGQ_NS_PER_CYCLE;
    return dest;
}

#endif

#define PROFILE_START(v) __PROFILE_START(&(_profile_timer_ ## v))

static void __PROFILE_START(struct profile_timer_t* v)
{
#if _PROFILE_PAPI_EVENTS == 1
    int rc = PAPI_read(_profile_eventset, (long long*)v->tmp_ctrs);
    if(rc != PAPI_OK) {
        ERROR("PAPI_read (start) (did you call PROFILE_INIT?) %s", PAPI_strerror(rc));
    }
#endif

#ifdef __bg__
    //Time is stored in cycles
    v->start = get_bgq_cycles();
    __fence();
#else
    //Time is stored in nanoseconds
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    v->start = ((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec);
#endif
    //printf("start %lu %lu %lu\n", v->start, ts.tv_sec, ts.tv_nsec);
}



#define PROFILE_STOP(v) __PROFILE_STOP(#v, &_profile_timer_ ## v)

static void __PROFILE_STOP(const char* name, struct profile_timer_t* v)
{
    //Grab the time right away
    //Do as little as possible until time and PAPI counters are grabbed.
#ifdef __bg__
    //Time is stored in cycles
    uint64_t cycles = get_bgq_cycles();
#else
    //Time is stored in nanoseconds
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif

#if _PROFILE_PAPI_EVENTS == 1
    //Grab counter values
    uint64_t ctrs[MAX_EVENTS];

    int rc = PAPI_read(_profile_eventset, (long long*)ctrs);
    if(rc != PAPI_OK) {
        ERROR("PAPI_read (stop) %s", PAPI_strerror(rc));
    }
#endif

    //Calculate time taken
#ifdef __bg__
    __fence();
    uint64_t t = ((double)(cycles - v->start) * BGQ_NS_PER_CYCLE);
#else
    uint64_t t = ((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec) - v->start;
#endif

    if(t < _profile_overhead) {
        t = 0;
    } else {
        t -= _profile_overhead;
    }

    //Accumulate the time
    v->time += t;
    v->count++;

#if _PROFILE_MAX_MIN
    if(t < v->min || v->min == 0) {
        v->min = t;
    }

    if(t > v->max) {
        v->max = t;
    }
#endif

#if _PROFILE_PAPI_EVENTS == 1
    int i;

    //Accumulate the counter values
    for(i = 0; i < _profile_num_events; i++) {
        //The counters are continuous, so subtract off the count at which
        // we started to get the count that occurred during the timing region.
        ctrs[i] -= v->tmp_ctrs[i];

        v->ctrs[i] += ctrs[i];
        if(v->ctr_max[i] < ctrs[i]) {
            v->ctr_max[i] = ctrs[i];
        }

        if(v->ctr_min[i] > ctrs[i] || v->ctr_min[i] == 0) {
            v->ctr_min[i] = ctrs[i];
        }
    }

#if _PROFILE_PAPI_FILE == 1
    fprintf(_profile_fd, "%s %lu", name, t);
    for(i = 0; i < _profile_num_events; i++) {
        fprintf(_profile_fd, " %lu", ctrs[i]);
    }

    fprintf(_profile_fd, "\n");
#endif //_PROFILE_PAPI_FILE == 1
#endif //_PROFILE_PAPI_EVENTS == 1
}


#define PROFILE_TIMER_RESULTS(v, result) __PROFILE_TIMER_RESULTS(#v, &_profile_timer_ ## v, result)

static __attribute__((unused)) void __PROFILE_TIMER_RESULTS(const char* name, struct profile_timer_t* v, profile_timer_results_t* r)
{
    uint64_t r_total;
#if _PROFILE_MAX_MIN
    uint64_t r_max;
    uint64_t r_min;
#endif

    MPI_Allreduce(&v->count, &r->count, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&v->time, &r_total, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

#if _PROFILE_MAX_MIN
    MPI_Allreduce(&v->max, &r_max, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&v->min, &r_min, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
#endif //_PROFILE_MAX_MIN

#if _PROFILE_PAPI_EVENTS == 1
    int i;

    MPI_Allreduce(v->ctrs, r->total_ctrs, _profile_num_events,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

#if _PROFILE_MAX_MIN
    MPI_Allreduce(v->ctr_max, r->max_ctrs, _profile_num_events,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(v->ctr_min, r->min_ctrs, _profile_num_events,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
#endif //_PROFILE_MAX_MIN

    for(i = 0; i < _profile_num_events; i++) {
        r->avg_ctrs[i] = (double)r->total_ctrs[i] / r->count;
    }

#endif //_PROFILE_PAPI_EVENTS == 1

    //Times stored as microseconds
    r->total = (double)r_total / 1000.0;
    r->avg = ((double)r_total / r->count) / 1000.0;

#if _PROFILE_MAX_MIN
    r->max = r_max / 1000.0;
    r->min = r_min / 1000.0;
#endif
}


#define PROFILE_TIMER_SHOW(v) __PROFILE_TIMER_SHOW(#v, &_profile_timer_ ## v)

static __attribute__((unused)) void __PROFILE_TIMER_SHOW(const char* name, struct profile_timer_t* v)
{
    profile_timer_results_t r;
    int rank;

    __PROFILE_TIMER_RESULTS(name, v, &r);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0 && r.count > 0) {
#if _PROFILE_MAX_MIN
        printf("TIME %15s cnt %-7lu time %13.6f ms total %13.6f ms avg %13.6f max %13.6f min\n",
                name, r.count, r.total / 1000.0, r.avg / 1000.0,
                r.max / 1000.0, r.min / 1000.0);
#else
        printf("TIME %15s cnt %-7lu time %13.6f ms total %13.6f ms avg\n",
                name, r.count, r.total / 1000.0, r.avg / 1000.0);
#endif

#if _PROFILE_PAPI_EVENTS == 1
        int i;

        for(i = 0; i < _profile_num_events; i++) {
            PAPI_event_info_t info;

            if(PAPI_get_event_info(_profile_event_codes[i], &info) != PAPI_OK) {
                printf("ERROR PAPI_get_event_info %d\n",
                        _profile_event_codes[i]);
                abort();
            }
#if _PROFILE_MAX_MIN
            printf("PAPI %20s %10lu total %14.3f avg %10lu max %10lu min\n",
                    info.symbol, r.total_ctrs[i], r.avg_ctrs[i],
                    r.max_ctrs[i], r.min_ctrs[i]);
#else //!_PROFILE_MAX_MIN
            printf("PAPI %20s %10lu total %14.3f avg\n",
                    info.symbol, r.total_ctrs[i], r.avg_ctrs[i]);
#endif
        }
#endif
        fflush(stdout);
    }
}


#define PROFILE_ACCUMULATE(v, a)  \
    __PROFILE_ACCUMULATE(&(_profile_counter_ ## v), (a))

static __attribute__((unused)) void __PROFILE_ACCUMULATE(struct profile_counter_t* v, uint64_t amount)
{
    v->total += amount;
    v->count += 1;

#if _PROFILE_MAX_MIN
    if(amount < v->min || v->min == 0) {
        v->min = amount;
    }

    if(amount > v->max) {
        v->max = amount;
#endif
}


#define PROFILE_COUNTER_RESULTS(v, result) \
    __PROFILE_COUNTER_RESULTS(#v, &_profile_timer_ ## v, result)

static __attribute__((unused)) void __PROFILE_COUNTER_RESULTS(const char* name,
        struct profile_counter_t* v, profile_counter_results_t* r)
{
    MPI_Allreduce(&v->count, &r->count, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&v->total, &r->total, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

#if _PROFILE_MAX_MIN
    MPI_Allreduce(&v->max, &r->max, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&v->min, &r->min, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
#endif //_PROFILE_MAX_MIN

    r->avg = (double)r->total / (double)r->count;
}


#define PROFILE_COUNTER_SHOW(v) \
    __PROFILE_COUNTER_SHOW(#v, &_profile_counter_ ## v)

static __attribute__((unused)) void __PROFILE_COUNTER_SHOW(const char* name,
        struct profile_counter_t* v)
{
    profile_counter_results_t r;
    int rank;

    __PROFILE_COUNTER_RESULTS(name, v, &r);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
        if(r.count == r.total) {
            //This really was just a simple counter: no need for avg/min/max.
            printf("COUNT %15s cnt %-7lu\n", name, r.count);
        } else {
#if _PROFILE_MAX_MIN
            printf("COUNT %15s cnt %-7lu amt %-8lu total %13.6f avg %-8lu max %-8lu min\n",
                    name, r.count, r.total, r.avg,
                    r.max, r.min);
#else
            printf("COUNT %15s cnt %-7lu amt %-8lu total %13.6f avg\n",
                    name, r.count, r.total, r.avg);
#endif
        }

        fflush(stdout);
    }
}




#warning "PROFILING ON"

#else
#define PROFILE_DECLARE()
static inline void PROFILE_INIT(void) {}
static inline void PROFILE_FINALIZE(void) {}
#define PROFILE_TIMER(var)
#define PROFILE_TIMER_EXTERN(var)
#define PROFILE_START(var)
#define PROFILE_STOP(var)
#define PROFILE_TIMER_RESULTS(var)
#define PROFILE_TIMER_SHOW(var)

#define PROFILE_COUNTER(var)
#define PROFILE_COUNTER_EXTERN(var)
#define PROFILE_ACCUMULATE(var, c)
#define PROFILE_COUNTER_RESULTS(var)
#define PROFILE_COUNTER_SHOW(var)
//#warning "PROFILING OFF"
#endif

#endif

