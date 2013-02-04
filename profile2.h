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

#define _PROFILE_MAX_MIN 1

#if _PROFILE == 1

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>


#ifdef __cplusplus
extern "C" {
#endif

#if _PROFILE_PAPI_EVENTS == 1
#include <papi.h>

#if 0
#define NUM_EVENTS 4

static int _profile_events[NUM_EVENTS] =
          //{ PAPI_L1_DCM, PAPI_L2_DCM, PAPI_STL_ICY, PAPI_TOT_INS};
          //{ PAPI_L1_ICM, PAPI_TLB_DM, PAPI_TLB_IM, PAPI_TOT_INS};
          //{ PAPI_L2_ICM, PAPI_L3_TCM, PAPI_BR_MSP, PAPI_BR_PRC};
          { PAPI_L2_DCM, PAPI_TLB_IM, PAPI_L2_ICM, PAPI_TOT_INS};

//int _profile_eventset = PAPI_NULL;
#endif
#include "papi_ctrs.h"

static char _profile_event_names[NUM_EVENTS][128] = {{0}};

extern int _profile_eventset;

#if _PROFILE_PAPI_FILE == 1

extern FILE* _profile_fd;

#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \
  FILE* _profile_fd; \
  int _profile_eventset = PAPI_NULL;

#else
#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \
  int _profile_eventset = PAPI_NULL;
#endif

#else //_PROFILE_PAPI_EVENTS != 1

#define PROFILE_DECLARE() \
  uint64_t _profile_overhead = 0; \

#endif //_PROFILE_PAPI_EVENTS != 1

extern uint64_t _profile_overhead;

#ifdef __cplusplus
}
#endif


typedef struct profile_vars_t {
    uint64_t count;
    uint64_t time;  //Measured in nanoseconds
    uint64_t start; //Measured in nanoseconds
#if _PROFILE_MAX_MIN
    uint64_t min;
    uint64_t max;
#endif
#if _PROFILE_PAPI_EVENTS == 1
    uint64_t tmp_ctrs[NUM_EVENTS];  //Used during timing regions
    uint64_t ctrs[NUM_EVENTS];
    uint64_t ctr_min[NUM_EVENTS];
    uint64_t ctr_max[NUM_EVENTS];
#endif
} profile_vars_t;


typedef struct profile_results_t
{
    uint64_t count;
    double total; //Measured in microseconds
    double avg;   //Measured in microseconds

#if _PROFILE_MAX_MIN
    double max;
    double min;
#endif

#if _PROFILE_PAPI_EVENTS == 1
    uint64_t total_ctrs[NUM_EVENTS];
    double avg_ctrs[NUM_EVENTS];
#if _PROFILE_MAX_MIN
    uint64_t max_ctrs[NUM_EVENTS];
    uint64_t min_ctrs[NUM_EVENTS];
#endif
#endif
} profile_results_t;


#define PROFILE_VAR(v) \
    profile_vars_t _profile_ ## v = {0}

#define PROFILE_EXTERN(v) \
    extern profile_vars_t _profile_ ## v

#define PROFILE_RESET(v) \
    memset(&_profile_ ## v, 0, sizeof(profile_vars_t))


PROFILE_EXTERN(MPI_Other);

static inline void __PROFILE_START(struct profile_vars_t* v);
static inline void __PROFILE_STOP(const char* name, struct profile_vars_t* v);

static void PROFILE_CALIBRATE()
{
    int i;
    //int rank;
    //uint64_t min;
    struct profile_vars_t v = {0};

    for(i = 0; i < 100000; i++) {
        __PROFILE_START(&v);
        __PROFILE_STOP("calibrate", &v);
    }

    //MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    //MPI_Allreduce(&v.min, &min, 1,
    //        MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);

    _profile_overhead = v.min;
}

static void PROFILE_INIT() __attribute__((unused));

static void PROFILE_INIT()
{
#if _PROFILE_PAPI_EVENTS == 1
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if(ret < 0) {
        printf("PAPI init failure %s\n", PAPI_strerror(ret));
        fflush(stdout);
        exit(-1);
    }

    PAPI_thread_init((long unsigned int (*)())pthread_self);

    int num_hwcntrs = 0;

    num_hwcntrs = PAPI_num_counters();
    if(num_hwcntrs < NUM_EVENTS) {
        printf("ERROR PAPI reported < %d events available\n", NUM_EVENTS);
        exit(-1);
    }

    _profile_eventset = PAPI_NULL;
    ret = PAPI_create_eventset(&_profile_eventset);
    if(ret != PAPI_OK) {
        printf("PAPI create eventset error %s\n", PAPI_strerror(ret));
        fflush(stdout);
        exit(-1);
    }

    int i;
    for(i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            //continue;
        } else {
            //printf("PAPI event %16s %s\n", info.symbol, info.long_descr);
            strcpy(_profile_event_names[i], info.symbol);
        }

        ret = PAPI_add_event(_profile_eventset, _profile_events[i]);
        if(ret != PAPI_OK) {
            printf("PAPI add event %d failed %s\n", i, PAPI_strerror(ret));
            fflush(stdout);
            exit(-1);
        }
    }

#if _PROFILE_PAPI_FILE == 1
    char filename[128];

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    sprintf(filename, "profile-%d-%d.out", tid, rank);

    _profile_fd = fopen(filename, "w+");
    if(_profile_fd == NULL) {
        printf("ERROR opening profile data file\n");
        exit(-1);
    }

    fprintf(_profile_fd, "VAR TIME");
    for(i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

        printf("PAPI event %16s %s\n", info.symbol, info.long_descr);
        fflush(stdout);

        fprintf(_profile_fd, " %s", info.symbol);
    }
    fprintf(_profile_fd, "\n");
#endif //_PROFILE_PAPI_FILE == 1

    //Start the events now.  We leave them counting all the time, and use
    //PAPI_Read() to get the values.
    int rc = PAPI_start(_profile_eventset);
    if(rc != PAPI_OK) {
        printf("papi start error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
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

#define PROFILE_START(v) __PROFILE_START(&(_profile_ ## v))

static void __PROFILE_START(struct profile_vars_t* v)
{
#if _PROFILE_PAPI_EVENTS == 1
#if 0
    int rc = PAPI_start(_profile_eventset);
    if(rc != PAPI_OK) {
        printf("papi start error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
    }
#endif
    int rc = PAPI_read(_profile_eventset, (long long*)v->tmp_ctrs);
    if(rc != PAPI_OK) {
        printf("papi start read error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
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



#define PROFILE_STOP(v) __PROFILE_STOP(#v, &_profile_ ## v)

static void __PROFILE_STOP(const char* name, struct profile_vars_t* v)
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
    uint64_t ctrs[NUM_EVENTS];

#if 0
    //int rc = PAPI_read_counters((long long*)ctrs, NUM_EVENTS);
    int rc = PAPI_stop(_profile_eventset, (long long*)ctrs);
    if(rc != PAPI_OK) {
        printf("papi read error %s %s\n", PAPI_strerror(rc), name); fflush(stdout);
        exit(-1);
    }
#endif

    int rc = PAPI_read(_profile_eventset, (long long*)ctrs);
    if(rc != PAPI_OK) {
        printf("papi start read error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
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
    for(i = 0; i < NUM_EVENTS; i++) {
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
    for(i = 0; i < NUM_EVENTS; i++) {
        fprintf(_profile_fd, " %lu", ctrs[i]);
    }

    fprintf(_profile_fd, "\n");
#endif //_PROFILE_PAPI_FILE == 1
#endif //_PROFILE_PAPI_EVENTS == 1
}


#define PROFILE_RESULTS(v, result) __PROFILE_RESULTS(#v, &_profile_ ## v, result)

static void __PROFILE_RESULTS(const char* name, struct profile_vars_t* v, profile_results_t* r) __attribute__((unused));

static void __PROFILE_RESULTS(const char* name, struct profile_vars_t* v, profile_results_t* r)
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

    MPI_Allreduce(v->ctrs, r->total_ctrs, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

#if _PROFILE_MAX_MIN
    MPI_Allreduce(v->ctr_max, r->max_ctrs, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(v->ctr_min, r->min_ctrs, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
#endif //_PROFILE_MAX_MIN

    for(i = 0; i < NUM_EVENTS; i++) {
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


#define PROFILE_SHOW(v) __PROFILE_SHOW(#v, &_profile_ ## v)

static void __PROFILE_SHOW(const char* name, struct profile_vars_t* v) __attribute__((unused));

static void __PROFILE_SHOW(const char* name, struct profile_vars_t* v)
{
    profile_results_t r;
    int rank;

    __PROFILE_RESULTS(name, v, &r);

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

        for(i = 0; i < NUM_EVENTS; i++) {
#if _PROFILE_MAX_MIN
            printf("PAPI %20s %lu total %10.3f avg %lu max %lu min\n",
                    _profile_event_names[i], r.total_ctrs[i], r.avg_ctrs[i],
                    r.max_ctrs[i], r.min_ctrs[i]);
#else
            printf("PAPI %20s %lu total %10.3f avg\n",
                    _profile_event_names[i], r.total_ctrs[i], r.avg_ctrs[i]);
#endif
        }
#endif
        fflush(stdout);
    }
}



#warning "PROFILING ON"

#else
#define PROFILE_DECLARE()
static inline void PROFILE_INIT(void) {}
static inline void PROFILE_FINALIZE(void) {}
#define PROFILE_VAR(var)
#define PROFILE_EXTERN(var)
#define PROFILE_START(var)
#define PROFILE_STOP(var)
#define PROFILE_RESULTS(var)
#define PROFILE_SHOW(var)
//#warning "PROFILING OFF"
#endif

#endif

