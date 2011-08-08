#ifndef _AWF_PROFILE_HEADER
#define _AWF_PROFILE_HEADER

#define THREAD __thread

#ifndef _PROFILE
#define _PROFILE 0
#endif

#ifndef _PROFILE_PAPI_EVENTS
#define _PROFILE_PAPI_EVENTS 0
#endif

#ifndef _PROFILE_MPI
#define _PROFILE_MPI 0
#endif

#ifndef _PROFILE_HMPI
#define _PROFILE_HMPI 0
#endif

#if _PROFILE == 1

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <papi.h>


#if _PROFILE_PAPI_EVENTS == 1

#define NUM_EVENTS 2

static int _profile_events[NUM_EVENTS] =
        //{ PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_L2_TCM, PAPI_HW_INT };
        { PAPI_TOT_CYC, PAPI_TOT_INS };

extern THREAD FILE* _profile_fd;

#define PROFILE_DECLARE() \
  FILE* THREAD _profile_fd;
  ///*__thread*/ struct profile_info_t _profile_info; 

#else

#define PROFILE_DECLARE()
  ///*__thread*/ struct profile_info_t _profile_info; 

#endif


typedef struct profile_vars_t {
    uint64_t time;
    uint64_t count;
    uint64_t start;
#if _PROFILE_PAPI_EVENTS == 1
    uint64_t ctrs[NUM_EVENTS];
#endif
} profile_vars_t;


#define PROFILE_VAR(v) \
    THREAD profile_vars_t _profile_ ## v = {0};

#define PROFILE_EXTERN(v) \
    extern THREAD profile_vars_t _profile_ ## v;


//This needs to be declared once in a C file somewhere
//extern /*__thread*/ struct profile_info_t _profile_info;

static inline void PROFILE_INIT(void)
{
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if(ret < 0) {
        printf("PAPI init failure\n");
        fflush(stdout);
        exit(-1);
    }

    PAPI_thread_init(pthread_self);

#if _PROFILE_PAPI_EVENTS == 1
    int num_hwcntrs = 0;

    if ((num_hwcntrs = PAPI_num_counters()) <= PAPI_OK) {
        printf("ERROR PAPI_num_counters\n");
        exit(-1);
    }

    if(num_hwcntrs < NUM_EVENTS) {
        printf("ERROR PAPI reported < %d events available\n", NUM_EVENTS);
    }

    _profile_fd = fopen("profile.out", "w+");
    if(_profile_fd == NULL) {
        printf("ERROR opening profile data file\n");
        exit(-1);
    }

    fprintf(_profile_fd, "VAR TIME");
    for(int i = 0; i < NUM_EVENTS; i++) {
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
#endif
}


static inline void PROFILE_FINALIZE()
{
#if _PROFILE_PAPI_EVENTS == 1
    fclose(_profile_fd);
#endif
}


#define PROFILE_START(v) __PROFILE_START(&(_profile_ ## v))

static inline void __PROFILE_START(struct profile_vars_t* v)
{
#if _PROFILE_PAPI_EVENTS == 1
    PAPI_start_counters(_profile_events, NUM_EVENTS);
#endif
    //v->start = PAPI_get_real_usec();
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    v->start = ((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec) / 1000;
}


#define PROFILE_STOP(v) __PROFILE_STOP(#v, &_profile_ ## v)

static inline void __PROFILE_STOP(char* name, struct profile_vars_t* v)
{
    //Grab the time right away
    //uint64_t t = PAPI_get_real_usec() - v->start;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t t = (((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec) / 1000) - v->start;

#if _PROFILE_PAPI_EVENTS == 1
    //Grab counter values
    uint64_t ctrs[NUM_EVENTS];
    PAPI_read_counters((long long*)ctrs, NUM_EVENTS);
#endif

    //Accumulate the time
    v->time += t;
    v->count++;

#if _PROFILE_PAPI_EVENTS == 1
    //Accumulate the counter values
    fprintf(_profile_fd, "%s %lu", name, t);
    for(int i = 0; i < NUM_EVENTS; i++) {
        v->ctrs[i] += ctrs[i];
        fprintf(_profile_fd, " %lu", ctrs[i]);
    }

    fprintf(_profile_fd, "\n");
#endif
}


#define PROFILE_SHOW(v) __PROFILE_SHOW(#v, &_profile_ ## v)

static void __PROFILE_SHOW(char* name, struct profile_vars_t* v)
{
    printf("%12s cnt %-7lu time %lu us total %08.3lf avg\n",
            name, v->count, v->time, (double)v->time / v->count);

#if _PROFILE_PAPI_EVENTS == 1
    for(int i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

        printf("    %20s %lu total %8.3lf avg\n", info.symbol,
                v->ctrs[i], (double)v->ctrs[i] / v->count);
    }
#endif
}


#if _PROFILE_MPI == 1
#include <mpi.h>

#define PROFILE_SHOW_REDUCE(v) __PROFILE_SHOW_REDUCE(#v, &_profile_ ## v)

static void __PROFILE_SHOW_REDUCE(char* name, struct profile_vars_t* v)
{
    double rt, ra;

    double a = (double)v->time / v->count;
    MPI_Reduce(&v->time, &rt, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&a, &ra, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    int r;

    MPI_Comm_rank(MPI_COMM_WORLD, &r);
    if(r == 0) {
        printf("%12s cnt %-7lu time %lf us total %08.3lf avg\n",
                name, v->count, (double)v->time / 1000.0, (double)v->time / v->count);
    }

#if 0
#if _PROFILE_PAPI_EVENTS == 1
    for(int i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

        printf("    %20s %lu total %8.3lf avg\n", info.symbol,
                v->ctrs[i], (double)v->ctrs[i] / v->count);
    }
#endif
#endif
}

#elif _PROFILE_HMPI == 1
#include "hmpi.h"

#define PROFILE_SHOW_REDUCE(v) __PROFILE_SHOW_REDUCE(#v, &_profile_ ## v)

static void __PROFILE_SHOW_REDUCE(char* name, struct profile_vars_t* v)
{
    double rt, ra;

    double a = (double)v->time / v->count;
    HMPI_Allreduce(&v->time, &rt, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD);
    HMPI_Allreduce(&a, &ra, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD);

    int r;

    HMPI_Comm_rank(HMPI_COMM_WORLD, &r);
    if(r == 0) {
        printf("%12s cnt %-7lu time %lf us total %08.3lf avg\n",
                name, v->count, (double)v->time / 1000.0, (double)v->time / v->count);
    }

#if 0
#if _PROFILE_PAPI_EVENTS == 1
    for(int i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

        printf("    %20s %lu total %8.3lf avg\n", info.symbol,
                v->ctrs[i], (double)v->ctrs[i] / v->count);
    }
#endif
#endif
}

#else
#define PROFILE_SHOW_REDUCE(var)
#endif


#warning "PROFILING ON"

#else
#define PROFILE_DECLARE()
static inline void PROFILE_INIT(void) {}
static inline void PROFILE_FINALIZE(void) {}
#define PROFILE_VAR(var)
#define PROFILE_EXTERN(var)
#define PROFILE_START(var)
#define PROFILE_STOP(var)
#define PROFILE_SHOW(var)
#define PROFILE_SHOW_REDUCE(var)
//#warning "PROFILING OFF"
#endif

#endif

