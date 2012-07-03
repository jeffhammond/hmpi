#ifndef _AWF_PROFILE_HEADER
#define _AWF_PROFILE_HEADER

#define THREAD __thread

#ifndef _PROFILE
#define _PROFILE 0
#endif

#ifndef _PROFILE_PAPI_EVENTS
#define _PROFILE_PAPI_EVENTS 0
#endif

#ifndef _PROFILE_PAPI_FILE
#define _PROFILE_PAPI_FILE 0
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

#if _PROFILE_MPI == 1
#include <mpi.h>
#endif

#if _PROFILE_HMPI == 1
#include "hmpi.h"
#endif



#if _PROFILE_PAPI_EVENTS == 1
#include <papi.h>

#define NUM_EVENTS 3

static int _profile_events[NUM_EVENTS] =
          { PAPI_L2_TCA, PAPI_L2_TCM, PAPI_TOT_INS/*, PAPI_TOT_CYC*/ };
//        { PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_L3_TCM/*, PAPI_HW_INT*/ };
//        { PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_L2_TCM, PAPI_RES_STL };
//        { PAPI_TOT_CYC, PAPI_TOT_INS, 1073741862, 1073741935 };
        //{ PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_HW_INT, 1073741935 };

//__thread int _profile_eventset = PAPI_NULL;

extern THREAD FILE* _profile_fd;

#if _PROFILE_PAPI_FILE == 1
#define PROFILE_DECLARE() \
  THREAD FILE* _profile_fd; \
  THREAD int _profile_eventset = PAPI_NULL;
//  __thread struct profile_info_t _profile_info; 

#else
#define PROFILE_DECLARE() \
  THREAD int _profile_eventset = PAPI_NULL;
//  __thread struct profile_info_t _profile_info; 
#endif

#else

#define PROFILE_DECLARE()
//  __thread struct profile_info_t _profile_info; 

#endif

//extern __thread struct profile_info_t _profile_info;
extern THREAD int _profile_eventset;

typedef struct profile_vars_t {
    uint64_t time;
    uint64_t count;
    uint64_t start;
    uint64_t min;
    uint64_t max;
#if _PROFILE_PAPI_EVENTS == 1
    uint64_t ctrs[NUM_EVENTS];
    uint64_t ctr_min[NUM_EVENTS];
    uint64_t ctr_max[NUM_EVENTS];
#endif
    float mhz;
} profile_vars_t;


#define PROFILE_VAR(v) \
    THREAD profile_vars_t _profile_ ## v = {0}

#define PROFILE_EXTERN(v) \
    extern THREAD profile_vars_t _profile_ ## v

#define PROFILE_RESET(v) \
    memset(&_profile_ ## v, 0, sizeof(profile_vars_t))

//This needs to be declared once in a C file somewhere
//extern /*__thread*/ struct profile_info_t _profile_info;

static inline void PROFILE_INIT(int tid)
{
  if(tid == 0) {
#if _PROFILE_PAPI_EVENTS == 1
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if(ret < 0) {
        printf("PAPI init failure %s\n", PAPI_strerror(ret));
        fflush(stdout);
        exit(-1);
    }

    PAPI_thread_init((long unsigned int (*)())pthread_self);

    int num_hwcntrs = 0;

    if ((num_hwcntrs = PAPI_num_counters()) <= PAPI_OK) {
        printf("ERROR PAPI_num_counters\n");
        exit(-1);
    }

    if(num_hwcntrs < NUM_EVENTS) {
        printf("ERROR PAPI reported < %d events available\n", NUM_EVENTS);
    }

    //TODO - consider using this in HMPI
    //PAPI_hw_info_t* info = PAPI_get_hardware_info();

    //printf("PAPI ncpu %d nnodes %d totalcpus %d\n", info->ncpu, info->nnodes, info->totalcpus);
    //fflush(stdout);

#endif
  }

#if _PROFILE_PAPI_EVENTS == 1
    int ret = PAPI_create_eventset(&_profile_eventset);
    if(ret != PAPI_OK) {
        printf("PAPI create eventset error %s\n", PAPI_strerror(ret));
        fflush(stdout);
        exit(-1);
    }

    int i;
    for(i = 0; i < NUM_EVENTS; i++) {
        if(tid == 0) {
            PAPI_event_info_t info;
            if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
                printf("ERROR PAPI_get_event_info %d\n", i);
                //continue;
            } else {
                printf("PAPI event %16s %s\n", info.symbol, info.long_descr);
                fflush(stdout);
            }
        }

        ret = PAPI_add_event(_profile_eventset, _profile_events[i]);
        if(ret != PAPI_OK) {
            printf("PAPI add event %d failed %s\n", i, PAPI_strerror(ret));
            fflush(stdout);
            exit(-1);
        }
    }
#endif


#if _PROFILE_PAPI_EVENTS == 1
#if _PROFILE_PAPI_FILE == 1
    char filename[128];

#if _PROFILE_HMPI == 1
    int rank;
    HMPI_Comm_rank(HMPI_COMM_WORLD, &rank);
    sprintf(filename, "profile-%d-%d.out", tid, rank);
#elif _PROFILE_MPI == 1
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    sprintf(filename, "profile-%d-%d.out", tid, rank);
#else
    sprintf(filename, "profile-%d.out", tid);
#endif
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
#endif
#endif
}


static inline void PROFILE_FINALIZE()
{
#if _PROFILE_PAPI_EVENTS == 1
#if _PROFILE_PAPI_FILE == 1
    fclose(_profile_fd);
#endif
#endif
}


#define PROFILE_START(v) __PROFILE_START(&(_profile_ ## v))

static inline void __PROFILE_START(struct profile_vars_t* v)
{
#if _PROFILE_PAPI_EVENTS == 1
    //int rc = PAPI_start_counters(_profile_events, NUM_EVENTS);
    int rc = PAPI_start(_profile_eventset);
    if(rc != PAPI_OK) {
        printf("papi start error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
    }
#endif
    //v->start = PAPI_get_real_usec();
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    v->start = ((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec) / 1000;
}


#define PROFILE_STOP(v) __PROFILE_STOP(#v, &_profile_ ## v)

static inline void __PROFILE_STOP(const char* name, struct profile_vars_t* v)
{
    //Grab the time right away
    //uint64_t t = PAPI_get_real_usec() - v->start;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t t = (((uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec) / 1000) - v->start;

#if _PROFILE_PAPI_EVENTS == 1
    //Grab counter values
    uint64_t ctrs[NUM_EVENTS] = {0};
    //int rc = PAPI_read_counters((long long*)ctrs, NUM_EVENTS);
    int rc = PAPI_stop(_profile_eventset, (long long*)ctrs);
    if(rc != PAPI_OK) {
        printf("papi read error %s\n", PAPI_strerror(rc)); fflush(stdout);
        exit(-1);
    }
#endif

    //Accumulate the time
    v->time += t;
    v->count++;

#if 0
    if(t > 1000 * 1000) {
        printf("DYING!\n"); fflush(stdout);
        assert(0);
    }
#endif
    if(t < v->min || v->min == 0) {
        v->min = t;
    }

    if(t > v->max) {
        v->max = t;
    }

#if _PROFILE_PAPI_EVENTS == 1
    int i;

    //Accumulate the counter values
    for(i = 0; i < NUM_EVENTS; i++) {
        v->ctrs[i] += ctrs[i];
        if(v->ctr_max[i] < ctrs[i]) {
            v->ctr_max[i] = ctrs[i];
        }

        if(v->ctr_min[i] > ctrs[i] || v->ctr_min[i] == 0) {
            //if(ctrs[i] == 0) {
            //    printf("setting 0 min\n"); fflush(stdout);
            //}
            v->ctr_min[i] = ctrs[i];
        }

        //printf("ctr %llu val %llu max %llu\n", v->ctrs[i], ctrs[i], v->ctr_max[i]);
    }
    //fflush(stdout);

#if _PROFILE_PAPI_FILE == 1
    fprintf(_profile_fd, "%s %lu", name, t);
    for(i = 0; i < NUM_EVENTS; i++) {
        fprintf(_profile_fd, " %lu", ctrs[i]);
    }

    fprintf(_profile_fd, "\n");
#endif
#endif
}


#define PROFILE_SHOW(v) __PROFILE_SHOW(#v, &_profile_ ## v)

static void __PROFILE_SHOW(char* name, struct profile_vars_t* v) __attribute__((unused));

static void __PROFILE_SHOW(char* name, struct profile_vars_t* v)
{
    printf("%12s cnt %-7lu time %lu us total %08.3lf avg\n",
            name, v->count, v->time, (double)v->time / v->count);

#if _PROFILE_PAPI_EVENTS == 1
    int i;
    for(i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

/*#if _PROFILE_HMPI == 1
        int rank;
        HMPI_Comm_rank(HMPI_COMM_WORLD, &rank);
        printf("%3d %20s %lu total %8.3lf avg %lu max\n", rank, info.symbol,
                v->ctrs[i], (double)v->ctrs[i] / v->count, v->ctr_max[i]);
#else*/
        printf("    %20s %lu total %8.3lf avg %lu max\n", info.symbol,
                v->ctrs[i], (double)v->ctrs[i] / v->count, v->ctr_max[i]);
//#endif
    }
#endif
}


#if _PROFILE_MPI == 1 || _PROFILE_HMPI == 1

#define PROFILE_SHOW_REDUCE(v) __PROFILE_SHOW_REDUCE(#v, &_profile_ ## v)

static void __PROFILE_SHOW_REDUCE(const char* name, struct profile_vars_t* v) __attribute__((unused));

static void __PROFILE_SHOW_REDUCE(const char* name, struct profile_vars_t* v)
{
    uint64_t r_count;
    uint64_t r_time;
    uint64_t r_max;
    uint64_t r_min;
    double r_avg;

    double a = (double)v->time / v->count;

    int rank;
    int size;

#if _PROFILE_HMPI == 1
    HMPI_Comm_rank(HMPI_COMM_WORLD, &rank);
    HMPI_Comm_size(HMPI_COMM_WORLD, &size);

    HMPI_Reduce(&v->count, &r_count, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(&v->time, &r_time, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(&v->max, &r_max, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(&v->min, &r_min, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(&a, &r_avg, 1, MPI_DOUBLE, MPI_SUM, 0, HMPI_COMM_WORLD);
#else
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Reduce(&v->count, &r_count, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&v->time, &r_time, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&v->max, &r_max, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&v->min, &r_min, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&a, &r_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
#endif

#if _PROFILE_PAPI_EVENTS == 1
    uint64_t rtc[NUM_EVENTS];
    double avg_ctr[NUM_EVENTS];
    double rac[NUM_EVENTS];
    uint64_t min_ctr[NUM_EVENTS];
    uint64_t max_ctr[NUM_EVENTS];
    int i;

    for(i = 0; i < NUM_EVENTS; i++) {
        //printf("%d %d ctr %lu\n", rank, i, v->ctrs[i]); fflush(stdout);
        avg_ctr[i] = (double)v->ctrs[i] / (double)v->count;
    }
    //printf("last ctr %llu max %llu min %llu avg %f\n", v->ctrs[1], v->ctr_max[1], v->ctr_min[1], avg_ctr[1]);
    //fflush(stdout);


#if _PROFILE_HMPI == 1
    HMPI_Reduce(v->ctrs, rtc, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(&avg_ctr, rac, NUM_EVENTS,
            MPI_DOUBLE, MPI_SUM, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(v->ctr_min, min_ctr, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, HMPI_COMM_WORLD);
    HMPI_Reduce(v->ctr_max, max_ctr, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, HMPI_COMM_WORLD);
#else 
    MPI_Reduce(v->ctrs, rtc, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&avg_ctr, rac, NUM_EVENTS,
            MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(v->ctr_min, min_ctr, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(v->ctr_max, max_ctr, NUM_EVENTS,
            MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
#endif

#endif //_PROFILE_PAPI_EVENTS == 1

    if(rank == 0) {
        //printf("TIME %12s cnt %-7lu time %8.3f ms total %11.6f ms avg\n", name,
        //        v->count, (double)v->time / 1000.0, ((double)v->time / v->count) / 1000.0);
        printf("TIME %12s cnt %-7lu time %10.3f ms total %13.6f ms avg %13.6f max %13.6f min\n", name,
                r_count, (double)r_time / 1000.0,
                ((double)r_time / r_count) / 1000.0,
                r_max / 1000.0, r_min / 1000.0);

#if _PROFILE_PAPI_EVENTS == 1
        for(i = 0; i < NUM_EVENTS; i++) {
            PAPI_event_info_t info;
            if(PAPI_get_event_info(_profile_events[i], &info) != PAPI_OK) {
                printf("ERROR PAPI_get_event_info %d\n", i);
                continue;
            }

            //printf("PAPI %20s %lu total %8.3lf avg\n", info.symbol, rtc[i], rac[i]);
            printf("PAPI %20s %llu total %8.3f avg %llu min %llu max\n",
                    info.symbol, rtc[i], (double)rac[i] / size,
                    min_ctr[i], max_ctr[i]);
            //printf("PAPI %20s %llu\n", info.symbol, rtc[i]);
        }
#endif
    }
}


#else
#define PROFILE_SHOW_REDUCE(var)
#endif


#warning "PROFILING ON"

#else
#define PROFILE_DECLARE()
static inline void PROFILE_INIT(int tid) {}
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

