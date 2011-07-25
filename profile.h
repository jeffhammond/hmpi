#ifndef _AWF_PROFILE_HEADER
#define _AWF_PROFILE_HEADER

//#define _PROFILE 1

#ifdef _PROFILE

#include <stdio.h>
#include <papi.h>

#define PROFILE_VAR(var) \
    uint64_t time_ ## var; \
    uint64_t count_ ## var; \
    uint64_t tmp_ ## var;

struct profile_info_t {
    PROFILE_VAR(match);
    PROFILE_VAR(copy);
    PROFILE_VAR(send);
    PROFILE_VAR(add_send_req);
    PROFILE_VAR(barrier);
    PROFILE_VAR(alltoall);
};

//This needs to be declared once in a C file somewhere
extern __thread struct profile_info_t _profile_info;


#define PROFILE_DECLARE() \
  __thread struct profile_info_t _profile_info;

static inline void PROFILE_INIT(void)
{
    int ret = PAPI_library_init(PAPI_VER_CURRENT);
    if(ret < 0) {
        printf("PAPI init failure\n");
        fflush(stdout);
        exit(-1);
    }
}


#define PROFILE_START(var) \
    (_profile_info.tmp_ ## var = PAPI_get_real_usec())

#define PROFILE_STOP(var) { \
    uint64_t t = PAPI_get_real_usec(); \
    _profile_info.time_ ## var += (t - _profile_info.tmp_ ## var); \
    _profile_info.count_ ## var ++; \
}


#if 0
    do { \
      HRT_TIMESTAMP_T t2; \
      uint64_t t = 0; \
      HRT_GET_TIMESTAMP(t2); \
      HRT_GET_ELAPSED_TICKS(_profile_info.tmp_ ## var, t2, &t); \
      if((int64_t)t > 0) { \
        _profile_info.time_ ## var += t; \
        _profile_info.count_ ## var ++; \
        /*printf("%d %lu %lu\n", me, t, _profile_info.time_ ## var); */\
      } \
    } while(0);
#endif


#if 0
#define PROFILE_SHOW(var, r) \
    do { \
      double t; \
      t = HRT_GET_USEC(_profile_info.time_ ## var) / 1000.0; \
      printf("%d " #var " time %lf ms total %lf avg\n", \
              r, t, t / (double)_profile_info.count_ ## var); \
    } while(0);
#endif


#define PROFILE_SHOW_REDUCE(var, r) { \
    double rt, ra; \
    double t = (double)_profile_info.time_ ## var; \
    double a = t / (double)_profile_info.count_ ## var; \
    HMPI_Allreduce(&t, &rt, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD); \
    HMPI_Allreduce(&a, &ra, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD); \
    int size; \
    HMPI_Comm_size(HMPI_COMM_WORLD, &size); \
    if(r == 0) { \
        printf("%8s cnt %-7llu time %-8.3lf us total %08.3lf avg\n", \
                #var, _profile_info.count_ ## var, \
                rt / (double)size, ra / (double)size); \
    } \
}

#if 0
    do { \
      double t, a; \
      double rt, ra; \
      int size; \
      t = HRT_GET_USEC(_profile_info.time_ ## var) / 1000.0; \
      a = t / (double)_profile_info.count_ ## var; \
      HMPI_Allreduce(&t, &rt, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD); \
      HMPI_Allreduce(&a, &ra, 1, MPI_DOUBLE, MPI_SUM, HMPI_COMM_WORLD); \
      HMPI_Comm_size(HMPI_COMM_WORLD, &size); \
      if(r == 0) { \
        printf(#var " time %lf ms total %lf avg\n", \
                rt / (double)size, ra / (double)size); \
      } \
    } while(0);
#endif



#warning "PROFILING ON"

#else
#define PROFILE_DECLARE()
static inline void PROFILE_INIT(void) {}
#define PROFILE_VAR(var)
#define PROFILE_START(var)
#define PROFILE_STOP(var)
#define PROFILE_SHOW(var, r)
#define PROFILE_SHOW_REDUCE(var, r)
#warning "PROFILING OFF"
#endif


#endif

