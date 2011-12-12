#include <stdio.h>
#include <stdlib.h>
#include <papi.h>

#include "events.h"

//#define _P_DEBUG 1

static void mypapi_init(void)
{
    int num_hwcntrs = 0;
    int i;

    PAPI_library_init(PAPI_VER_CURRENT);

    for(i = 0; i < NUM_EVENTS; i++) {
        PAPI_event_info_t info;
        if(PAPI_get_event_info(_p_events[i], &info) != PAPI_OK) {
            printf("ERROR PAPI_get_event_info %d\n", i);
            continue;
        }

        printf("PAPI event %16s %s\n", info.symbol, info.long_descr);
        fflush(stdout);
    }

#ifdef _P_DEBUG
    if ((num_hwcntrs = PAPI_num_counters()) <= PAPI_OK) {
        printf("ERROR PAPI_num_counters\n");
        exit(-1);
    }

    printf("This system has %d available counters.\n", num_hwcntrs);
#endif
}

static void mypapi_start(void)
{
    int ret;
    /* Start counting events */
    ret = PAPI_start_counters(_p_events, NUM_EVENTS);
    if(ret == PAPI_ECNFLCT) {
        printf("ERROR PAPI_start_counters ECNFLCT\n");
        exit(-1);
    } else if(ret == PAPI_ENOEVNT) {
        printf("ERROR PAPI_start_counters ENOEVNT\n");
        exit(-1);
    } else if(ret != PAPI_OK) {
        printf("ERROR PAPI_start_counters\n");
        exit(-1);
    }
}

static void mypapi_stop(long long int* values)
{
    int ret = PAPI_stop_counters(values, NUM_EVENTS);
#ifdef _P_DEBUG
    if(ret != PAPI_OK) {
        printf("ERROR PAPI_stop_counters\n");
        exit(-1);
    }
#endif
}

static void mypapi_add(long long int* values)
{
    int ret = PAPI_accum_counters(values, NUM_EVENTS);
#ifdef _P_DEBUG
    if(ret != PAPI_OK) {
        printf("ERROR PAPI_read_counters\n");
        exit(-1);
    }
#endif
}

static void mypapi_read(long long int* values)
{
    int ret = PAPI_read_counters(values, NUM_EVENTS);

#ifdef _P_DEBUG
    if(ret != PAPI_OK) {
        printf("ERROR PAPI_read_counters\n");
        exit(-1);
    }
#endif
}

