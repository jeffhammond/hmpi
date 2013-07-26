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

