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
#ifndef __OPI_H__
#define __OPI_H__
#include "hmpi.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OPI_Request_info {
    HMPI_Request req;
    void* ptr;
    int do_free;
} OPI_Request_info;

typedef OPI_Request_info* OPI_Request;


void OPI_Init(void);
void OPI_Finalize(void);

int OPI_Alloc(void** ptr, size_t length);
int OPI_Free(void** ptr);

int OPI_Give(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req);

int OPI_Take(void** ptr, int count, MPI_Datatype datatype, int rank, int tag, MPI_Comm comm, OPI_Request* req);

int OPI_Test(OPI_Request *request, int *flag, HMPI_Status *status);
int OPI_Testall(int count, OPI_Request *requests, int* flag, HMPI_Status *statuses);
int OPI_Wait(OPI_Request *request, HMPI_Status *status);
int OPI_Waitall(int count, OPI_Request* requests, HMPI_Status* statuses);
int OPI_Waitany(int count, OPI_Request* requests, int* index, HMPI_Status *status);

#ifdef __cplusplus
}
#endif
#endif
