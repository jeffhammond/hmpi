#Copyright (c) 2010-2013 The Trustees of Indiana University.
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#
#- Redistributions of source code must retain the above copyright notice, this
#  list of conditions and the following disclaimer.
#
#- Redistributions in binary form must reproduce the above copyright notice,
#  this list of conditions and the following disclaimer in the documentation
#  and/or other materials provided with the distribution.
#
#- Neither the Indiana University nor the names of its contributors may be used
#  to endorse or promote products derived from this software without specific
#  prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
#ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

CC=mpicc -std=gnu99 

WARN=-Wall -Wuninitialized -Winline #-Wno-unused-function
CFLAGS+=$(WARN) -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
#CFLAGS=$(WARN) -O3 -mcpu=power7 -fomit-frame-pointer
#CFLAGS=$(WARN) $(INCLUDE) -O3 -march=native -fomit-frame-pointer
#CFLAGS=$(WARN) -O0 -g

LIBS=-lrt -lnuma

INCS=
INCS+=-DENABLE_OPI=1
#INCS+=-DHMPI_LOGCALLS=1 
#INCS+=-DHMPI_CHECKSUM=1
#INCS+=-D_PROFILE=1
#INCS+=-DFULL_PROFILE
#INCS+= -DHMPI_STATS
#INCS+=-D_PROFILE_PAPI_EVENTS=1

SRCS=hmpi_p2p.c hmpi.c #hmpi_coll.c nbc_op.c #hmpi_opi.c
SRCS+=hmpi_opi.c
ASSRCS=hmpi_p2p.s hmpi.s #hmpi_coll.c nbc_op.c #hmpi_opi.c
MAIN=main.c
HDRS=hmpi.h barrier.h lock.h profile2.h


all: INCS+=-DUSE_NUMA=1 -DUSE_MCS=1
all: SRCS+=sm_malloc.c
all: $(SRCS:%.c=%.o) sm_malloc.o
	ar sr libhmpi.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

#bgq: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE) -qinline=auto:level=5 -qassert=refalign -qlibansi -qlibmpi -qipa -qhot  -qprefetch=aggressive
bgq: CC=mpixlc
bgq: SRCS+=sm_malloc.c
bgq: CFLAGS=-O3 -qcompact -qhot=novector -qsimd=auto -qlibansi -qlibmpi $(INCLUDE)
bgq: $(SRCS:%.c=%.o) sm_malloc.o
	ar sr libhmpi-bgq.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

bgq_debug: LIBS =
bgq_debug: CC=mpixlc
bgq_debug: CFLAGS=-O0 -g -qhot=novector -qsimd=auto $(INCLUDE)
bgq_debug: SRCS+=sm_malloc.c
bgq_debug: $(SRCS:%.c=%.o) sm_malloc.o
	ar sr libhmpi-bgq.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

#main: CFLAGS = -g -O -D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS+=-D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS=-O5 -qhot=novector -qsimd=auto -D_PROFILE=1 -D_PROFILE_HMPI=1
main: all $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

bgq_main: CC=mpixlc
bgq_main: CFLAGS=-O2 -g $(INCLUDE)
bgq_main: bgq $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

debug: CFLAGS = $(WARN) -g -O0 -rdynamic $(INCLUDE)
debug: SRCS+=sm_malloc.c
debug: $(SRCS:%.c=%.o)  sm_malloc.o
	ar sr libhmpi.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

opi: all example_opi.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi example_opi.o libhmpi.a  $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o libhmpi.a

bgq_clean:
	rm -f *.o libhmpi-bgq.a

