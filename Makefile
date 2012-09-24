CC=mpicc -std=gnu99
#CC=mpixlc

WARN=-Wall -Wuninitialized #-Wno-unused-function
#CFLAGS=$(WARN) -O3 -mcpu=power7 -fomit-frame-pointer
CFLAGS=$(WARN) -O3 -march=native -fomit-frame-pointer

LIBS=-lrt
INCS=#-DENABLE_OPI=1 #-D_PROFILE=1 -D_PROFILE_HMPI=1 #-D_PROFILE_PAPI_EVENTS=1
SRCS=hmpi.c hmpi_coll.c nbc_op.c hmpi_opi.c
MAIN=main.c
HDRS=hmpi.h barrier.h lock.h profile2.h

OPI_SRCS=#opi.c

PSM_SRCS=hmpi_psm.c hmpi_coll.c nbc_op.c libpsm.c
PSM_HDRS=hmpi_psm.h barrier.h lock.h profile2.h libpsm.h
PSM_LIBS=$(LIBS) -lpsm_infinipath

PAMI_SRCS=hmpi.c hmpi_coll.c nbc_op.c libpami.c
PAMI_HDRS=barrier.h lock.h profile2.h libpami.h
PAMI_LIBS= -lpami


all: $(SRCS:%.c=%.o) 
	ar r hmpi.a $(SRCS:%.c=%.o)
	ranlib hmpi.a
#	ar r opi.a $(OPI_SRCS:%.c=%.o)
#	ranlib opi.a

psm: $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ar r hmpi.a $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ranlib hmpi.a

udawn: LIBS =
udawn: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

useq: CC=mpixlc
useq: CFLAGS=-O5 -qhot=novector -qsimd=auto -DENABLE_PAMI
useq: $(PAMI_SRCS:%.c=%.o)
	ar r hmpi.a $(PAMI_SRCS:%.c=%.o)
	ranlib hmpi.a

useq_debug: LIBS =
useq_debug: CC=mpixlc
useq_debug: CFLAGS=-O0 -g
useq_debug: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

#main: CFLAGS = -g -O -D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS+=-D_PROFILE=1 -D_PROFILE_HMPI=1
main: CFLAGS=-O5 -qhot=novector -qsimd=auto -D_PROFILE=1 -D_PROFILE_HMPI=1
main: all $(MAIN:%.c=%.o)
	$(CC) $(CCFLAGS) $(LDFLAGS) -o main main.o hmpi.a $(LIBS)

debug: CFLAGS = $(WARN) -g -O 
debug: $(SRCS:%.c=%.o) 
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a
#	$(CC) $(INCS) $(CFLAGS) $(LDFLAGS) -o $(PROG) $(SRCS:%.c=%.o) $(LIBS)

opi: all example_opi.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi example_opi.o hmpi.a  $(LIBS)

opi_useq: LIBS =
opi_useq: CC=mpixlc
opi_useq: CFLAGS=-O5 -qhot=novector -qsimd=auto -qlist -qreport -qsource
opi_useq: OPI_SRCS += example_opi.c
opi_useq: all $(OPI_SRCS:%.c=%.o) opi.h
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi $(OPI_SRCS:%.c=%.o) hmpi.a  $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o *.a

