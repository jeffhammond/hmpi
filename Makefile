CC=mpicc -std=gnu99
#CC=mpixlc

WARN=-Wall -Wuninitialized #-Wno-unused-function
#CFLAGS=$(WARN) -O3 -mcpu=power7 -fomit-frame-pointer
CFLAGS=$(WARN) -O3 -march=native -fomit-frame-pointer

LIBS=#-lrt -lpapi
INCS=-D_PROFILE=1 -D_PROFILE_HMPI=1 #-D_PROFILE_PAPI_EVENTS=1
SRCS=hmpi.c hmpi_coll.c nbc_op.c
MAIN=main.c
HDRS=hmpi.h barrier.h lock.h profile2.h

PSM_SRCS=hmpi_psm.c hmpi_coll.c nbc_op.c libpsm.c
PSM_HDRS=hmpi_psm.h barrier.h lock.h profile2.h libpsm.h
PSM_LIBS=$(LIBS) -lpsm_infinipath

all: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

udawn: LIBS =
udawn: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

useq: LIBS =
useq: CC=mpixlc
useq: CFLAGS=-O5 -qhot=novector -qsimd=auto -qlist -qreport -qsource
useq: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

useq_debug: LIBS =
useq_debug: CC=mpixlc
useq_debug: CFLAGS=-O0 -g
useq_debug: $(SRCS:%.c=%.o)
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a

main: CFLAGS = -g -O
main: all $(MAIN:%.c=%.o)
	$(CC) $(CCFLAGS) $(LDFLAGS) -o main main.o hmpi.a $(LIBS)

debug: CFLAGS = $(WARN) -g -O 
debug: $(SRCS:%.c=%.o) 
	ar r hmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib hmpi.a
#	$(CC) $(INCS) $(CFLAGS) $(LDFLAGS) -o $(PROG) $(SRCS:%.c=%.o) $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o $(PROG)

