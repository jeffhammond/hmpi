CC=mpicc -std=gnu99 
#CC=mpixlc

PTMALLOC=../ptmalloc3

WARN=-Wall -Wuninitialized #-Wno-unused-function
#CFLAGS=$(WARN) -O3 -mcpu=power7 -fomit-frame-pointer
#CFLAGS=$(WARN) $(INCLUDE) -O3 -march=native -fomit-frame-pointer
#CFLAGS=$(WARN) -O0 -g

LIBS=-lrt
INCS=#-D_PROFILE=1 -D_PROFILE_MPI=1 -D_PROFILE_PAPI_EVENTS=1 #-DFULL_PROFILE #-D_PROFILE_PAPI_EVENTS=1 #-DENABLE_OPI=1
SRCS=hmpi.c hmpi_opi.c sm_malloc.c #hmpi_coll.c nbc_op.c
USEQ_SRCS=hmpi.c #hmpi_coll.c nbc_op.c
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
	ar r libhmpi.a $(SRCS:%.c=%.o)
	ranlib libhmpi.a
#	make -C $(PTMALLOC)
#	ar r libhmpi.a $(SRCS:%.c=%.o) #$(PTMALLOC)/ptmalloc3.o $(PTMALLOC)/malloc.o
#	ar r opi.a $(OPI_SRCS:%.c=%.o)
#	ranlib opi.a

psm: $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ar r libhmpi.a $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ranlib libhmpi.a

udawn: LIBS =
udawn: $(SRCS:%.c=%.o)
	ar r libhmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib libhmpi.a

useq: CC=mpixlc
useq: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE)
useq: $(USEQ_SRCS:%.c=%.o)
	ar r libhmpi.a $(USEQ_SRCS:%.c=%.o)
	ranlib libhmpi.a

useq_debug: LIBS =
useq_debug: CC=mpixlc
useq_debug: CFLAGS=-O0 -g -qhot=novector -qsimd=auto $(INCLUDE)
useq_debug: $(USEQ_SRCS:%.c=%.o)
	ar sr libhmpi.a $(USEQ_SRCS:%.c=%.o)

#main: CFLAGS = -g -O -D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS+=-D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS=-O5 -qhot=novector -qsimd=auto -D_PROFILE=1 -D_PROFILE_HMPI=1
main: all $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

main_useq: CC=mpixlc
main_useq: CFLAGS=-O2 -g $(INCLUDE)
main_useq: useq $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

debug: CFLAGS = $(WARN) -g -O0 -rdynamic $(INCLUDE)
debug: $(SRCS:%.c=%.o) 
	ar r libhmpi.a $(SRCS:%.c=%.o)
	ranlib libhmpi.a
#	$(CC) $(INCS) $(CFLAGS) $(LDFLAGS) -o $(PROG) $(SRCS:%.c=%.o) $(LIBS)

opi: all example_opi.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi example_opi.o libhmpi.a  $(LIBS)

opi_useq: LIBS =
opi_useq: CC=mpixlc
opi_useq: CFLAGS=-O3 -qhot=novector -qsimd=auto -qlist -qreport -qsource
opi_useq: OPI_SRCS += example_opi.c
opi_useq: all $(OPI_SRCS:%.c=%.o) opi.h
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi $(OPI_SRCS:%.c=%.o) libhmpi.a  $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o *.a

