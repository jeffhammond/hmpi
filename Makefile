CC=mpicc -std=gnu99 

WARN=-Wall -Wuninitialized -Winline #-Wno-unused-function
CFLAGS+=$(WARN) -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
#CFLAGS=$(WARN) -O3 -mcpu=power7 -fomit-frame-pointer
#CFLAGS=$(WARN) $(INCLUDE) -O3 -march=native -fomit-frame-pointer
#CFLAGS=$(WARN) -O0 -g

LIBS=-lrt -lnuma

INCS=
#INCS+=-DENABLE_OPI=1
#INCS+=-DHMPI_LOGCALLS=1 
#INCS+=-DHMPI_CHECKSUM=1
INCS+=-D_PROFILE=1 -D_PROFILE_MPI=1
#INCS+=-DFULL_PROFILE
INCS+= -DHMPI_STATS
#INCS+=-D_PROFILE_PAPI_EVENTS=1

SRCS=hmpi_p2p.c hmpi.c #hmpi_coll.c nbc_op.c #hmpi_opi.c
ASSRCS=hmpi_p2p.s hmpi.s #hmpi_coll.c nbc_op.c #hmpi_opi.c
MAIN=main.c
HDRS=hmpi.h barrier.h lock.h profile2.h


all: INCS+=-DUSE_NUMA=1 
all: SRCS+=sm_malloc.c
all: $(SRCS:%.c=%.o) sm_malloc.o
	ar r libhmpi.a $(SRCS:%.c=%.o)
	ranlib libhmpi.a

psm: $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ar r libhmpi.a $(SRCS:%.c=%.o) $(PSM_SRCS:%.c=%.o)
	ranlib libhmpi.a

udawn: LIBS =
udawn: $(SRCS:%.c=%.o)
	ar r libhmpi.a hmpi.o hmpi_coll.o nbc_op.o
	ranlib libhmpi.a

#bgq: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE) -qinline=auto:level=5 -qassert=refalign -qlibansi -qlibmpi -qipa -qhot  -qprefetch=aggressive
bgq: CC=mpixlc
bgq: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE)
bgq: $(SRCS:%.c=%.o)
	ar sr libhmpi-bgq.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

bgq_as: CC=mpixlc
bgq_as: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE)
bgq_as: $(SRCS:%.c=%.s)

bgq_as_link: CC=mpixlc
bgq_as_link: CFLAGS=-O3 -qhot=novector -qsimd=auto $(INCLUDE)
bgq_as_link: $(ASSRCS:%.s=%.o)
	ar sr libhmpi-bgq.a $(ASSRCS:%.s=%.o)

bgq-gcc: CC=mpicc
bgq-gcc: CFLAGS=-O3 -fomit-frame-pointer -m64 -std=gnu99 $(INCLUDE)
bgq-gcc: $(SRCS:%.c=%.o)
	ar sr libhmpi-bgq.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

bgq_debug: LIBS =
bgq_debug: CC=mpixlc
bgq_debug: CFLAGS=-O3 -g -qhot=novector -qsimd=auto $(INCLUDE)
bgq_debug: $(SRCS:%.c=%.o)
	ar sr libhmpi-bgq.a $(SRCS:%.c=%.o)
	rm $(SRCS:%.c=%.o)

#main: CFLAGS = -g -O -D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS+=-D_PROFILE=1 -D_PROFILE_HMPI=1
#main: CFLAGS=-O5 -qhot=novector -qsimd=auto -D_PROFILE=1 -D_PROFILE_HMPI=1
main: all $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

main_bgq: CC=mpixlc
main_bgq: CFLAGS=-O2 -g $(INCLUDE)
main_bgq: bgq $(MAIN:%.c=%.o)
	$(CC) $(CFLAGS) $(LDFLAGS) -Wl,--allow-multiple-definition -o main main.o libhmpi.a $(LIBS)

debug: CFLAGS = $(WARN) -g -O0 -rdynamic $(INCLUDE)
debug: SRCS+=sm_malloc.c
debug: $(SRCS:%.c=%.o)  sm_malloc.o
	ar r libhmpi.a $(SRCS:%.c=%.o)
	ranlib libhmpi.a
#	$(CC) $(INCS) $(CFLAGS) $(LDFLAGS) -o $(PROG) $(SRCS:%.c=%.o) $(LIBS)

opi: all example_opi.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi example_opi.o libhmpi.a  $(LIBS)

opi_bgq: LIBS =
opi_bgq: CC=mpixlc
opi_bgq: CFLAGS=-O3 -qhot=novector -qsimd=auto -qlist -qreport -qsource
opi_bgq: OPI_SRCS += example_opi.c
opi_bgq: all $(OPI_SRCS:%.c=%.o) opi.h
	$(CC) $(CCFLAGS) $(LDFLAGS) -o example_opi $(OPI_SRCS:%.c=%.o) libhmpi.a  $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

.s.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

.c.s: $(HDRS)
	$(CC) -S $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o libhmpi.a

clean_bgq:
	rm -f *.o libhmpi-bgq.a

