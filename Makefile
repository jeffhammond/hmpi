CC=mpicc -std=gnu99
#CC=mpixlc

WARN=-Wall -Wuninitialized
CFLAGS+=$(WARN)

HWLOC=/g/g19/friedley/local

#PROG=lulesh
LIBS=-L$(HWLOC)/lib -lhwloc
INCS=-I$(HWLOC)/include
SRCS=hmpi.c hmpi_coll.c nbc_op.c
HDRS=hmpi.h barrier.h lock.h profile2.h

all: $(SRCS:%.c=%.o)
	ar r $@ hmpi.o nbc_op.o
	ranlib $@

udawn: LIBS =
udawn: $(SRCS:%.c=%.o)
	ar r $@ hmpi.o nbc_op.o
	ranlib $@

main: CFLAGS = -g -O
main: all main.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o main hmpi.a $(LIBS)

debug: CFLAGS = -g -O
debug: $(SRCS:%.c=%.o) 
	$(CC) $(INCS) $(CFLAGS) $(LDFLAGS) -o $(PROG) $(SRCS:%.c=%.o) $(LIBS)

.c.o: $(HDRS)
	$(CC) $(INCS) $(CFLAGS) $(CPPFLAGS) -c $<

clean:
	rm -f *.o $(PROG)

