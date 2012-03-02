CC=mpicc -std=gnu99
#CC=mpixlc

HWLOC=/g/g19/friedley/local

#CFLAGS=-O0 -g -Wall -Wuninitialized -I$(HWLOC)/include -L$(HWLOC)/lib -L/usr/local/tools/papi/lib -I/usr/local/tools/papi/include
CFLAGS=-O3 -march=native -fomit-frame-pointer -Wall -Wuninitialized -I$(HWLOC)/include -L$(HWLOC)/lib
#CFLAGS=-O5 -qarch=450d
#CFLAGS=-O0 -g 
#CFLAGS=-march=k8 -O2 -fomit-frame-pointer
#CFLAGS=-march=native -O3 -fomit-frame-pointer -Iopenpa-1.0.2/src -L/usr/local/tools/papi/lib -I/usr/local/tools/papi/include

all: main

nbc_op.o: nbc_op.c
	$(CC) $(CFLAGS) -c nbc_op.c -o nbc_op.o 

hmpi.o: hmpi.c hmpi.h
	$(CC) $(CFLAGS) -c hmpi.c -o hmpi.o

hmpi.a: hmpi.o nbc_op.o
	ar r $@ hmpi.o nbc_op.o
	ranlib $@

main: hmpi.a main.c
#	$(CC) main.c $(CFLAGS) hmpi.a -o $@ 
	$(CC) main.c $(CFLAGS) -lhwloc hmpi.a -o $@ 

#$(CC) main.c $(CFLAGS) -lhwloc -lpapi hmpi.a -o $@ 
#$(CC) main.c $(CFLAGS) -lhwloc -lpmi -lpapi hmpi.a -o $@ 

hmpi.S: hmpi.c hmpi.h
	$(CC) hmpi.c -S -fverbose-asm -o $@

hmpi.tgz: hmpi*
	tar czf hmpi.tgz hmpi.c hmpi.h main.c Makefile nbc_op.c


clean:
	rm -f hmpi.a hmpi.o main nbc_op.o
