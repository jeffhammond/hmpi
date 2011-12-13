CXX=mpicxx -std=gnu99

HWLOC=/g/g19/friedley/local

CXXFLAGS=-O0 -g -I$(HWLOC)/include -L$(HWLOC)/lib -L/usr/local/tools/papi/lib -I/usr/local/tools/papi/include
#CXXFLAGS=-march=k8 -O2 -fomit-frame-pointer
#CXXFLAGS=-march=native -O3 -fomit-frame-pointer -Iopenpa-1.0.2/src -L/usr/local/tools/papi/lib -I/usr/local/tools/papi/include

all: main

nbc_op.o: nbc_op.c
	$(CXX) $(CXXFLAGS) -c nbc_op.c -o nbc_op.o 

hmpi.o: hmpi.c hmpi.h
	$(CXX) $(CXXFLAGS) -c hmpi.c -o hmpi.o

hmpi.a: hmpi.o nbc_op.o
	ar r $@ hmpi.o nbc_op.o
	ranlib $@

main: hmpi.a main.cpp
	$(CXX) main.cpp $(CXXFLAGS) -lhwloc -lpapi hmpi.a -o $@ 
	#$(CXX) main.cpp $(CXXFLAGS) -lhwloc -lpmi -lpapi hmpi.a -o $@ 

hmpi.S: hmpi.c hmpi.h
	$(CXX) hmpi.c -S -fverbose-asm -o $@

hmpi.tgz: hmpi*
	tar czf hmpi.tgz hmpi.c hmpi.h main.cpp Makefile nbc_op.c


clean:
	rm -f hmpi.a hmpi.o main nbc_op.o
