CC=mpicc -std=gnu99 

WARN=-Wall -Wuninitialized -Winline #-Wno-unused-function
CFLAGS+=$(WARN) -O0 -g

HMPI=..

LIBS=-lrt -lnuma

INCS=#-D_PROFILE=1 -D_PROFILE_MPI=1 -D_PROFILE_PAPI_EVENTS=1 #-DFULL_PROFILE
#INCS+=-DENABLE_OPI=1
#INCS+=-DHMPI_LOGCALLS=1 #-DHMPI_CHECKSUM=1
#INCS+=-D_PROFILE=1 -D_PROFILE_MPI=1 -DFULL_PROFILE #-D_PROFILE_PAPI_EVENTS=1


#all: INCS+=-DUSE_NUMA=1 
#all: SRCS+=sm_malloc.c
all: test_comm_dup test_get_count test_comm_split


test_comm_dup: test_comm_dup.c
	$(CC) test_comm_dup.c -o test_comm_dup $(CFLAGS) $(INCS) -I$(HMPI) -L$(HMPI) -lhmpi

test_comm_split: test_comm_split.c
	$(CC) test_comm_split.c -o test_comm_split $(CFLAGS) $(INCS) -I$(HMPI) -L$(HMPI) -lhmpi

test_get_count: test_get_count.c
	$(CC) test_get_count.c -o test_get_count $(CFLAGS) $(INCS) -I$(HMPI) -L$(HMPI) -lhmpi

clean:
	rm -f test_comm_dup test_get_count test_comm_split

