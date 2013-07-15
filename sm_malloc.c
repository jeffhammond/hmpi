#include "malloc.c"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "error.h"
//#include "hmpi.h"
//#include "profile2.h"


//#define USE_MMAP 1
#define USE_PSHM 1
//#define USE_SYSV 1


#if 0
PROFILE_DECLARE();
PROFILE_TIMER(malloc);
PROFILE_TIMER(calloc);
PROFILE_TIMER(free);
PROFILE_TIMER(realloc);
PROFILE_TIMER(memalign);
PROFILE_TIMER(mmap);

void sm_profile_show(void)
{
#if 0
    PROFILE_TIMER_SHOW(malloc);
    PROFILE_TIMER_SHOW(calloc);
    PROFILE_TIMER_SHOW(free);
    PROFILE_TIMER_SHOW(realloc);
    PROFILE_TIMER_SHOW(memalign);
    PROFILE_TIMER_SHOW(mmap);
#endif
}
#endif



#define unlikely(x)     __builtin_expect((x),0)


struct sm_region
{
    intptr_t limit; //End of shared memory region
    intptr_t brk;   //Next available shared memory address.

};

void* sm_lower = NULL;
void* sm_upper = NULL;


static struct sm_region* sm_region = NULL;
static mspace sm_mspace = NULL;


#define TEMP_SIZE (1024 * 1024 * 2L) //Temporary mspace capacity

//Keep this around for use with valgrind.
//static char sm_temp[TEMP_SIZE] = {0};


#ifdef USE_MMAP
//MMAP and SYSV have different space capabilities.

#define MSPACE_SIZE (1024L * 1024L * 896) //Use with file in /tmp
//#define MSPACE_SIZE (1024L * 1024L * 1536) //Use with file in /p/lscratchX
#define DEFAULT_SIZE (MSPACE_SIZE * 16L + (long)getpagesize()) //Default shared heap size

//On the LC machines, /tmp is a tmpfs, limiting us to half of the free memory.
static char* sm_filename = "/tmp/friedley/sm_file";
//static char sm_filename[256] = {0};


static void __sm_destroy(void)
{
    unlink(sm_filename);
}


static int __sm_init_region(void)
{
    int fd;
    int do_init = 1; //Whether to do initialization
    //size_t size;

    //Find a filename.
#if 0
    char* tmp = getenv("SM_FILE");
    if(tmp == NULL) {
        tmp = getenv("TMP");
        if(tmp == NULL) {
            printf("ERROR neither SM_FILE nor TMP are set\n");
            exit(-1);
        }

        snprintf(sm_filename, 255, "%s/sm_file", tmp);
    } else {
        strncpy(sm_filename, tmp, 255);
    }
#endif

    //printf("SM filename %s\n", sm_filename);
    //fflush(stdout);

    //Find the SM region size.
#if 0
    tmp = getenv("SM_SIZE");
    if(tmp == NULL) {
        size = DEFAULT_SIZE;
    } else {
        size = atol(tmp) * 1024L * 1024L;
    }
#endif

#if 0
    char host[128] = {0};
    if(gethostname(host, 127) != 0) {
        abort();
    }

    sprintf(sm_filename, "/p/lscratchd/friedley/sm.%s\n", host);
#endif

    //printf("SM size %lx\n", size);
    //fflush(stdout);

    //Open the SM region file.
    fd = open(sm_filename, O_RDWR|O_CREAT|O_EXCL|O_TRUNC, S_IRUSR|S_IWUSR); 
    if(fd == -1) {
        do_init = 0;

        if(errno == EEXIST) {
            //Another process has already created the file.
            fd = open(sm_filename, O_RDWR, S_IRUSR|S_IWUSR);
        } 
        
        if(fd == -1) {
            abort();
        }
    }

    if(ftruncate(fd, DEFAULT_SIZE) == -1) {
        abort();
    }

    //Map the SM region.
    sm_region = mmap(NULL, DEFAULT_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(sm_region == (void*)MAP_FAILED) {
        abort();
    }

    close(fd);

    return do_init;
}

#endif

#ifdef USE_PSHM

#ifdef __bg__
#define MSPACE_SIZE (1024L * 1024L * 512L * 1L)
#define DEFAULT_SIZE (MSPACE_SIZE * 16L + (long)getpagesize())
#else
#define MSPACE_SIZE (1024L * 1024L * 1024L * 16L)
#define DEFAULT_SIZE (MSPACE_SIZE * 16L + (long)getpagesize())
#endif

static char* sm_filename = "hmpismfile";


static void __sm_destroy(void)
{
    shm_unlink(sm_filename);
}


static int __sm_init_region(void)
{
    int do_init = 1; //Whether to do initialization

    //Open the SM region file.
    int fd = shm_open(sm_filename, O_RDWR|O_CREAT|O_EXCL|O_TRUNC, S_IRUSR|S_IWUSR); 
    if(fd == -1) {
        do_init = 0;

        if(errno == EEXIST) {
            //Another process has already created the file.
            fd = shm_open(sm_filename, O_RDWR, S_IRUSR|S_IWUSR);
        } 
        
        if(fd == -1) {
            //perror("shm_open");
            abort();
        }
    }



    if(ftruncate(fd, DEFAULT_SIZE) == -1) {
        abort();
    }

    //Map the SM region.
    sm_region = mmap(NULL, DEFAULT_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(sm_region == (void*)MAP_FAILED) {
        abort();
    }

    close(fd);

    return do_init;
}

#endif

#ifdef USE_SYSV
//MMAP and SYSV have different space capabilities.
//LC machines are configured for 12gb of sysv memory (768mb for 16 ranks)
#define MSPACE_SIZE (1024L * 1024L * 700L)
//#define DEFAULT_SIZE (MSPACE_SIZE * 16L + (long)getpagesize()) //Default shared heap size
#define DEFAULT_SIZE (1024L * 1024L * 12200L)


static int sm_shmid = -1;


static void __sm_destroy(void)
{
    shmctl(sm_shmid, IPC_RMID, NULL);
}


static int __sm_init_region(void)
{
    int do_init = 1; //Whether to do initialization

    //Use the PWD for an ftok file -- we don't have argv[0] here,
    // and "_" points to srun under slurm.
    char* pwd = getenv("PWD");
    if(pwd == NULL) {
        abort();
    }

    key_t key = ftok(pwd, 'S' << 1);


    sm_shmid = shmget(key, DEFAULT_SIZE, 0600 | IPC_CREAT | IPC_EXCL);
    if(sm_shmid == -1) {

        if(errno == EEXIST) {
            //SM region exists, try again -- we won't initialize.
            sm_shmid = shmget(key, DEFAULT_SIZE, 0600 | IPC_CREAT);
            do_init = 0;
        }


            printf("DEFAULT_SIZE %ld %d\n", DEFAULT_SIZE, errno);
            fflush(stdout);
        //Abort if both tries failed.
        if(sm_shmid == -1) {
            abort();
        }
    }


    sm_region = shmat(sm_shmid, NULL, 0);
    if(sm_region == (void*)-1) {
        abort();
    }

    return do_init;
}

#endif


static void __attribute__((noinline)) __sm_init(void)
{
    int do_init; //Whether to do initialization

    //Set up a temporary area on the stack for malloc() calls during our
    // initialization process.

    void* temp_space = alloca(TEMP_SIZE);
    sm_region = create_mspace_with_base(temp_space, TEMP_SIZE, 0);

    //Keep this for use with valgrind.
    //sm_region = create_mspace_with_base(sm_temp, TEMP_SIZE, 0);
    //sm_region->brk = (intptr_t)sm_region + sizeof(struct sm_region);

    sm_region->limit = (intptr_t)sm_region + TEMP_SIZE;


    //Find the SM region size.
    //SM_SIZE environment variable is size per proc in megabytes.
    //We assume 16 procs per node.
#if 0
    char* tmp = getenv("SM_SIZE");
    if(tmp == NULL) {
        size = DEFAULT_SIZE;
    } else {
        size = atol(tmp) * 1024L * 1024L * 16 + getpagesize();
    }
#endif


    //Set up the SM region using one of mmap/sysv/pshm
    do_init = __sm_init_region();



    //Only the process creating the file should initialize.
    if(do_init) {
        //Only the initializing process registers the shutdown handler.
        atexit(__sm_destroy);

        sm_region->limit = (intptr_t)sm_region + DEFAULT_SIZE;

        int pagesize = getpagesize();
        int offset = ((sizeof(struct sm_region) / pagesize) + 1) * pagesize;

#ifdef __bg__
        //Ensure everything above is set before brk below:
        // setting brk is the synchronization signal.
        __lwsync();
#endif

        sm_region->brk = (intptr_t)sm_region + offset;
        //printf("SM region %p default size 0x%lx mspace size 0x%lx limit 0x%lx brk 0x%lx\n",
        //        sm_region, DEFAULT_SIZE, MSPACE_SIZE, sm_region->limit, sm_region->brk);
        //fflush(stdout);
    } else {
        //Wait for another process to finish initialization.
        void* volatile * brk_ptr = (void**)&sm_region->brk;

        while(*brk_ptr == NULL);

        //Ensure none of the following loads occur during/before the above spin loop.
#ifdef __bg__
        __lwsync();
#endif

        //Check that this process' region is mapped to the same address as the
        //process that initialized the region.
        if(sm_region->limit != (intptr_t)sm_region + DEFAULT_SIZE) {
            abort();
        }
    }

    sm_lower = sm_region;
    sm_upper = (void*)sm_region->limit;

    //Create my own mspace.
    //void* base = sm_morecore(MSPACE_SIZE);
    void* base = (void*)__sync_fetch_and_add(&sm_region->brk, MSPACE_SIZE);
    //if(base == (void*)-1) {
    if(base < sm_lower || base >= sm_upper) {
        abort();
    }

    //Clearing the memory seems to avoid some bugs and
    // forces out subtle OOM issues here instead of later.
    //memset(base, 0, MSPACE_SIZE);

    //Careful to subtract off space for the local data.
    sm_mspace = create_mspace_with_base(base,
            MSPACE_SIZE, 1);

    //This should go last so it can use proper malloc and friends.
    //PROFILE_INIT();
}


void* sm_morecore(intptr_t increment)
{
    abort();
#if 0
    void* oldbrk = (void*)__sync_fetch_and_add(&sm_region->brk, increment);

/*    printf("%d sm_morecore incr %ld brk %p limit %p\n",
            getpid(), increment, oldbrk, sm_region->limit);
    fflush(stdout);*/

    if((uintptr_t)oldbrk + increment > (uintptr_t)sm_region->limit) {
        errno = ENOMEM;
        return (void*)-1;
    }

    //memset(oldbrk, 0, increment);
    return oldbrk;
#endif
}


#if 0
void* sm_mmap(void* addr, size_t len, int prot, int flags, int fildes, off_t off)
{
    //PROFILE_START(mmap);
    void* ptr = sm_morecore(len);
    //PROFILE_STOP(mmap);
    return ptr;
}


int sm_munmap(void* addr, size_t len)
{
    //For now, just move the break back if possible.

    //Clear this so MMAP_CLEARS works right -- free mem is always clear.
    memset(addr, 0, len);

    /*int success =*/ __sync_bool_compare_and_swap(&sm_region->brk,
            (intptr_t)addr + len, addr);

    //if(success) {
    //    printf("munmap returned break %lx\n", len);
    //} else {
    //    printf("munmap leaking mem %p len %lx (%p) brk 0x%lx\n",
    //            addr, len, (void*)((uintptr_t)addr + len), sm_region->brk);
    //}
    //fflush(stdout);

    return 0;
}
#endif


int is_sm_buf(void* mem) {
    //if(sm_region == NULL) __sm_init();

    return (intptr_t)mem >= (intptr_t)sm_region &&
        (intptr_t)mem < sm_region->limit;
}


void* malloc(size_t bytes) {
    if(unlikely(sm_region == NULL)) __sm_init();
    //PROFILE_START(malloc);

    void* ptr = mspace_malloc(sm_mspace, bytes);

    //PROFILE_STOP(malloc);
    return ptr;
}

void free(void* mem) {
    //if(unlikely(sm_region == NULL)) __sm_init();

    //PROFILE_START(free);

    if(mem < sm_lower || mem >= sm_upper) {
        return;
    }

    if(unlikely(sm_region == NULL)) return;

    mspace_free(sm_mspace, mem);
    //PROFILE_STOP(free);
}

void* realloc(void* mem, size_t newsize) {
    if(unlikely(sm_region == NULL)) __sm_init();

    //PROFILE_START(realloc);
    void* ptr = mspace_realloc(sm_mspace, mem, newsize);
    //PROFILE_STOP(realloc);

    return ptr;
}

void* calloc(size_t n_elements, size_t elem_size) {
    if(unlikely(sm_region == NULL)) __sm_init();

    //PROFILE_START(calloc);
    void* ptr = mspace_calloc(sm_mspace, n_elements, elem_size);
    //PROFILE_STOP(calloc);

    return ptr;
}

void* memalign(size_t alignment, size_t bytes) {
    if(unlikely(sm_region == NULL)) __sm_init();

    //PROFILE_START(memalign);
    void* ptr = mspace_memalign(sm_mspace, alignment, bytes);
    //PROFILE_STOP(memalign);

    return ptr;
}

