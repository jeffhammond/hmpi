#ifndef _LOCK_H_
#define _LOCK_H_
#include <stdio.h>

//AWF - I made my own primitives for several reasons:
// - Referencing the OPA atomic types requires a pointer dereference.  The
//   compiler probably eliminates them, but still results in less clean code.
// - OPA lacks a size_t (64bit) integer atomic type.

#if (defined __IBMC__ || defined __IBMCPP__) && defined __64BIT__ && defined __bg__
#warning "Using BlueGene/Q (64bit) primitives"

//fetch-add for send-recv offset support:
// Wrap the atomic alloc call for HMPI to call
// Provide L2-atomic load-increment (fetch and add) operation

#if defined __IBMCPP__
#include <builtins.h>
#endif

#define STORE_FENCE() __lwsync()
#define LOAD_FENCE() __sync()
#define FENCE() __lwsync(); __fence()

#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define CAS_PTR_VAL(ptr, oldval, newval) \
  (void*)__sync_val_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define CAS_T_BOOL(t, ptr, oldval, newval) \
  __sync_bool_compare_and_swap((t*)(ptr), (t)(oldval), (t)(newval))


#define FETCH_ADD32(ptr, val) \
    __sync_fetch_and_add(ptr, val)

#define FETCH_ADD64(ptr, val) \
    __sync_fetch_and_add(ptr, val)


//The __compare_and_swap function is useful when a single word value must be
//updated only if it has not been changed since it was last read. If you use
//__compare_and_swap as a locking primitive, insert a call to the __isync
//built-in function at the start of any critical sections.

#if 0
static inline int CAS_PTR_BOOL(void** restrict ptr, void* oldval, void* newval)
{
    return __compare_and_swaplp((volatile long*)ptr, (long*)&oldval, (long)newval);
}

static inline int FETCH_ADD32(int* ptr, int val)
{
    int ret;

    do {
        ret = __lwarx(ptr);
    } while(!__stwcx(ptr, ret + val));

    return ret;
}

static inline int FETCH_ADD64(long int* ptr, long int val)
{
    int ret;

    do {
        ret = __ldarx(ptr);
    } while(!__stdcx(ptr, ret + val));

    return ret;
}
#endif

static inline void* FETCH_STORE(void** ptr, void* val)
{
    return (void*)__fetch_and_swaplp((volatile long*)ptr, (long)val);
}


// Lock routines
#include <spi/include/kernel/memory.h>
#include <spi/include/l2/lock.h>

typedef L2_Lock_t lock_t;

static void LOCK_INIT(lock_t* __restrict l) __attribute__((unused));

static void LOCK_INIT(lock_t* __restrict l) {
    if(Kernel_L2AtomicsAllocate(l, sizeof(lock_t))) {
        printf("ERROR Unable to allocate L2 atomic memory\n");
        fflush(stdout);
        assert(0);
    }

    L2_LockInit(l);
}

static inline void LOCK_ACQUIRE(lock_t* __restrict l) {
    L2_LockAcquire(l);
}

//Returns non-zero if lock was acquired, 0 if not.
static inline long int LOCK_TRY(lock_t* __restrict l) {
    return L2_LockTryAcquire(l);
}

static inline void LOCK_RELEASE(lock_t* __restrict l) {
    L2_LockRelease(l);
}

static inline long int LOCK_GET(lock_t* __restrict l) {
    assert(0); //I want to see if/when this is used; below may be wrong.
    return !L2_LockIsBusy(l);
}


//This code works on BGQ, but MCS offers no performance advantage due to Q's
// cache coherency design.
#if 0
typedef struct mcs_qnode_t {
    volatile struct mcs_qnode_t* next;
    volatile int locked;
} mcs_qnode_t;

typedef mcs_qnode_t* mcs_lock_t;


static inline void MCS_LOCK_INIT(mcs_lock_t* __restrict l) {
    *l = NULL;
    STORE_FENCE();
        __lwsync();
        __fence();
}
    

static inline void MCS_LOCK_ACQUIRE(mcs_lock_t* __restrict l, mcs_qnode_t* q) {
    q->next = NULL;

    mcs_qnode_t* pred;

    //Replace node at head of lock with our own, saving the previous node.
    pred = (mcs_qnode_t*)FETCH_STORE((void**)l, q);

    //Was there actually a previous node?  If not, we got the lock.
    if(pred != NULL) {
        //Otherwise we need to wait -- set our node locked, and wait for
        //the thread that owns the lock to release it.
        volatile int* locked = &q->locked;

        *locked = 1;

        //STORE_FENCE();  //Prevent q->locked from being set afer pred->next
        //TODO - are both needed?
        __lwsync();
        __fence();

        pred->next = q;

        while(*locked == 1);
    }

    __fence(); //Ensures compiler doesn't move code before lock release.
}


static inline void MCS_LOCK_RELEASE(mcs_lock_t* __restrict l, mcs_qnode_t* q) {
    volatile mcs_qnode_t* next = q->next;

    __fence(); //Ensures compiler doesn't move code after lock release.

    //Is another thread waiting on the lock?
    if(next == NULL) {
        //Doesn't look like it -- CAS NULL into the lock
        if(CAS_PTR_BOOL((void**)l, (void*)q, NULL)) {
            return;
        }

        //A thread jumped in between the branch and the CAS --
        //wait for them to set our next pointer.

        //AWF - the way this type is declared is CRITICAL for correctness!!!
        mcs_qnode_t* volatile * vol_next = (mcs_qnode_t* volatile *)&q->next;

        while((next = *vol_next) == NULL);
    }

    //TODO - are both needed?
    __lwsync();
    __fence();
    next->locked = 0;
}
#endif


#elif (defined __IBMC__ || defined __IBMCPP__) && defined __32BIT__
#warning "Using BlueGene/P (32bit) primitives"

typedef struct lock_t {
#if defined __powerpc64__
    // POWER architectures (works on Q)
    volatile int32_t lock;
#else
    // Default GCC/ICC (x86)
    volatile int lock;
#endif
} lock_t;


#if defined __IBMCPP__
#include <builtins.h>
#endif

#define STORE_FENCE() __lwsync()
#define LOAD_FENCE() __sync()

//if *ptr == oldval, then write *newval
#if 0
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __compare_and_swap((volatile intptr_t*)(ptr), \
          (intptr_t)(oldval), (intptr_t)(newval))
#endif

static inline int CAS_PTR_BOOL(volatile void** ptr, void* oldval, void* newval)
{
    return __compare_and_swap((volatile int*)ptr, (int*)&oldval, (int)newval);
}


static inline int FETCH_ADD32(volatile int* __restrict ptr, int val)
{
    int ret;

    do {
        ret = __lwarx(ptr);
    } while(!__stwcx(ptr, ret + val));

    return ret;
}

static inline int FETCH_ADD64(volatile long int* __restrict ptr, long int val)
{
    int ret;

    do {
        ret = __ldarx(ptr);
    } while(!__stdcx(ptr, ret + val));

    return ret;
}


// Lock routines

static void LOCK_INIT(lock_t* __restrict l) __attribute__((unused));

static inline void LOCK_INIT(lock_t* __restrict l) {
    l->lock = 0;
    STORE_FENCE();
}

static inline void LOCK_ACQUIRE(lock_t* __restrict l) {
    while(__check_lock_mp((int*)(&l->lock), 0, 1));
}

//Returns non-zero if lock was acquired, 0 if not.
static inline int LOCK_TRY(lock_t* __restrict l) {
    return __check_lock_mp((int*)(&l->lock), 0, 1) == 0;
}

static inline void LOCK_RELEASE(lock_t* __restrict l) {
    __clear_lock_mp((int*)(&l->lock), 0);
}

static inline int LOCK_GET(lock_t* __restrict l) {
    return (int)l->lock;
}



//This is generic locking support using GCC/ICC atomic builtins.
#elif defined __GNUC__ //Should cover ICC too

#if 0
typedef struct lock_t {
#if defined __powerpc64__
    // POWER architectures (works on Q)
    volatile int32_t lock;
#else
    // Default GCC/ICC (x86)
    volatile int lock;
#endif
} lock_t;
#endif


#ifdef __x86_64__ //Better x86 versions
#define STORE_FENCE() __asm__ volatile ("sfence")
#define LOAD_FENCE() __asm__ volatile ("lfence")
#else //Default GCC builtins
#define STORE_FENCE() __sync_synchronize()
#define LOAD_FENCE() __sync_synchronize()
#endif

//if *ptr == oldval, then write *newval
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define CAS_PTR_VAL(ptr, oldval, newval) \
  (void*)__sync_val_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define CAS_T_BOOL(t, ptr, oldval, newval) \
  __sync_bool_compare_and_swap((t*)(ptr), (t)(oldval), (t)(newval))


#define FETCH_ADD32(ptr, val) \
    __sync_fetch_and_add(ptr, val)

#define FETCH_ADD64(ptr, val) \
    __sync_fetch_and_add(ptr, val)


static inline void* FETCH_STORE(void** ptr, void* val)
{
    void* out;

#ifdef __x86_64__
    asm volatile ("lock xchg %0, (%1)" : "=r" (out) : "r" (ptr), "0" (val));
#else
#warning "FETCH_STORE not implemented for this architecture"
    //This builtin, as described by Intel, is not a traditional test-and-set
    // operation, but rather an atomic exchange operation. It writes value into
    // *ptr, and returns the previous contents of *ptr.
    //STORE_FENCE();
    out = __sync_lock_test_and_set(ptr, val);
#endif

    return out;
}



#ifdef __x86_64__ //Better x86 versions

//Use MCS locks by default.
// Algorithms for Scalable Synchronization on Shared-Memory Multiprocessors
// by John Mellor-Crummey and Michael Scott

typedef struct mcs_qnode_t {
    struct mcs_qnode_t* next;
    int locked;
} mcs_qnode_t;

typedef mcs_qnode_t* lock_t;


static void LOCK_INIT(lock_t* __restrict l) __attribute__((unused));

static void LOCK_INIT(lock_t* __restrict l) {
    *l = NULL;
}

#define LOCK_ACQUIRE(l) \
    mcs_qnode_t q; \
    __LOCK_ACQUIRE(l, &q)

#define LOCK_RELEASE(l) __LOCK_RELEASE(l, &q)

static inline void __LOCK_ACQUIRE(lock_t* __restrict l, mcs_qnode_t* q) {
    q->next = NULL;

    mcs_qnode_t* pred;

    //Replace node at head of lock with our own, saving the previous node.
    pred = (mcs_qnode_t*)FETCH_STORE((void**)l, q);

    //Was there actually a previous node?  If not, we got the lock.
    if(pred != NULL) {
        //Otherwise we need to wait -- set our node locked, and wait for
        //the thread that owns the lock to release it.
        volatile int* locked = &q->locked;

        *locked = 1;

        STORE_FENCE();  //Prevent q->locked from being set afer pred->next

        pred->next = q;
        while(*locked == 1);
    }
}

static inline void __LOCK_RELEASE(lock_t* __restrict l, mcs_qnode_t* q) {
    mcs_qnode_t* next = q->next;

    //Is another thread waiting on the lock?
    if(next == NULL) {
        //Doesn't look like it -- CAS NULL into the lock
        if(CAS_PTR_BOOL(l, q, NULL)) {
            return;
        }

        //A thread jumped in between the branch and the CAS --
        //wait for them to set our next pointer.

        //AWF - the way this type is declared is CRITICAL for correctness!!!
        mcs_qnode_t* volatile * vol_next = (mcs_qnode_t* volatile *)&q->next;

        while((next = *vol_next) == NULL);
    }

    next->locked = 0;
}

#endif

#else
#error "Unrecognized platform; no atomics defined"
#endif


#endif
