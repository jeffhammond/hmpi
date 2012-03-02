#ifndef _LOCK_H_
#define _LOCK_H_

//#define CACHE_LINE 128

//AWF - I made my own primitives for several reasons:
// - Referencing the OPA atomic types requires a pointer dereference.  The
//   compiler probably eliminates them, but still results in less clean code.
// - OPA lacks a size_t (64bit) integer atomic type.

//TODO - make sure this is always aligned
typedef struct lock_t {
    volatile int lock;
//    char padding[CACHE_LINE - sizeof(int)];
} lock_t;

#if (defined __IBMC__ || defined __IBMCPP__) && defined __PPC__

#if defined __IBMCPP__
#include <builtins.h>
#endif

#define STORE_FENCE() __lwsync()
#define LOAD_FENCE() __sync()

//if *ptr == oldval, then write *newval
//#define CAS_PTR_BOOL(ptr, oldval, newval) \
//  __compare_and_swap((volatile intptr_t*)(ptr), \
//          (intptr_t)(oldval), (intptr_t)(newval))

static inline int CAS_PTR_BOOL(volatile void** ptr, void* oldval, void* newval)
{
    return __compare_and_swap((volatile int*)ptr, (int*)&oldval, (int)newval);
}


static inline int FETCH_ADD(volatile int* __restrict ptr, int val)
{
    int ret;

    do {
        ret = __lwarx(ptr);
    } while(!__stwcx(ptr, ret + val));

    return ret;
}


// Lock routines

static inline void LOCK_INIT(lock_t* __restrict l, int locked) {
    l->lock = locked;
    STORE_FENCE();
}

static inline void LOCK_SET(lock_t* __restrict l) {
    while(__check_lock_mp((int*)(&l->lock), 0, 1));
}

//Returns non-zero if lock was acquired, 0 if not.
static inline int LOCK_TRY(lock_t* __restrict l) {
    return __check_lock_mp((int*)(&l->lock), 0, 1) == 0;
}

static inline void LOCK_CLEAR(lock_t* __restrict l) {
    __clear_lock_mp((int*)(&l->lock), 0);
}

static inline int LOCK_GET(lock_t* __restrict l) {
    return (int)l->lock;
}


#elif defined __GNUC__ //Should cover ICC too

#ifdef __x86_64__ //Better x86 versions
#define STORE_FENCE() __asm__("sfence")
#define LOAD_FENCE() __asm__("lfence")
#else //Default GCC builtins
#define STORE_FENCE() __sync_synchronize()
#define LOAD_FENCE() __sync_synchronize()
#endif

//if *ptr == oldval, then write *newval
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))

#define FETCH_ADD(ptr, val) \
    __sync_fetch_and_add(ptr, val)


// Lock routines

static inline void LOCK_INIT(lock_t* __restrict l, int locked) {
    l->lock = locked;
    STORE_FENCE();
}

static inline void LOCK_SET(lock_t* __restrict l) {
    while(__sync_lock_test_and_set(&l->lock, 1) == 1);
}

//Returns non-zero if lock was acquired, 0 if not.
static inline int LOCK_TRY(lock_t* __restrict l) {
    if(l->lock == 0) {
        return __sync_lock_test_and_set(&l->lock, 1) == 0;
    } else {
        return 0;
    }
}

static inline void LOCK_CLEAR(lock_t* __restrict l) {
    __sync_lock_release(&l->lock);
}

static inline int LOCK_GET(lock_t* __restrict l) {
    return (int)l->lock;
}

#else
#error "Unrecognized platform; no atomics defined"
#endif



#if 0
static inline void LOCK_INIT(lock_t* __restrict l, int locked) {
    l->lock = locked;
    STORE_FENCE();
}

static inline void LOCK_SET(lock_t* __restrict l) {
#if defined __IBMC__ || defined __IBMCPP__
    while(__check_lock_mp((int*)(&l->lock), 0, 1));
#else
    while(__sync_lock_test_and_set(&l->lock, 1) == 1);
#endif
}

//Returns non-zero if lock was acquired, 0 if not.
static inline int LOCK_TRY(lock_t* __restrict l) {
#if defined __IBMC__ || defined __IBMCPP__
    return __check_lock_mp((int*)(&l->lock), 0, 1) == 0;
#else
    return __sync_lock_test_and_set(&l->lock, 1) == 0;
#endif
}

static inline void LOCK_CLEAR(lock_t* __restrict l) {
#if defined __IBMC__ || defined __IBMCPP__
    __clear_lock_mp((int*)(&l->lock), 0);
#else
    __sync_lock_release(&l->lock);
#endif
}

static inline int LOCK_GET(lock_t* __restrict l) {
    return (int)l->lock;
}
#endif

#endif
