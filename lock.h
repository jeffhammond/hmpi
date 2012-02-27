#ifndef _LOCK_H_
#define _LOCK_H_

//#define CACHE_LINE 128

#if defined __IBMCPP__
#include <builtins.h>
#endif

#ifdef __x86_64__
#define STORE_FENCE() __asm__("sfence")
#define LOAD_FENCE() __asm__("lfence")

#elif defined (__IBMC__) || defined (__IBMCPP__)
#define STORE_FENCE() __lwsync()
#define LOAD_FENCE() __sync()

#else //Default GCC builtins
#define STORE_FENCE() __sync_synchronize()
#define LOAD_FENCE() __sync_synchronize()
#endif


#if defined __IBMC__ || defined __IBMCPP__

//TODO assumes 32bit pointers (true on BGP)
//if *ptr == oldval, then write *newval
#ifndef CAS_PTR_BOOL
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif
#ifndef FETCH_ADD

#define FETCH_ADD(ptr, val) fetch_add((int*)ptr, (int)val)

static inline unsigned int fetch_add(int* __restrict ptr, int val)
{
    int ret;

    do {
        ret = __lwarx((volatile int*)ptr);
        //int tmp = ret + val;
    } while(!__stwcx((volatile int*)ptr, ret + val));

    return ret;
#if 0
    unsigned int ret;
    register tmp = 0;

    //TODO - rewrite using lwarx/stwcx builtins
    asm volatile (
            "1:\n\t"
            "lwarx  %0,0,%1\n\t"
            "add    %3,%2,%0\n\t"
            "stwcx. %3,0,%1\n\t"
            "bne-   1b\n\t"
            : "=r" (ret) : "r" (ptr), "r" (val), "r" (tmp));

    return ret;
#endif
}
#endif //FETCH_ADD

#else //GCC/ICC and compatible

//if *ptr == oldval, then write *newval
#ifndef CAS_PTR_BOOL
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif

#ifndef FETCH_ADD
#define FETCH_ADD(ptr, val) \
    __sync_fetch_and_add(ptr, val)
#endif
#endif // else GCC/ICC


typedef struct lock_t {
    volatile int lock;
//    char padding[CACHE_LINE - sizeof(int)];
} lock_t;

static inline void LOCK_INIT(lock_t* __restrict l, int locked) {
//#ifdef __IBMC__
    l->lock = locked;
    STORE_FENCE();
//#else
//    __sync_lock_test_and_set(&l->lock, locked);
//#endif
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
