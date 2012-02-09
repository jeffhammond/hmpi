#ifndef _LOCK_H_
#define _LOCK_H_

//#define CACHE_LINE 128

//if *ptr == oldval, then write *newval
#ifndef CAS_PTR
#define CAS_PTR(ptr, oldval, newval) \
  __sync_val_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif

#ifndef CAS_PTR_BOOL
#define CAS_PTR_BOOL(ptr, oldval, newval) \
  __sync_bool_compare_and_swap((uintptr_t*)(ptr), \
          (uintptr_t)(oldval), (uintptr_t)(newval))
#endif

#ifndef FETCH_ADD
#define FETCH_ADD(ptr, val) \
    __sync_fetch_and_add(ptr, val)
#endif


typedef struct lock_t {
    volatile int lock;
//    char padding[CACHE_LINE - sizeof(int)];
} lock_t;

static inline void LOCK_INIT(lock_t* __restrict l, int locked) {
    __sync_lock_test_and_set(&l->lock, locked);
}

static inline void LOCK_SET(lock_t* __restrict l) {
    while(__sync_lock_test_and_set(&l->lock, 1) == 1);
}

//Returns non-zero if lock was acquired, 0 if not.
static inline int LOCK_TRY(lock_t* __restrict l) {
    return __sync_lock_test_and_set(&l->lock, 1) == 0;
}

static inline void LOCK_CLEAR(lock_t* __restrict l) {
    __sync_lock_release(&l->lock);
}

static inline int LOCK_GET(lock_t* __restrict l) {
    return (int)l->lock;
}

#endif
