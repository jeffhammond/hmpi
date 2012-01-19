#ifndef _LOCK_H_
#define _LOCK_H_

//#define CACHE_LINE 128

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
