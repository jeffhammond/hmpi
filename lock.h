#ifndef _LOCK_H_
#define _LOCK_H_

#define CACHE_LINE 128

typedef struct lock_t {
    volatile int lock;
    char padding[CACHE_LINE - sizeof(int)];
} lock_t;

static inline void LOCK_SET(lock_t* l) {
    while(__sync_lock_test_and_set(&l->lock, 1) == 1);
}

static inline void LOCK_CLEAR(lock_t* l) {
    __sync_lock_release(&l->lock);
}

static inline int LOCK_GET(lock_t* l) {
    return (int)l->lock;
}

#endif
