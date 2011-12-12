#include <stdint.h>
#include <stdlib.h>

#ifndef STORE_FENCE
#define STORE_FENCE() __asm__("sfence")
#endif


#define BLOCK_SIZE (32 * 1024)
#define NUM_BLOCKS 128

//Each thread will alloc a struct
typedef struct buffer_t
{
    char data[BLOCK_SIZE * NUM_BLOCKS];

    volatile uint64_t head;
    volatile uint64_t tail;
} buffer_t;


//buffer_t* g_buffers[NUM_THREADS];

static inline void send_msg(char* data, size_t len, buffer_t* buf)
{
    int head = buf->head;

    while(len > 0) {
        // Wait until head - tail < NUM_BLOCKS
        while(head - buf->tail >= NUM_BLOCKS);

        // fill block[head]
        int copy_len = (len < BLOCK_SIZE ? len : BLOCK_SIZE);

        memcpy(&buf->data[(head % NUM_BLOCKS) * BLOCK_SIZE], data, copy_len);

        // atomically increment head
        //TODO - maybe use sfence and non-atomic
        head = __sync_fetch_and_add(&buf->head, 1) + 1;

        len -= copy_len;
        data += copy_len;
    }
}


static inline void recv_msg(char* data, size_t len, buffer_t* buf)
{
    //To receive
    int tail = buf->tail;

    while(len > 0) {
        // wait until tail != head
        while(tail == buf->head);

        // read block[tail]
        int copy_len = (len < BLOCK_SIZE ? len : BLOCK_SIZE);

        //printf("tail %d %d head %d copy_len %d len %d\n", tail % NUM_BLOCKS, tail, buf->head, copy_len, len);
        //printf("data %p bufdata %p len %d\n", data, &buf->data[(tail % NUM_BLOCKS) * BLOCK_SIZE], copy_len);
        memcpy(data, &buf->data[(tail % NUM_BLOCKS) * BLOCK_SIZE], copy_len);

        // increment tail
        tail = __sync_fetch_and_add(&buf->tail, 1) + 1;

        len -= copy_len;
        data += copy_len;
    }
}

