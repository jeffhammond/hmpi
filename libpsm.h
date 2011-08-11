#ifndef _LIBPSM_H_
#define _LIBPSM_H_
#include <stdint.h>
#include <stdlib.h>
#include <psm.h>
#include <psm_mq.h>

typedef struct libpsm_req_t
{
    psm_mq_req_t psm_req;
} libpsm_req_t;

int libpsm_init(void);

int libpsm_shutdown(void);

void libpsm_connect2(int rank);

int post_recv(int src_rank, void* buf, uint32_t len, uint32_t tag, uint16_t comm, libpsm_req_t* req);

//void wait_recv(libpsm_req_t* req);
//int test_recv(libpsm_req_t* req);

int post_send(int dst_rank, void* buf, uint32_t len, uint32_t tag, uint16_t comm, libpsm_req_t* req);

//void wait_send(libpsm_req_t* req);
//int test_send(libpsm_req_t* req);

void wait(libpsm_req_t* req);
int test(libpsm_req_t* req);

void poll(void);

#define wait_send wait
#define test_send test
#define wait_recv wait
#define test_recv test


int print_stats(void);

#endif

