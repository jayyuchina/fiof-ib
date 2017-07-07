#ifndef JAY_IB_UTIL_H
#define JAY_IB_UTIL_H

#include "jay_global.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <netdb.h>
#include <infiniband/verbs.h>



#define RDMA_WRID 3

// if x is NON-ZERO, error is printed
#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)

// if x is ZERO, error is printed
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)

// if x is NEGATIVE, error is printed
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)


typedef struct
{
    int             	lid;
    int            	 	qpn;
    int             	psn;
    unsigned 			rkey;
    //int64_t			 	vaddr;	// maybe there is no need to transfer addr.
} IB_Connection;


typedef struct
{
    struct ibv_context 		*context;
    struct ibv_pd      		*pd;
    struct ibv_mr      		*mr;
    struct ibv_cq      		*rcq;
    struct ibv_cq      		*scq;
    struct ibv_qp      		*qp;
    struct ibv_comp_channel *ch;
    int                 	tx_depth;
    struct ibv_sge      	sge_list;
    struct ibv_send_wr  	wr;

    struct ibv_device		*ib_dev;

    IB_Connection 			loc_ib_conn;
    IB_Connection 			rmt_ib_conn;
	pthread_mutex_t 		ib_lock;
}IB_Context;


typedef struct
{
	int 	pair_srv_id;
	void*	rmt_mem_addr;
	void*	rmt_mem_off;
	int64_t size;
} ib_rdma_data_req;


#endif

