#include "client_ib_rdma.h"


void pull_ib_rdma_data(IB_Context *ctx, ib_rdma_data_req *rdma_req, void* buf_addr, int64_t buf_off)
{
    assert(ctx != NULL);

    rdma_read(ctx, buf_addr, buf_off, rdma_req->rmt_mem_addr, rdma_req->rmt_mem_off, rdma_req->size);
}
 


