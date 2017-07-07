#include "server_ib_rdma.h"

typedef struct
{
    void*	addr;
    int		in_use_num;
    int*	in_use_flag;
    pthread_mutex_t lock;
} IB_Rdma_Buf;

typedef struct
{
    IB_Context *	ib_ctx;
    IB_Rdma_Buf		ib_buf;		// has num_buf_per_qp segments
} Client_Rdma;

static Client_Rdma *	CLEINT_RDMA;
static int				NUM_CLIENT_RDMA;
static int				SIZE_PER_BUF;
static int				NUM_BUF_PER_QP;

void server_ib_rdma_init(int total_server_num, int num_buf_per_qp)
{
    NUM_CLIENT_RDMA = total_server_num;
    NUM_BUF_PER_QP = num_buf_per_qp;
    SIZE_PER_BUF = G_BLOCK_SIZE_IN_BYTES;

    if ((CLEINT_RDMA = calloc(NUM_CLIENT_RDMA, sizeof(Client_Rdma))) == NULL)
    {
        fprintf(stderr, "Failed to allocate client_rdma structures\n");
        exit(1);
    }

    int page_size = sysconf(_SC_PAGESIZE);

    int i;
    for(i=0; i<NUM_CLIENT_RDMA; i++)
    {
        if(posix_memalign((void**)&(CLEINT_RDMA[i].ib_buf.addr), page_size, num_buf_per_qp*SIZE_PER_BUF))
        {
            fprintf(stderr, "Failed to allocate client_rdma memory\n");
            exit(1);
        }
        CLEINT_RDMA[i].ib_buf.in_use_flag = calloc(NUM_BUF_PER_QP, sizeof(int));
        CLEINT_RDMA[i].ib_buf.in_use_num = 0;
        if (pthread_mutex_init(&(CLEINT_RDMA[i].ib_buf.lock),NULL)<0)
        {
            fprintf(stderr, "Initialize CLEINT_RDMA[i].ib_buf.lock failure\n");
            exit(1);
        }
    }
}

// return the loc_ib_conn to client for shake hands
IB_Connection * init_ib_with_client(int srv_id, IB_Connection* rmt_ib_conn)
{
    assert(CLEINT_RDMA[srv_id].ib_ctx == NULL);

    CLEINT_RDMA[srv_id].ib_ctx = create_ib_qp(CLEINT_RDMA[srv_id].buf, SIZE_PER_BUF*NUM_BUF_PER_QP);
    memcpy(&(CLEINT_RDMA[srv_id].ib_ctx->rmt_ib_conn), rmt_ib_conn, sizeof(IB_Connection));

    // TODO: shake hands before client get my ib_conn, is it buggy?
    ib_change_qp_state_rts(CLEINT_RDMA[srv_id].ib_ctx->qp, rmt_ib_conn);

    return &(CLEINT_RDMA[srv_id].ib_ctx->loc_ib_conn);
}

void* request_ib_rdma_buf(int srv_id)
{
	void *ret = NULL;
	IB_Rdma_Buf *ib_buf = &CLEINT_RDMA[srv_id].ib_buf;
    pthread_mutex_lock(&(ib_buf->lock));
	if(ib_buf->in_use_num < NUM_BUF_PER_QP)
	{
		int i;
		for(i=0; i<NUM_BUF_PER_QP; i++)
		{
			// find a free buf
			if(ib_buf->in_use_flag[i] == 0)
			{
				ib_buf->in_use_flag[i] = 1;
				ib_buf->in_use_num++;
				ret = ib_buf->addr + SIZE_PER_BUF * i;
				break;
			}
		}
	}
	else
	{
		ret = NULL;
	}
    pthread_mutex_unlock(&(ib_buf->lock));

	return ret;
}

void free_ib_rdma_buf(int srv_id, void* buf_addr)
{
	assert(buf_addr != NULL);
	
	IB_Rdma_Buf *ib_buf = &CLEINT_RDMA[srv_id].ib_buf;

	pthread_mutex_lock(&(ib_buf->lock));
	assert(ib_buf->in_use_num != 0);
	int buf_id = (buf_addr - ib_buf->addr) / SIZE_PER_BUF;
	assert(ib_buf->in_use_flag[buf_id] == 1);
	ib_buf->in_use_flag[buf_id] = 0;
	ib_buf->in_use_num --;
    pthread_mutex_unlock(&(ib_buf->lock));
}

void pull_ib_rdma_data(ib_rdma_data_req *rdma_req, void* buf_addr, int64_t buf_off)
{
    IB_Context *ctx = CLEINT_RDMA[rdma_req->pair_srv_id].ib_ctx;
    assert(ctx != NULL);

    rdma_read(ctx, buf_addr, buf_off, rdma_req->rmt_mem_addr, rdma_req->rmt_mem_off, rdma_req->size);
}



