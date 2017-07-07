#include "ib_util.h"

static int die(const char *reason)
{
    fprintf(stderr, "Err: %s - %s\n ", strerror(errno), reason);
    exit(EXIT_FAILURE);
    return -1;
}

int ib_change_qp_state_rtr(struct ibv_qp *qp, IB_Connection *rmt_ib_conn)
{

    struct ibv_qp_attr *attr;

    attr =  malloc(sizeof *attr);
    memset(attr, 0, sizeof *attr);

    attr->qp_state              = IBV_QPS_RTR;
    attr->path_mtu              = IBV_MTU_2048;
    attr->dest_qp_num           = rmt_ib_conn->qpn;
    attr->rq_psn                = rmt_ib_conn->psn;
    attr->max_dest_rd_atomic    = 1;
    attr->min_rnr_timer         = 12;
    attr->ah_attr.is_global     = 0;
    attr->ah_attr.dlid          = rmt_ib_conn->lid;
    attr->ah_attr.sl            = 1;
    attr->ah_attr.src_path_bits = 0;
    attr->ah_attr.port_num      = 1;	// we only have 1 port in the IB card

    TEST_NZ(ibv_modify_qp(qp, attr,
                          IBV_QP_STATE                |
                          IBV_QP_AV                   |
                          IBV_QP_PATH_MTU             |
                          IBV_QP_DEST_QPN             |
                          IBV_QP_RQ_PSN               |
                          IBV_QP_MAX_DEST_RD_ATOMIC   |
                          IBV_QP_MIN_RNR_TIMER),
            "Could not modify QP to RTR state");

    free(attr);

    return 0;
}



/*
 *  qp_change_state_rts
 * **********************
 *  Changes Queue Pair status to RTS (Ready to send)
 *	QP status has to be RTR before changing it to RTS
 */
int ib_change_qp_state_rts(struct ibv_qp *qp, IB_Connection *rmt_ib_conn)
{

    // first the qp state has to be changed to rtr
    ib_change_qp_state_rtr(qp, rmt_ib_conn);

    struct ibv_qp_attr *attr;

    attr =  malloc(sizeof *attr);
    memset(attr, 0, sizeof *attr);

    attr->qp_state              = IBV_QPS_RTS;
    attr->timeout               = 14;
    attr->retry_cnt             = 7;
    attr->rnr_retry             = 7;    /* infinite retry */
    attr->sq_psn                = rmt_ib_conn->psn;
    attr->max_rd_atomic         = 1;

    TEST_NZ(ibv_modify_qp(qp, attr,
                          IBV_QP_STATE            |
                          IBV_QP_TIMEOUT          |
                          IBV_QP_RETRY_CNT        |
                          IBV_QP_RNR_RETRY        |
                          IBV_QP_SQ_PSN           |
                          IBV_QP_MAX_QP_RD_ATOMIC),
            "Could not modify QP to RTS state");

    free(attr);

    return 0;
}


IB_Context * create_ib_qp(void * mem_region, uint32_t mem_size)
{
	fprintf(stderr, "################# create_ib_qp START\n");

	
    IB_Context *ctx = malloc(sizeof(IB_Context));
    //struct ibv_device *ib_dev;

    memset(ctx, 0, sizeof *ctx);

    //ctx->size = conf->size;
    ctx->tx_depth = 100;

    //TEST_NZ(posix_memalign(&ctx->buf, page_size, ctx->size * 2),
    //        "could not allocate working buffer ctx->buf");

    //memset(ctx->buf, 0, ctx->size * 2);

    struct ibv_device **dev_list;

    TEST_Z(dev_list = ibv_get_device_list(NULL),
           "No IB-device available. get_device_list returned NULL");

    TEST_Z(ctx->ib_dev = dev_list[0],
           "IB-device could not be assigned. Maybe dev_list array is empty");

    TEST_Z(ctx->context = ibv_open_device(ctx->ib_dev),
           "Could not create context, ibv_open_device");

    TEST_Z(ctx->pd = ibv_alloc_pd(ctx->context),
           "Could not allocate protection domain, ibv_alloc_pd");

    /* We dont really want IBV_ACCESS_LOCAL_WRITE, but IB spec says:
     * The Consumer is not allowed to assign Remote Write or Remote Atomic to
     * a Memory Region that has not been assigned Local Write.
     */

    TEST_Z(ctx->mr = ibv_reg_mr(ctx->pd, mem_region, mem_size,
                                 IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
           "Could not allocate mr, ibv_reg_mr. Do you have root access?");

    TEST_Z(ctx->ch = ibv_create_comp_channel(ctx->context),
           "Could not create completion channel, ibv_create_comp_channel");

    TEST_Z(ctx->rcq = ibv_create_cq(ctx->context, 1, NULL, NULL, 0),
           "Could not create receive completion queue, ibv_create_cq");

    TEST_Z(ctx->scq = ibv_create_cq(ctx->context,ctx->tx_depth, ctx, ctx->ch, 0),
           "Could not create send completion queue, ibv_create_cq");

    struct ibv_qp_init_attr qp_init_attr =
    {
        .send_cq = ctx->scq,
        .recv_cq = ctx->rcq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = ctx->tx_depth,
            .max_recv_wr = 1,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0
        }
    };

    TEST_Z(ctx->qp = ibv_create_qp(ctx->pd, &qp_init_attr),
           "Could not create queue pair, ibv_create_qp");
	

    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof( struct ibv_qp_attr));

    attr.qp_state        	= IBV_QPS_INIT;
    attr.pkey_index      	= 0;
    attr.port_num        	= 1;	// we only have 1 port on the IB card
    attr.qp_access_flags	=  IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

    TEST_NZ(ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE        |
                          IBV_QP_PKEY_INDEX   |
                          IBV_QP_PORT         |
                          IBV_QP_ACCESS_FLAGS),
            "Could not modify QP to INIT, ibv_modify_qp");

	

    // init ib info
    struct ibv_port_attr tmp_attr;
    TEST_NZ(ibv_query_port(ctx->context,1,&tmp_attr),
            "Could not get port attributes, ibv_query_port");

	srand48(time(NULL));

    ctx->loc_ib_conn.lid = tmp_attr.lid;
    ctx->loc_ib_conn.qpn = ctx->qp->qp_num;
    ctx->loc_ib_conn.psn = lrand48() & 0xffffff;
    ctx->loc_ib_conn.rkey = ctx->mr->rkey;
    //ctx->loc_ib_conn.vaddr = (uintptr_t)mem_region;	// local memory addr

	if (pthread_mutex_init(&(ctx->ib_lock),NULL)<0)
	{
		fprintf(stderr, "Initialize ib_lock failure\n");
		exit(1);
	}


	fprintf(stderr, "################# loc mem: %#016Lx\n", mem_region);

	fprintf(stderr, "################# create_ib_qp COMPLETE\n");

	return ctx;
}

void destory_ib_qp(IB_Context * ctx)
{ 

    TEST_NZ(ibv_destroy_qp(ctx->qp),
            "Could not destroy queue pair, ibv_destroy_qp");

    TEST_NZ(ibv_destroy_cq(ctx->scq),
            "Could not destroy send completion queue, ibv_destroy_cq");

    TEST_NZ(ibv_destroy_cq(ctx->rcq),
            "Coud not destroy receive completion queue, ibv_destroy_cq");

    TEST_NZ(ibv_destroy_comp_channel(ctx->ch),
            "Could not destory completion channel, ibv_destroy_comp_channel");

    TEST_NZ(ibv_dereg_mr(ctx->mr),
            "Could not de-register memory region, ibv_dereg_mr");

    TEST_NZ(ibv_dealloc_pd(ctx->pd),
            "Could not deallocate protection domain, ibv_dealloc_pd");

    return;
}

void re_register_rdma_buf(IB_Context *ctx, void * mem_region, int64_t mem_size)
{
	TEST_Z(ctx->mr = ibv_reg_mr(ctx->pd, mem_region, mem_size,
                                 IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
           "Could not allocate mr, ibv_reg_mr. Do you have root access?");
}

int64_t rdma_read(IB_Context* ctx, void* loc_mem, int64_t loc_off,
						void* rmt_mem, int64_t rmt_off, int64_t size)
{
	
	//fprintf(stderr, "################# rb_block_receive_ib START, rmt_ip: %ld rmt_addr: %#016Lx\n", rmt_ip, rmt_mem_addr);

    pthread_mutex_lock(&(ctx->ib_lock));

	struct ib_connection *rmt_ib_conn = &ctx->rmt_ib_conn;

    ctx->sge_list.addr      = loc_mem + loc_off;
    ctx->sge_list.length    = size;
    ctx->sge_list.lkey      = ctx->mr->lkey;

    ctx->wr.wr.rdma.remote_addr = rmt_mem + rmt_off;
    ctx->wr.wr.rdma.rkey        = rmt_ib_conn->rkey;
    ctx->wr.wr_id       = RDMA_WRID;
    ctx->wr.sg_list     = &ctx->sge_list;
    ctx->wr.num_sge     = 1;
    ctx->wr.opcode      = IBV_WR_RDMA_READ;
    ctx->wr.send_flags  = IBV_SEND_SIGNALED;
    ctx->wr.next        = NULL;

    struct ibv_send_wr *bad_wr;

	
	
    //fprintf(stderr, "REAL TRANSFER IB CONN: LID %#04x, QPN %#06x, PSN %#06x RKey %#08x VAddr %#016Lx\n",
           //rmt_ib_conn->lid, rmt_ib_conn->qpn, rmt_ib_conn->psn, rmt_ib_conn->rkey, rmt_ib_conn->vaddr);

    TEST_NZ(ibv_post_send(ctx->qp,&ctx->wr,&bad_wr),
            "ibv_post_send failed. This is bad mkay");

    // Conrols if message was competely sent. But fails if client destroys his context to early. This would have to
    // be timed by the server telling the client that the rdma_write has been completed.

	int64_t ret = size;
	
    int ne;
    struct ibv_wc wc;

    do {
        ne = ibv_poll_cq(ctx->scq,1,&wc);
    } while(ne == 0);

    if (ne < 0) {
        fprintf(stderr, "rdma_read: poll CQ failed %d\n", ne);
		ret = 0;
    }

    if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "rdma_read: Failed status %d\n", wc.status);
			ret = 0;
        }
	
    pthread_mutex_unlock(&(ctx->ib_lock))
	//fprintf(stderr, "################# rb_block_receive_ib END\n");

	return ret;
}


