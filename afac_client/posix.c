/*
*		All the socket operation is not thread safe. We just build a prototype
*		assuming applications won't issue I/O in multi thread way.
*/
#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif


#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#include <limits.h>
#include <aio.h>
#include <netinet/in.h>
#include <sys/resource.h>
#define __USE_GNU
#include <pthread.h>

#include "communication.h"
#include "util.h"
#include "hash.h"

// JAY
#include "hashtable.h"
#include "../include/jay_global.h"
#include "../include/protocol_binary.h"
#include "../include/glex.h"
#include "sys/epoll.h"
#include "confread.h"
#include "shf.private.h"
#include "shf.h"
#include "tap.h"
#include <sys/sem.h>

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif
#ifndef HAVE_AIOCB64
#define aiocb64 aiocb
#endif



#ifdef DARSHAN_PRELOAD
#define __USE_GNU
#include <dlfcn.h>
#include <stdlib.h>

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  ret (*__real_ ## name)args = NULL;

#define DARSHAN_DECL(__name) __name

#define DARSHAN_MPI_CALL(func) __real_ ## func

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
           fprintf(stderr, "Darshan failed to map symbol: %s\n", #func); \
           exit(1); \
       } \
    }

#else

#define DARSHAN_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define DARSHAN_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#define DARSHAN_MPI_CALL(func) func

#endif

DARSHAN_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
DARSHAN_FORWARD_DECL(open, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
DARSHAN_FORWARD_DECL(close, int, (int fd));
DARSHAN_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
DARSHAN_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
DARSHAN_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
DARSHAN_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
DARSHAN_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
DARSHAN_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
DARSHAN_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
DARSHAN_FORWARD_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
DARSHAN_FORWARD_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
DARSHAN_FORWARD_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));
DARSHAN_FORWARD_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
DARSHAN_FORWARD_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
DARSHAN_FORWARD_DECL(fopen, FILE*,  (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
DARSHAN_FORWARD_DECL(fclose, int, (FILE *fp));
DARSHAN_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
DARSHAN_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(fseeko64, int, (FILE *stream, long offset, int whence));
DARSHAN_FORWARD_DECL(fsync, int, (int fd));
DARSHAN_FORWARD_DECL(fdatasync, int, (int fd));
DARSHAN_FORWARD_DECL(aio_read, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_read64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(aio_write, int, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_write64, int, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(lio_listio, int, (int mode, struct aiocb *const aiocb_list[], int nitems, struct sigevent *sevp));
DARSHAN_FORWARD_DECL(lio_listio64, int, (int mode, struct aiocb64 *const aiocb_list[], int nitems, struct sigevent *sevp));
DARSHAN_FORWARD_DECL(aio_return, ssize_t, (struct aiocb *aiocbp));
DARSHAN_FORWARD_DECL(aio_return64, ssize_t, (struct aiocb64 *aiocbp));
DARSHAN_FORWARD_DECL(fstat, int, (int fd, struct stat *buf));
DARSHAN_FORWARD_DECL(ioctl, int, (int fd, int command, char * argstruct));

// JAY
DARSHAN_FORWARD_DECL(unlink, int, (const char* path));


/********************************************************************/
/**********************   JAY  STRUCTS ********************************/
typedef struct
{
    char name[MAX_SERVER_NAME];
    int sfd;
    conn_cli* conn;
} server_conn_info;

typedef struct
{
    int file_block_id;
    int ost_id;
    int need_access_remotely;
} scatter_block_info;

typedef struct
{
    int is_local_cached;
    int device_block_id;
} MetadataLocalCachedInfo;




/********************************************************************/
/**********************   JAY  GLOBLES ********************************/
int DEBUG;

static int					G_MAX_FDS = -1;

static int 				G_SERVER_NUM;
static server_conn_info*	G_SERVER_CONNS;
static int			MY_SERVER_ID;
static int				G_ION_NUM;

static hash_table fd_map_table;

struct timeval start_time, end_time;

// RDMA
static rdma_endpoint cli_rdma;
static pthread_cond_t cli_mem_blocks_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t cli_mem_blocks_mutex = PTHREAD_MUTEX_INITIALIZER;
static rdma_mem_block *cli_mem_blocks;
static int MAX_NUM_CLI_RDMA_MEM_BLOCK;
static int num_free_cli_rdma_mem_block;


static Config_Param config_param;

/********************************************************************/
/**********************   JAY  DECLS     ********************************/

static void rdma_init();
static void ssd_device_init();

static void read_config_file();
static void metadata_caching_init();


static int intercept_init(void);
static void server_conn_init();
static void my_server_id_init();


static void establish_all_connection();


// RDMA
static rdma_mem_block* get_mem_block();
static void release_mem_block(int mem_block_id);


static void set_conn_cork(int sfd);
static void set_conn_uncork(int sfd);


static void put_entry_metadata_cache(char * path, int file_block_id, MetadataCachingEntry * entry);
static MetadataCachingEntry * get_entry_metadata_cache(char * path, int file_block_id);
static void del_entry_metadata_cache(char * path, int file_block_id);



enum hashfunc_type hash_type = JENKINS_HASH;

/* these are paths that we will not trace */
static char* exclusions[] =
{
    "/etc/",
    "/dev/",
    "/usr/",
    "/bin/",
    "/boot/",
    "/lib/",
    "/opt/",
    "/sbin/",
    "/sys/",
    "/proc/",
    NULL
};
/*this is the path we only intercept*/
static char* inclusions[] =
{
    "/vol-th/",
    "/vol-6/",
    "/WORK/",
    "/THL4/",
    "/vol6/",
    NULL
};

typedef struct
{
    int is_successive;
    int last_accessed_block_id;
    unsigned long accumulated_data_size;
    int last_cached_server_id;
} SuccessiveRecorder;

typedef struct
{
    int cfd;		// cache layer fd
    int server_id;
    int sfd;		// socket fd for this file
    char server_name[8];
    char file_name[PATH_MAX];
    uint64_t file_offset;

    ost_open_arg open_arg;

    SuccessiveRecorder successive_recorder;
} fd_map;

//static fd_map *fd_map_array;

pthread_mutex_t cp_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
#define CP_LOCK() pthread_mutex_lock(&cp_mutex)
#define CP_UNLOCK() pthread_mutex_unlock(&cp_mutex)

static int init_count = 0;

static int main_init()
{
    if(!init_count)
    {
        read_config_file();
        DEBUG = config_param.client_debug_level;


        CP_LOCK();
        if(!init_count)
        {
            init_count++;
            if((intercept_init() != 0) || (conn_cli_init(0) != 0))
            {
                if(DEBUG >= 2) fprintf(stderr, "=========== main init failed\n");
                return -1;
            }
        }
        //server_map_init();
        hash_init(hash_type);
        CP_UNLOCK();

        // JAY INIT
        mapping_realfd_cfd_init();
        server_conn_init();
        my_server_id_init();
        rdma_init();
        //metadata_caching_init();
        establish_all_connection();
        //ssd_device_init();

        if(DEBUG >= 2) fprintf(stderr, "=============================\n");
    }
    return 0;
}


static int intercept_init(void)
{
    /* We're unlikely to see an FD much higher than maxconns. */
    int next_fd = dup(1);
    int headroom = 10;      /* account for extra unexpected open FDs */
    struct rlimit rl;

    int max_fds = 1024 + headroom + next_fd;

    /* But if possible, get the actual highest FD we can possibly ever see. */
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0)
    {
        max_fds = rl.rlim_max;
    }
    else
    {
        fprintf(stderr, "Failed to query maximum file descriptor; "
                "falling back to maxconns\n");
    }
    MAP_OR_FAIL(close);
    __real_close(next_fd);

    G_MAX_FDS = max_fds;

    /*
    if ((fd_map_array = calloc(max_fds, sizeof(fd_map))) == NULL)
    {
        fprintf(stderr, "Failed to allocate connection structures\n");
        // This is unrecoverable so bail out early.
        return -1;
    }
    */

    ht_init(&fd_map_table, HT_KEY_CONST | HT_VALUE_CONST, 0.05);
    return 0;
}


/********************************************************************/
/**********************   JAY  FUNCS     ********************************/
static void insert_fd_map(fd_map *calloc_entry)
{
    ht_insert(&fd_map_table, &calloc_entry->cfd, sizeof(calloc_entry->cfd), calloc_entry, sizeof(calloc_entry));
}

static fd_map* get_fd_map(int cfd)
{
    if(fd_map_table.key_count == 0) return NULL;

    size_t dummy;
    return ht_get(&fd_map_table, &cfd, sizeof(cfd), &dummy);
}

static void remove_fd_map(int cfd)
{
    size_t dummy;
    fd_map* entry = ht_get(&fd_map_table, &cfd, sizeof(cfd), &dummy);
    if(entry != NULL)
        free(entry);
    ht_remove(&fd_map_table, &cfd, sizeof(cfd));
}

static void server_name_array_init()
{
    FILE *fd;
    fd = __real_fopen(SERVER_LIST_PATH, "a+");
    if(fd == NULL)
    {
        perror("open server name file failed!");
        return ;
    }

    int server_list_size = 8 * 1024;
    char buf[server_list_size];
    int read_byte = __real_fread(buf, 1, server_list_size, fd);
    if(read_byte <= 0)
    {
        perror("read server name file failure!");
    }

    // calculate the number of servers
    int i;
    int num_server = 0;
    for(i=0; i < read_byte; i++)
    {
        if(buf[i] == '\n')
            num_server ++;
    }
    assert(num_server != 0);
    G_SERVER_NUM = num_server;
    G_SERVER_CONNS = calloc(G_SERVER_NUM, sizeof(server_conn_info));

    // initialize the server name
    char *tmpstr = buf;
    char *token;
    int count = 0;
    while(token = strsep(&tmpstr, "\n"))
    {
        if(token[0] != 0)
        {
            assert(strlen(token) < MAX_SERVER_NAME);
            strcpy(G_SERVER_CONNS[count].name, token);
            count ++;
        }
    }

    //if(DEBUG >= 2)
    fprintf(stderr, "%d server,\t", G_SERVER_NUM);

    if(DEBUG >= 3)
    {
        for(i=0; i<G_SERVER_NUM; i++)
            fprintf(stderr, "%s\t", G_SERVER_CONNS[i].name);
        fprintf(stderr,"\n");
    }

    __real_fclose(fd);

    if(G_SERVER_NUM == 0)
    {
        fprintf(stderr, "error: can't initialize servers!\n");
        exit(1);
    }

}

static void ion_num_init()
{
	int num_ion = G_SERVER_NUM / config_param.ion_cn_ratio;
	if(num_ion <= 0)
	{
		fprintf(stderr, "ERROR: no enough server to support an ION\n");
		exit(1);
	}
	// 4,8,12 means 1,2,3
	if(G_SERVER_NUM % config_param.ion_cn_ratio == 0)
	{
		G_ION_NUM = num_ion;
	}
	// 5,6,7 means 2,  
	// 9,10,11 means 3.
	else
	{
		G_ION_NUM = num_ion + 1;		
	}
}

static void my_server_id_init()
{
	ion_num_init();
	
    char server_name[10];
    gethostname(server_name, sizeof(server_name));

    int i;
    for(i=0; i<G_SERVER_NUM; i++)
    {
        if(strcmp(server_name, G_SERVER_CONNS[i].name) == 0)
        {
            MY_SERVER_ID = i;
            fprintf(stderr, "MY_SERVER_ID = %d\n", MY_SERVER_ID);
            return;
        }
    }

    fprintf(stderr, "error: can't find my name(%s) in G_SERVER_NAME_ARRAY\n", server_name);
    MY_SERVER_ID = -1;
    exit(1);
}

static void server_conn_init()
{
    server_name_array_init();
    int i;
    for(i=0; i<G_SERVER_NUM; i++)
    {
        G_SERVER_CONNS[i].sfd = 0;
        G_SERVER_CONNS[i].conn = NULL;
    }

}

static void establish_all_connection()
{
    int ost_id;
    for(ost_id=0; ost_id<G_SERVER_NUM; ost_id++)
    {
        if(G_SERVER_CONNS[ost_id].sfd == 0) 	// never initialized before
        {
            G_SERVER_CONNS[ost_id].conn = cli_socket(G_SERVER_CONNS[ost_id].name, server_port, tcp_transport);
            if(G_SERVER_CONNS[ost_id].conn == NULL)
            {
                fprintf(stderr, "cli_socket failed!\n");
                return 0;
            }
            G_SERVER_CONNS[ost_id].sfd = G_SERVER_CONNS[ost_id].conn->sfd;
        }
    }
}


static int *mapping_between_real_fd_and_cfd;
static int mapping_realfd_cfd_init()
{
    mapping_between_real_fd_and_cfd = calloc(G_MAX_FDS, sizeof(int));
}

static int mapping_realfd_cfd_finalize()
{
    free(mapping_between_real_fd_and_cfd);
}


static int create_mapping_of_realfd_cfd(int real_fd, int cfd)
{
    assert(mapping_between_real_fd_and_cfd[real_fd] == 0);
    mapping_between_real_fd_and_cfd[real_fd] = cfd;
    return cfd;
}

static int clean_mapping_of_realfd(int real_fd)
{
    mapping_between_real_fd_and_cfd[real_fd] = 0;
    return 0;
}

static int get_mapping_of_realfd(int real_fd)
{
    return mapping_between_real_fd_and_cfd[real_fd];
}


static int unsafe_check_is_real_file_stream(FILE *stream)
{
    int cfd = (int) stream;
    int mfd = CFD_TO_MFD(cfd);
    int server_id = CFD_TO_SRV_ID(cfd);
    assert(G_MAX_FDS != -1);
    if(server_id >=0 && server_id < G_SERVER_NUM && mfd >= 3 && mfd <= G_MAX_FDS)
        return 0;	// this is jay's cfd
    else
        return 1;	// this is real file stream
}

static int check_is_hacked_fd(int fd)
{
    // if have no mapping, then I haven't hack this fd before
    // or, I closed this hacked fd
    return get_mapping_of_realfd(fd);
}

static int check_is_hacked_fstream(FILE * stream)
{
    int fd = fileno(stream);
    return get_mapping_of_realfd(fd);
}

static int get_file_real_fd(char* abs_path, int num_param, int flags, int mode)
{
    assert(num_param == 2 || num_param == 3);
    MAP_OR_FAIL(open);
    if(num_param == 2)
        return __real_open(abs_path, flags);
    else
        return __real_open(abs_path, flags, mode);
}
static conn_cli* send_request(	int ost_id, uint8_t op_code,
                                int num_para, void **para_addr_array, uint64_t *para_len_array)
{
    // check if this ost has never been connected before
    if(G_SERVER_CONNS[ost_id].sfd == 0) 	// never initialized before
    {
        G_SERVER_CONNS[ost_id].conn = cli_socket(G_SERVER_CONNS[ost_id].name, server_port, tcp_transport);
        if(G_SERVER_CONNS[ost_id].conn == NULL)
        {
            fprintf(stderr, "cli_socket failed!\n");
            return 0;
        }
        G_SERVER_CONNS[ost_id].sfd = G_SERVER_CONNS[ost_id].conn->sfd;
    }

    assert(G_SERVER_CONNS[ost_id].sfd != 0);	// all sfd must be initialized before

    int request_len = 0;
    int para_i;
    for(para_i=0; para_i<num_para; para_i++)
    {
        request_len += para_len_array[para_i];
    }
    int message_len = request_len + sizeof(protocol_binary_request_header);

    int package_num;
    int package_id;
    int package_offset;
    int package_body_len;

    // check the connection
    conn_cli* c = NULL;
    struct sockaddr server_addr;
    int connected =  connection_close_exam(G_SERVER_CONNS[ost_id].sfd, &server_addr) ;
    if(connected != 0)
    {
        if(DEBUG) fprintf(stderr, "ost[%d] no longer connected\n", ost_id);

        c = cli_socket(G_SERVER_CONNS[ost_id].name, server_port, tcp_transport);
        if(c == NULL)
        {
            fprintf(stderr, "cli_socket failed!\n");
            return 0;
        }
    }
    if(connected == 0) /*just do not consider the ip address equal or not*/
    {
        // this means the socket is still connected
        /*send data to server*/
        c = conn_cli_new(G_SERVER_CONNS[ost_id].sfd, 0, tcp_transport);
        if(c == NULL)
        {
            fprintf(stderr, "conn_cli_new failed!\n");
            return 0;
        }
    }
    assert(c != NULL);
    G_SERVER_CONNS[ost_id].conn= c;
    G_SERVER_CONNS[ost_id].sfd = c->sfd;

    // check the wirte buffer of conn
    if(c->wsize < message_len)
    {
        char *newbuf = realloc(c->wbuf, message_len);
        if(newbuf != NULL)
        {
            c->wbuf = newbuf;
            c->wsize = message_len;
            c->wcurr = c->wbuf;
        }
        else
        {
            perror("Out of memory");
            fprintf(stderr, "no memory for sendbuf\n");
            assert(0);
            return 0;
        }
    }
    if(c->wcurr != c->wbuf)
    {
        memmove(c->wbuf, c->wcurr, c->wbytes);
        c->wcurr = c->wbuf;
    }

    // pack the request
    protocol_binary_request_header *req;
    req = (protocol_binary_request_header *)(c->wcurr);
    c->wcurr += sizeof(protocol_binary_request_header);
    c->wbytes += sizeof(protocol_binary_request_header);
    /*fill the header of the message*/
    req->request.magic = PROTOCOL_BINARY_REQ;
    req->request.opcode = op_code;
    req->request.isfrag = 1; /*do not package any more*/
    req->request.frag_id = 1; /*beginning from 1*/
    req->request.request_id = 0;
    req->request.para_num = num_para;
    req->request.reserved = 0;
    req->request.body_len = htonll((uint64_t)request_len);
    req->request.request_len = req->request.body_len;
    req->request.frag_offset = 0;
    req->request.para_len = req->request.request_len;

    req->request.para1_len = 0;
    req->request.para2_len = 0;
    req->request.para3_len = 0;
    req->request.para4_len = 0;
    req->request.para5_len = 0;
    req->request.para6_len = 0;
    for(para_i=0; para_i<num_para; para_i++)
    {
        switch(para_i)
        {
        case 0:
            req->request.para1_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        case 1:
            req->request.para2_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        case 2:
            req->request.para3_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        case 3:
            req->request.para4_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        case 4:
            req->request.para5_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        case 5:
            req->request.para6_len = htonll((uint64_t)(para_len_array[para_i]));
            break;
        default:
            fprintf(stderr, "something wrong with the para num\n");
            break;
        }
    }

    /*copy the parameters of the request to the message body*/
    for(para_i=0; para_i<num_para; para_i++)
    {
        memcpy(c->wcurr, para_addr_array[para_i], para_len_array[para_i]);
        c->wcurr = c->wcurr + para_len_array[para_i];
        c->wbytes += para_len_array[para_i];
    }

    /*write the message to the server*/
    int res = 0;
    int left = message_len;
    char *snd_curr = c->wbuf;

    //if(message_len > 1024) set_conn_cork(c->sfd);

    MAP_OR_FAIL(write);

    while(left > 0)
    {
        res = __real_write(c->sfd, snd_curr, (size_t)left);		// send the read req to server
        if(res > 0)
        {
            left -= res;
            c->wbytes -= res;
            snd_curr = snd_curr + res;
        }
        /*error handling*/
        if((res == -1)&&((errno == EAGAIN)||(errno == EWOULDBLOCK)))
            continue;//break;
        if(res == 0)
        {
            fprintf(stderr, "The connection is closed\n");
            return 0;
        }
        if(res == 0)
        {
            fprintf(stderr, "send data error: some unknown error happen\n");
            return 0;
        }
    }

    //if(message_len > 1024) set_conn_uncork(c->sfd);

    return c;
}

static void detect_successive_io(fd_map* my_fd_map, int start_block_index, int end_block_index, size_t size)
{
    if(size <= config_param.LASIOD_SMALL_IO_SIZE)
    {
        // reset
        my_fd_map->successive_recorder.is_successive = 0;
        my_fd_map->successive_recorder.last_accessed_block_id = -1;
        my_fd_map->successive_recorder.accumulated_data_size = 0;
        my_fd_map->successive_recorder.last_cached_server_id = -1;
    }
    else
    {
        if(start_block_index - my_fd_map->successive_recorder.last_accessed_block_id == 0
                || start_block_index - my_fd_map->successive_recorder.last_accessed_block_id == 1)
        {
            my_fd_map->successive_recorder.is_successive = 1;
            my_fd_map->successive_recorder.last_accessed_block_id = end_block_index;
            my_fd_map->successive_recorder.accumulated_data_size += size;
            // my_fd_map->successive_recorder.last_cached_server_id remains unchanged
        }
        else
        {
            my_fd_map->successive_recorder.is_successive = 0;
            my_fd_map->successive_recorder.last_accessed_block_id = end_block_index;
            my_fd_map->successive_recorder.accumulated_data_size = size;
            my_fd_map->successive_recorder.last_cached_server_id = -1;
        }
    }
}

static void inform_my_own_ost_to_prepare_data(fd_map* my_fd_map, int cfd, size_t curr_offset, size_t count,
        int* block_metadata, int num_block, SuccessiveInfo *successive_info)
{
	int i, need_inform_my_own_ost = 0;
	for(i=0; i<num_block; i++)
	{
		if(block_metadata[i] == MY_SERVER_ID)
		{
			need_inform_my_own_ost = 1;
			break;
		}
	}
	if(need_inform_my_own_ost == 0)	return;
	
	
    if(DEBUG >= 2) fprintf(stderr, "<<< send inform_ost_to_prepare_data\n");

    int ost_id = MY_SERVER_ID;

    // send the request
    // QUERY MDS: 4 params: cfd, offset, size, MY_SERVER_ID
    int num_para = 5;
    void *para_addr_array[num_para];
    uint64_t para_len_array[num_para];

    para_addr_array[0] = &cfd;
    para_len_array[0] = sizeof(cfd);
    para_addr_array[1] = &count;
    para_len_array[1] = sizeof(count);
    para_addr_array[2] = &curr_offset;
    para_len_array[2] = sizeof(curr_offset);
    para_addr_array[3] = block_metadata;
    para_len_array[3] = sizeof(int) * num_block;
    para_addr_array[4] = successive_info;
    para_len_array[4] = sizeof(SuccessiveInfo);
    conn_cli *c = send_request(ost_id, PROTOCOL_BINARY_CMD_OST_PREPARE_DATA,
                               num_para, para_addr_array, para_len_array);

    /*fourth: wait for response, check integrity of the package */
    int estimate_resp_size = sizeof(int);
    if(c->rsize < (sizeof(protocol_binary_response_header) + estimate_resp_size))
    {
        c->rbuf = realloc(c->rbuf, sizeof(protocol_binary_response_header) + estimate_resp_size);
        c->rsize = sizeof(protocol_binary_response_header) + estimate_resp_size;
        c->rcurr = c->rbuf;
    }

    while(1)
    {
        int read_metedata_ret = cli_must_read_command(c);		// blocked to wait for the requested data
        if(read_metedata_ret != 0)
        {
            fprintf(stderr, "cli_try_read_command failed\n");
            exit(1);
        }
        protocol_binary_response_header *resp;
        int ret;
        if(c->frag_num == 1)
        {
            resp = (protocol_binary_response_header *)c->rbuf;
            if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_OST_PREPARE_DATA))
            {
                fprintf(stderr, "Message failure, magic or opcode is wrong\n");
            	exit(1);
            }
            memcpy(&ret, (char*)c->rbuf + sizeof(protocol_binary_response_header), resp->response.para1_len);
        }
        if(c->frag_num > 1)
        {
            resp = (protocol_binary_response_header *)c->assemb_buf;
            if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_OST_PREPARE_DATA))
            {
                fprintf(stderr, "Message failure, magic or opcode is wrong\n");
            	exit(1);
            }
            memcpy(&ret, (char*)c->assemb_buf + sizeof(protocol_binary_response_header), resp->response.para1_len);
        }

        // means need an open_arg
        if (ret == 0)
        {
            ost_open_arg *open_arg = &(my_fd_map->open_arg);

            int num_para = 6;
            void *para_addr_array[num_para];
            uint64_t para_len_array[num_para];

            para_addr_array[0] = &cfd;
            para_len_array[0] = sizeof(cfd);
            para_addr_array[1] = &count;
            para_len_array[1] = sizeof(count);
            para_addr_array[2] = &curr_offset;
            para_len_array[2] = sizeof(curr_offset);
            para_addr_array[3] = block_metadata;
            para_len_array[3] = sizeof(int) * num_block;
            para_addr_array[4] = successive_info;
            para_len_array[4] = sizeof(SuccessiveInfo);
            para_addr_array[5] = open_arg;
            para_len_array[5] = sizeof(ost_open_arg);
            conn_cli *c = send_request(ost_id, PROTOCOL_BINARY_CMD_OST_PREPARE_DATA,
                                       num_para, para_addr_array, para_len_array);
        }
        else
        {
            break;
        }

    }

    if(DEBUG >= 2) fprintf(stderr, ">>> recv inform_ost_to_prepare_data\n");
}

static int retrieve_block_metadata(fd_map* my_fd_map, int cfd, size_t curr_offset, size_t count,
                                   int* block_metadata, MetadataLocalCachedInfo *meta_cache_info, int is_read)
{
    int start_block_index = curr_offset >> G_BLOCK_SIZE_SHIFT;
    int end_block_index = (curr_offset + count - 1) >> G_BLOCK_SIZE_SHIFT;
    int num_block_index = end_block_index - start_block_index + 1;
    assert(num_block_index >= 1);
    int bytes_block_metadata = sizeof(int) * num_block_index;
	
    int i;
	for(i=0; i<num_block_index; i++)
	{
		block_metadata[i] = my_fd_map->server_id;
	}

	
	return bytes_block_metadata;  
}

static int device_fd;
static int DEVICE_SEM_ID;
static void ssd_device_init()
{
    char my_server_name[10];
    gethostname(my_server_name, sizeof(my_server_name));

    char device_path[PATH_MAX];

    // lustre file device path
    //sprintf(device_path, "%s%s", OST_CACHE_DEICE_PATH_PREFIX, my_server_name);

    // tmpfs file device path
    if(config_param.cache_device_tmpfs == 1)
    {
        char* tmpfs_path_prefix = config_param.tmpfs_path_prefix;
        sprintf(device_path, "%s%s%s", tmpfs_path_prefix, my_server_name, config_param.tmpfs_path_suffix);
    }
    else
    {
        strcpy(device_path, config_param.ssd_path);
    }

    device_fd = __real_open(device_path, O_RDWR);
    if(device_fd < 0)
    {
        fprintf(stderr, "Cannot open device: %s\n", device_path);
        exit(1) ;
    }

    // FOR SIMULATE SSD
    DEVICE_SEM_ID = semget((key_t)SIMULATE_SSD_SEM_KEY, 1, 0666 | IPC_CREAT);

}

static void ssd_device_finalize()
{
    __real_close(device_fd);
}

static inline void asm_delay(int count)
{
    int i;
    for(i=0; i<count; i++)
    {
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
        asm("xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx; xchg %bx,%bx");
    }
}



static struct timeval sema_start, sema_end;
static inline void device_semaphore_p()
{
    //if(DEBUG >= 2) gettimeofday(&sema_start, NULL);

    struct sembuf sem_b;
    sem_b.sem_num = 0;
    sem_b.sem_op = -1;//P()
    sem_b.sem_flg = SEM_UNDO;
    if(semop(DEVICE_SEM_ID, &sem_b, 1) == -1)
    {
        fprintf(stderr, "device_semaphore_p failed\n");
        exit(1);
    }

    //if(DEBUG >= 2) gettimeofday(&sema_end, NULL);
    //if(DEBUG >= 2) fprintf(stderr, "semaphore_p:\t %lu\n", 1000000 * ( sema_end.tv_sec - sema_start.tv_sec ) + sema_end.tv_usec - sema_start.tv_usec);
}

static inline void device_semaphore_v()
{
    //if(DEBUG >= 2) gettimeofday(&sema_start, NULL);

    struct sembuf sem_b;
    sem_b.sem_num = 0;
    sem_b.sem_op = 1;//V()
    sem_b.sem_flg = SEM_UNDO;
    if(semop(DEVICE_SEM_ID, &sem_b, 1) == -1)
    {
        fprintf(stderr, "semaphore_v failed\n");
        exit(1);
    }

    //if(DEBUG >= 2) gettimeofday(&sema_end, NULL);
    //if(DEBUG >= 2) fprintf(stderr, "semaphore_v:\t %lu\n", 1000000 * ( sema_end.tv_sec - sema_start.tv_sec ) + sema_end.tv_usec - sema_start.tv_usec);
}

/*
struct timeval local_delay_start, local_delay_end;


static inline void simulate_ssd_start()
{
	if(config_param.cache_device_tmpfs == 0 || config_param.simulate_ssd == 0) return;

	device_semaphore_p();
	gettimeofday(&local_delay_start, NULL);
}

static inline void simulate_ssd_end()
{
	if(config_param.cache_device_tmpfs == 0 || config_param.simulate_ssd == 0) return;

	gettimeofday(&local_delay_end, NULL);
	unsigned long usecs_delay = config_param.latency_delay_ratio * (1000000 * ( local_delay_end.tv_sec - local_delay_start.tv_sec ) + local_delay_end.tv_usec - local_delay_start.tv_usec);
	//if(DEBUG >= 2) fprintf(stderr, "simulate ssd need delay:\t %lu\n", usecs_delay);
	//usleep(usecs_delay);

	//if(DEBUG >= 2) gettimeofday(&local_delay_start, NULL);
	asm_delay(usecs_delay);
	//if(DEBUG >= 2) gettimeofday(&local_delay_end, NULL);
	//unsigned long actual_delay = (1000000 * ( local_delay_end.tv_sec - local_delay_start.tv_sec ) + local_delay_end.tv_usec - local_delay_start.tv_usec);
	//if(DEBUG >= 2) fprintf(stderr, "simulate ssd actual delay:\t %lu\n", actual_delay);

	device_semaphore_v();
}
*/

//static double discount_array[9] = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
static double discount_array[9] = {0, 0, 1, 1, 1, 1, 1, 1, 1};
//static double discount_array[9] = {0, 0, 1, 1, 1, 1, 1, 1, 1};
static size_t discount_size[9] = {4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576};
static double dicount_with_io_size(int size)
{
	int i;
	for(i=0; i<9; i++)
	{
		if (size < discount_size[i]) break;
	}

	if(i == 9)
	{
		return 1;
	}
	else if(i == 0)
	{
		return discount_array[i] * size / discount_size[i];
	}
	else
	{
		return discount_array[i - 1] + (double)(size - discount_size[i - 1]) / discount_size[i - 1] * (discount_array[i] - discount_array[i - 1]); 
	}
}

static void keep_tmpfs_busy_by_writing(char* buf, unsigned long offset, int size, double num_times)
{
    if(config_param.cache_device_tmpfs == 1 && config_param.simulate_ssd == 1)
    {
		if(size < 5000) num_times *= 0.5;
		
        int count = (int)num_times;
        double digit_times = num_times - count;
        if(size * digit_times != 0)
        {
            if(__real_lseek(device_fd, offset, SEEK_SET) < 0)
            {
                perror("lseek error in keep_tmpfs_busy_by_writing()");
                return;
            }
            int bytes = __real_write(device_fd, buf, size * digit_times);
            if(bytes <= 0)
            {
                perror("write error in keep_tmpfs_busy_by_writing()\n");
            }
        }

        int i;
        for(i=0; i<count; i++)
        {
            if(__real_lseek(device_fd, offset, SEEK_SET) < 0)
            {
                perror("lseek error in keep_tmpfs_busy_by_writing()");
                return;
            }
            int bytes = __real_write(device_fd, buf, size);
            if(bytes <= 0)
            {
                perror("write error in keep_tmpfs_busy_by_writing()\n");
            }
        }
    }
}

static void keep_tmpfs_busy_by_reading(char* buf, unsigned long offset, int size, double num_times)
{
    if(config_param.cache_device_tmpfs == 1 && config_param.simulate_ssd == 1)
    {
		//num_times *= dicount_with_io_size(size);
		
        int count = (int)num_times;
        double digit_times = num_times - count;
        if(size * digit_times != 0)
        {
            if(__real_lseek(device_fd, offset, SEEK_SET) < 0)
            {
                perror("lseek error in keep_tmpfs_busy_by_writing()");
                return;
            }
            int bytes = __real_read(device_fd, buf, size * digit_times);
            if(bytes <= 0)
            {
                perror("read error in keep_tmpfs_busy_by_writing()\n");
            }
        }

        int i;
        for(i=0; i<count; i++)
        {
            if(__real_lseek(device_fd, offset, SEEK_SET) < 0)
            {
                perror("lseek error in keep_tmpfs_busy_by_writing()");
                return;
            }
            int bytes = __real_read(device_fd, buf, size);
            if(bytes <= 0)
            {
                perror("read error in keep_tmpfs_busy_by_writing()\n");
            }
        }
    }
}


static void read_ssd_data_locally(char * buf, int offset_in_buf, int device_block_id, int start_offset, int end_offset)
{
    if(__real_lseek(device_fd, ((unsigned long)device_block_id) * G_BLOCK_SIZE_IN_BYTES + start_offset, SEEK_SET) < 0)
    {
        perror("lseek error in read_ssd_data_locally()");
        return;
    }
    device_semaphore_p();
    int bytes = __real_read(device_fd, buf, end_offset - start_offset + 1);
    if(bytes <= 0)
    {
        perror("read error in read_ssd_data_locally()\n");
    }
    assert(bytes == end_offset - start_offset + 1);

    if(config_param.cache_device_tmpfs == 1 && config_param.simulate_ssd == 1)
    {
        keep_tmpfs_busy_by_writing(buf, ((unsigned long)device_block_id) * G_BLOCK_SIZE_IN_BYTES + start_offset,
                                   (end_offset - start_offset + 1), config_param.simulate_read_latency );
    }

    device_semaphore_v();

}

static void write_ssd_data_locally(char * buf, int offset_in_buf, int device_block_id, int start_offset, int end_offset)
{
    if(__real_lseek(device_fd, ((unsigned long)device_block_id) * G_BLOCK_SIZE_IN_BYTES + start_offset, SEEK_SET) < 0)
    {
        perror("lseek error in write_ssd_data_locally()");
        return;
    }
    device_semaphore_p();
    int bytes = __real_write(device_fd, buf, end_offset - start_offset + 1);
    if(bytes <= 0)
    {
        perror("write error in write_ssd_data_locally()\n");
    }
    assert(bytes == end_offset - start_offset + 1);

    if(config_param.cache_device_tmpfs == 1 && config_param.simulate_ssd == 1)
        keep_tmpfs_busy_by_writing(buf, ((unsigned long)device_block_id) * G_BLOCK_SIZE_IN_BYTES + start_offset,
                                   (end_offset - start_offset + 1), config_param.simulate_write_latency);
    device_semaphore_v();
}

static int check_ost_id_is_unique(scatter_block_info* this_round_block_array, int size_this_round, int ost_id)
{
    int i;
    /*
    if(DEBUG)
    {
    	printf("round_array: ");
    	for(i=0; i<size_this_round; i++)
    	{
    		printf("%d ", this_round_block_array[i].ost_id);
    	}
    	printf("\n");
    }
    */

    for(i=0; i<size_this_round; i++)
    {
        if(this_round_block_array[i].ost_id == ost_id)
            return 0;	// find a same id, not unique
    }
    return 1;	// is unique
}

static void scatter_block_metadata(scatter_block_info* scatterred_block_metadata, int *block_metadata, int num_block_metadata,
                                   int start_block_index, MetadataLocalCachedInfo *meta_cache_info)
{
    int i;

    /*	for(i=0; i<num_block_metadata; i++)
        {
            scatterred_block_metadata[i].file_block_id = start_block_index + i;
            scatterred_block_metadata[i].ost_id = block_metadata[i];
        }

        // test if not

        return scatterred_block_metadata;
    */

    if(num_block_metadata == 1)
    {
        scatterred_block_metadata[0].ost_id = block_metadata[0];
        scatterred_block_metadata[0].file_block_id = start_block_index;
        scatterred_block_metadata[0].need_access_remotely = 1;
        return scatterred_block_metadata;
    }

    uint8_t is_scattered[num_block_metadata];
    memset(is_scattered, 0, num_block_metadata * sizeof(uint8_t));
    int this_round_start = 0, size_this_round = 0;
    int num_is_scattered = 0;
    while(1)
    {
        this_round_start += size_this_round;
        size_this_round = 0;


        for(i=0; i<num_block_metadata; i++)
        {
            if(is_scattered[i] == 0 && check_ost_id_is_unique(scatterred_block_metadata + this_round_start, size_this_round, block_metadata[i]))
            {
                // is unique
                size_this_round++;
                scatterred_block_metadata[this_round_start + size_this_round - 1].ost_id = block_metadata[i];
                scatterred_block_metadata[this_round_start + size_this_round - 1].file_block_id = start_block_index + i;
                is_scattered[i] = 1;
                num_is_scattered++;
                if(num_is_scattered == num_block_metadata)
                    break;

                //if(DEBUG)
                //printf("scatterred_block_metadata[%d].ost_id = block_metadata[%d]\n", this_round_start + size_this_round - 1, i);
            }
        }

        assert(size_this_round != 0);	// must get some unique id

        if(num_is_scattered == num_block_metadata)
            break;
    }

    if(DEBUG >= 2)
    {
        printf("before: ");
        for(i=0; i<num_block_metadata; i++)
        {
            printf("%d ", block_metadata[i]);
        }
        printf("\n");
        printf("after: ");
        for(i=0; i<num_block_metadata; i++)
        {
            printf("%d ", scatterred_block_metadata[i].ost_id);
        }
        printf("\n");
    }


    for(i=0; i<num_block_metadata; i++)
    {
        int tmp_i = scatterred_block_metadata[i].file_block_id - start_block_index;
        if(meta_cache_info[tmp_i].is_local_cached && meta_cache_info[tmp_i].device_block_id != -1)
        {
            scatterred_block_metadata[i].need_access_remotely = 0;
        }
        else
        {
            scatterred_block_metadata[i].need_access_remotely = 1;
        }
    }


    return scatterred_block_metadata;
}

static void send_read_request_to_ost(int cfd, scatter_block_info* scatter_blk, int start_offset, int end_offset)
{
    int dest_ost = scatter_blk->ost_id == -1 ? MY_SERVER_ID : scatter_blk->ost_id;

    // pack the blk_req
    block_data_req blk_req;
    blk_req.requesting_srv_id = MY_SERVER_ID;
    blk_req.cfd = cfd;
    blk_req.file_block_id = scatter_blk->file_block_id;
    blk_req.start_offset = start_offset;
    blk_req.end_offset = end_offset;

    // send block req to OST
    int num_para = 1;
    void *para_addr_array[num_para];
    uint64_t para_len_array[num_para];
    para_addr_array[0] = &blk_req;
    para_len_array[0] = sizeof(blk_req);
    G_SERVER_CONNS[dest_ost].conn = send_request(dest_ost, PROTOCOL_BINARY_CMD_RDMA_READ_OST,
                                    num_para, para_addr_array, para_len_array);

    if(DEBUG >= 2) fprintf(stderr, "<<< send block[%d] read req to OST[%d]\n", blk_req.file_block_id, dest_ost);

}

static void send_write_request_to_ost(int cfd, scatter_block_info* scatter_blk, int offset_in_buf, int start_offset, int end_offset)
{
    int dest_ost = scatter_blk->ost_id == -1 ? MY_SERVER_ID : scatter_blk->ost_id;

    // pack the blk_req
    block_data_req blk_req;
    blk_req.requesting_srv_id = MY_SERVER_ID;
    blk_req.cfd = cfd;
    blk_req.file_block_id = scatter_blk->file_block_id;
    blk_req.start_offset = start_offset;
    blk_req.end_offset = end_offset;

    // pack the rdma_req
    rdma_data_req rdma_req;
    rdma_req.mh = cli_rdma.mh;
    rdma_req.ep_addr = cli_rdma.ep_addr;
    rdma_req.offset = offset_in_buf;
    rdma_req.len = end_offset - start_offset + 1;
    //rdma_req.mem_blk_id = rdma_mem_blk->id;
    rdma_req.src_srv_id = MY_SERVER_ID;
    //memcpy(rdma_mem_blk->mem + start_offset, data_buf, rdma_req.len);

    // send block req to OST
    int num_para = 2;
    void *para_addr_array[num_para];
    uint64_t para_len_array[num_para];
    para_addr_array[0] = &blk_req;
    para_len_array[0] = sizeof(block_data_req);
    para_addr_array[1] = &rdma_req;
    para_len_array[1] = sizeof(rdma_data_req);
    G_SERVER_CONNS[dest_ost].conn = send_request(dest_ost, PROTOCOL_BINARY_CMD_RDMA_WRITE_OST,
                                    num_para, para_addr_array, para_len_array);

    if(DEBUG >= 2) fprintf(stderr, "<<< send block[%d] write req to OST[%d]\n", blk_req.file_block_id, dest_ost);
}


static void pull_rdma_data(rdma_mem_block *mem_blk, block_data_req *blk_req, rdma_data_req *rdma_req)
{
    struct timeval pull_start_time, pull_end_time;
    if(DEBUG >= 2)	gettimeofday(&pull_start_time, NULL);

    glex_ret_t glex_ret;
    struct glex_rdma_req glex_req, *bad_glex_req;
    uint32_t cqe_num;
    struct glex_cqe cqe[4];

    glex_req.local_mh.v 	= mem_blk->endpoint->mh.v;
    glex_req.local_offset	= mem_blk->mem - mem_blk->endpoint->ep_mem + blk_req->start_offset;
    glex_req.len			= rdma_req->len;
    glex_req.rmt_mh.v		= rdma_req->mh.v;
    glex_req.rmt_offset 	= rdma_req->offset;
    glex_req.type			= GLEX_RDMA_TYPE_READ; 	/* GLEX_RDMA_TYPE_READ */
    glex_req.rmt_key		= mem_blk->endpoint->ep_attr.key;
    glex_req.flag			= GLEX_FLAG_SIGNALED;
    glex_req.next			= NULL;

    pthread_mutex_lock(&mem_blk->endpoint->trans_mutex);
    while ((glex_ret = glex_rdma(mem_blk->endpoint->ep, rdma_req->ep_addr, &glex_req, &bad_glex_req)) == GLEX_BUSY) {	}
    if (glex_ret != GLEX_SUCCESS)
    {
        fprintf(stderr,"error: _rdma(), return: %d\n", glex_ret);
        exit(1);
    }

    do
    {
        cqe_num = 1;
        glex_poll_cq(mem_blk->endpoint->ep, &cqe_num, cqe);
    }
    while (cqe_num < 1);
    pthread_mutex_unlock(&mem_blk->endpoint->trans_mutex);

    if(DEBUG >= 2)	gettimeofday(&pull_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ rdma time: %lu\n", 1000000 * ( pull_end_time.tv_sec - pull_start_time.tv_sec ) + pull_end_time.tv_usec - pull_start_time.tv_usec);
}


static void set_conn_cork(int sfd)
{
    int tcp_cork_on = 1;
    if(setsockopt(sfd, SOL_TCP, TCP_CORK, &tcp_cork_on, sizeof(tcp_cork_on)) != 0)
    {
        perror("set_conn_cork error");
    }
}

static void set_conn_uncork(int sfd)
{
    int tcp_cork_on = 0;
    if(setsockopt(sfd, SOL_TCP, TCP_CORK, &tcp_cork_on, sizeof(tcp_cork_on)) != 0)
    {
        perror("set_conn_uncork error");
    }
}

static void rdma_init()
{
    glex_ret_t ret;

    /*
    size_t page_size = sysconf(_SC_PAGESIZE);
    uint32_t ep_mem_size = ((uint32_t) SIZE_RDMA_MEM_BLK) * NUM_RDMA_MEM_BLK_PER_PROC;
    if (posix_memalign((void **)&cli_rdma.ep_mem, page_size, ep_mem_size))
    {
        printf("error: cannot allocate memory\n");
        exit(1);
    }
    memset(cli_rdma.ep_mem, 0, ep_mem_size);
    if(DEBUG >= 2) fprintf(stderr, "Init RDMA Memory: [%d Bytes]\n", ep_mem_size);
    */

    ret = glex_init();
    if (ret != GLEX_SUCCESS)
    {
        printf("error: rdma_init(), return: %d\n", ret);
        exit(1);
    }

    cli_rdma.ep_attr.key					= 0x66;
    cli_rdma.ep_attr.cap.dq_capacity		= GLEX_EP_CAP_DQ_CAPACITY_DEFAULT;
    cli_rdma.ep_attr.cap.cq_capacity		= GLEX_EP_CAP_CQ_CAPACITY_DEFAULT;
    cli_rdma.ep_attr.cap.mpq_capacity	= GLEX_EP_CAP_MPQ_CAPACITY_DEFAULT;

    uint32_t local_ep_num = GLEX_ANY_EP_NUM;

    ret = glex_create_ep(0, local_ep_num, &cli_rdma.ep_attr, &cli_rdma.ep);
    if (ret != GLEX_SUCCESS)
    {
        printf("error: _create_ep(), return: %d\n", ret);
        exit(1);
    }

    glex_get_ep_addr(cli_rdma.ep, &cli_rdma.ep_addr);

    pthread_mutex_init(&cli_rdma.trans_mutex, NULL);

    /*
    struct timeval rgst_start_time, rgst_end_time;
    if(DEBUG >= 2)	gettimeofday(&rgst_start_time, NULL);

    ret = glex_register_mem(cli_rdma.ep, cli_rdma.ep_mem, ep_mem_size, &cli_rdma.mh);

    if(DEBUG >= 2) gettimeofday(&rgst_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ rgst time: %lu\n", 1000000 * ( rgst_end_time.tv_sec - rgst_start_time.tv_sec ) + rgst_end_time.tv_usec - rgst_start_time.tv_usec);


    if (ret != GLEX_SUCCESS)
    {
        printf("error: _register_mem(), return: %d\n", ret);
        exit(1);
    }


    MAX_NUM_CLI_RDMA_MEM_BLOCK = NUM_RDMA_MEM_BLK_PER_PROC;
    cli_mem_blocks = calloc(1, sizeof(rdma_mem_block) * MAX_NUM_CLI_RDMA_MEM_BLOCK);
    int i;
    for(i=0; i<MAX_NUM_CLI_RDMA_MEM_BLOCK; i++)
    {
        cli_mem_blocks[i].id = i;
        cli_mem_blocks[i].is_free = 1;
        cli_mem_blocks[i].endpoint = &cli_rdma;
        cli_mem_blocks[i].mem = cli_rdma.ep_mem + i * SIZE_RDMA_MEM_BLK;
        assert(cli_mem_blocks[i].mem);
    }
    num_free_cli_rdma_mem_block = MAX_NUM_CLI_RDMA_MEM_BLOCK;
    */
}

static void rdma_finalize()
{
    free(cli_rdma.ep_mem);
    //free(cli_mem_blocks);
}

static void register_rdma_buf(char * buf, size_t buf_len)
{
    struct timeval rgst_start_time, rgst_end_time;
    if(DEBUG >= 2)	gettimeofday(&rgst_start_time, NULL);

    glex_ret_t ret = glex_register_mem(cli_rdma.ep, buf, buf_len, &cli_rdma.mh);
    if (ret != GLEX_SUCCESS)
    {
        printf("error: register_rdma_buf(), return: %d\n", ret);
        exit(1);
    }

    if(DEBUG >= 2) gettimeofday(&rgst_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ rgst time: %lu\n", 1000000 * ( rgst_end_time.tv_sec - rgst_start_time.tv_sec ) + rgst_end_time.tv_usec - rgst_start_time.tv_usec);
}

static void deregister_rdma_buf()
{
    glex_ret_t ret = glex_deregister_mem(cli_rdma.ep, cli_rdma.mh);
    if (ret != GLEX_SUCCESS)
    {
        printf("error: register_rdma_buf(), return: %d\n", ret);
        exit(1);
    }
}

static void pull_rdma_buf(rdma_data_req *rdma_req, int offset_in_buf)
{
    struct timeval pull_start_time, pull_end_time;
    if(DEBUG >= 2)	gettimeofday(&pull_start_time, NULL);

    glex_ret_t glex_ret;
    struct glex_rdma_req glex_req, *bad_glex_req;
    uint32_t cqe_num;
    struct glex_cqe cqe[4];

    glex_req.local_mh.v 	= cli_rdma.mh.v;
    glex_req.local_offset	= offset_in_buf;
    glex_req.len			= rdma_req->len;
    glex_req.rmt_mh.v		= rdma_req->mh.v;
    glex_req.rmt_offset 	= rdma_req->offset;
    glex_req.type			= GLEX_RDMA_TYPE_READ;	/* GLEX_RDMA_TYPE_READ */
    glex_req.rmt_key		= cli_rdma.ep_attr.key;
    glex_req.flag			= GLEX_FLAG_SIGNALED;
    glex_req.next			= NULL;

    //pthread_mutex_lock(&mem_blk->endpoint->trans_mutex);
    while ((glex_ret = glex_rdma(cli_rdma.ep, cli_rdma.ep_addr, &glex_req, &bad_glex_req)) == GLEX_BUSY) {	}
    if (glex_ret != GLEX_SUCCESS)
    {
        fprintf(stderr,"error: _rdma(), return: %d\n", glex_ret);
        exit(1);
    }

    do
    {
        cqe_num = 1;
        glex_poll_cq(cli_rdma.ep, &cqe_num, cqe);
    }
    while (cqe_num < 1);
    //pthread_mutex_unlock(&mem_blk->endpoint->trans_mutex);

    if(DEBUG >= 2)	gettimeofday(&pull_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ rdma time: %lu\n", 1000000 * ( pull_end_time.tv_sec - pull_start_time.tv_sec ) + pull_end_time.tv_usec - pull_start_time.tv_usec);

}

static int last_assign_mem_blk = -1;
static rdma_mem_block* get_mem_block()
{
    rdma_mem_block* ret = NULL;
    pthread_mutex_lock(&cli_mem_blocks_mutex);

    if(num_free_cli_rdma_mem_block == 0)
    {
        ret = NULL;
    }
    else
    {
        int count, cur_blk;
        for(count=0; count<MAX_NUM_CLI_RDMA_MEM_BLOCK; count++)
        {
            cur_blk = (last_assign_mem_blk + 1 + count) % MAX_NUM_CLI_RDMA_MEM_BLOCK;
            if(cli_mem_blocks[cur_blk].is_free)
            {
                ret = &cli_mem_blocks[cur_blk];
                cli_mem_blocks[cur_blk].is_free = 0;
                num_free_cli_rdma_mem_block --;
                last_assign_mem_blk = cur_blk;
                break;
            }
        }
    }
    pthread_mutex_unlock(&cli_mem_blocks_mutex);

    return ret;
}

static void release_mem_block(int mem_block_id)
{
    pthread_mutex_lock(&cli_mem_blocks_mutex);
    assert(cli_mem_blocks[mem_block_id].is_free == 0);
    cli_mem_blocks[mem_block_id].is_free = 1;
    num_free_cli_rdma_mem_block ++;
    if(num_free_cli_rdma_mem_block == 1)
    {
        //pthread_cond_signal(&srv_mem_blocks_cond);
    }
    pthread_mutex_unlock(&cli_mem_blocks_mutex);
}


/********************** Metadata Caching Start *******************************/
static SHF * metadata_caching_shf;
static void metadata_caching_init()
{

    char metadata_caching_file_folder[] = "/dev/shm";
    char metadata_caching_file_name[256];

    pid_t pid = getpid();
    SHF_SNPRINTF(1, metadata_caching_file_name, "Metadata_Caching", pid);
    shf_init  ();
    metadata_caching_shf = shf_attach_existing(metadata_caching_file_folder, metadata_caching_file_name);
}

static void metadata_caching_finalize()
{
    shf_detach(metadata_caching_shf);
}


static char key_buf[PATH_MAX + 8];
static void put_entry_metadata_cache(char * path, int file_block_id, MetadataCachingEntry *entry)
{
    memset(key_buf, 0, PATH_MAX + 8);
    strcpy(key_buf, path);
    memcpy(key_buf + PATH_MAX, &file_block_id, sizeof(file_block_id));

    shf_make_hash(key_buf, PATH_MAX + 8);
    shf_put_key_val(metadata_caching_shf, SHF_CAST(const char *, entry), sizeof(MetadataCachingEntry));
}

static MetadataCachingEntry * get_entry_metadata_cache(char * path, int file_block_id)
{
    memset(key_buf, 0, PATH_MAX + 8);
    strcpy(key_buf, path);
    memcpy(key_buf + PATH_MAX, &file_block_id, sizeof(file_block_id));

    shf_make_hash(key_buf, PATH_MAX + 8);
    MetadataCachingEntry *entry = shf_get_key_val_addr(metadata_caching_shf);

    if(entry == NULL)
    {
        //fprintf(stderr, "Error: Can not find the metadata of %s, file_block[%d] in cache\n", path, file_block_id);
        return NULL;
    }
    else
        return entry;
}

static void del_entry_metadata_cache(char * path, int file_block_id)
{
    memset(key_buf, 0, PATH_MAX + 8);
    strcpy(key_buf, path);
    memcpy(key_buf + PATH_MAX, &file_block_id, sizeof(file_block_id));

    shf_make_hash(key_buf, PATH_MAX + 8);
    shf_del_key_val(metadata_caching_shf);
}



/********************** Metadata Caching End *******************************/



/********************** Config File Start *******************************/


static void digest_config_pair(struct confread_pair *pair)
{
    char * key = pair->key;
    char * value = pair->value;

    if(strcmp(key, "server debug level") == 0)
    {
        config_param.server_debug_level = atoi(value);
    }
    else if(strcmp(key, "client debug level") == 0)
    {
        config_param.client_debug_level = atoi(value);
    }
    else if(strcmp(key, "ion_cn_ration") == 0)
    {
        config_param.ion_cn_ratio = atoi(value) + 1;	// need +1 to create the right interval
    }
    else if(strcmp(key, "ion_with_hash") == 0)
    {
		if(strcmp(value, "on") == 0)
        	config_param.ion_with_hash = 1;	
		else
			config_param.ion_with_hash = 0;
    }
    else if(strcmp(key, "metadata management") == 0)
    {
        if(strcmp(value, "hash") == 0)
            config_param.metadata_hash = 1;
        else
            config_param.metadata_hash = 0;
    }
    else if(strcmp(key, "metadata caching") == 0)
    {
        if(strcmp(value, "on") == 0)
            config_param.metadata_caching = 1;
        else
            config_param.metadata_caching = 0;
    }
    else if(strcmp(key, "cache device") == 0)
    {
        if(strcmp(value, "tmpfs") == 0)
            config_param.cache_device_tmpfs = 1;
        else
            config_param.cache_device_tmpfs = 0;
    }
    else if(strcmp(key, "tmpfs path prefix") == 0)
    {
        strcpy(config_param.tmpfs_path_prefix, value);
    }
    else if(strcmp(key, "tmpfs path suffix") == 0)
    {
        strcpy(config_param.tmpfs_path_suffix, value);
    }
    else if(strcmp(key, "ssd path") == 0)
    {
        strcpy(config_param.ssd_path, value);
    }
    else if(strcmp(key, "simulate ssd with tmpfs") == 0)
    {
        if(strcmp(value, "on") == 0)
            config_param.simulate_ssd = 1;
        else
            config_param.simulate_ssd = 0;
    }
    else if(strcmp(key, "simulate read latency") == 0)
    {
        config_param.simulate_read_latency = atof(value);
    }
    else if(strcmp(key, "simulate write latency") == 0)
    {
        config_param.simulate_write_latency = atof(value);
    }
    else if(strcmp(key, "LASIOD") == 0)
    {
        if(strcmp(value, "on") == 0)
            config_param.LASIOD = 1;
        else
            config_param.LASIOD = 0;
    }
    else if(strcmp(key, "LASIOD_SCATTER_NUM") == 0)
    {
        config_param.LASIOD_SCATTER_NUM = atoi(value);
    }
    else if(strcmp(key, "LASIOD_SMALL_IO_SIZE_KB") == 0)
    {
        config_param.LASIOD_SMALL_IO_SIZE = atoi(value) * 1024;
    }
    else if(strcmp(key, "LASIOD_LARGE_IO_SIZE_MB") == 0)
    {
        config_param.LASIOD_LARGE_IO_SIZE = atoi(value) * 1024 * 1024;
    }
    else if(strcmp(key, "SRV_RDMA_EP_NUM") == 0)
    {
        config_param.SRV_RDMA_EP_NUM = atoi(value);
    }
    else if(strcmp(key, "SRV_MEM_BLK_PER_EP") == 0)
    {
        config_param.SRV_MEM_BLK_PER_EP = atoi(value);
    }
    else
    {
        fprintf(stderr, "Config File Error: Unknown Key-Value Pair <%s, %s>!\n", key, value);
        exit(1);
    }
}

static void read_config_file()
{
    char *path = CONFIG_PARAM_PATH;

    struct confread_file *configFile;
    struct confread_section *thisSect = NULL;
    struct confread_pair *thisPair = NULL;

    if (!(configFile = confread_open(path)))
    {
        fprintf(stderr, "Config open failed\n");
        exit(1);
    }

    thisSect = configFile->sections;
    while(thisSect)
    {
        thisPair = thisSect->pairs;
        while (thisPair)
        {
            digest_config_pair(thisPair);
            thisPair = thisPair->next;
        }
        thisSect = thisSect->next;
    }

    confread_close(&configFile);

}

static int determine_ion_id(char *absolute_path, int my_server_id)
{
	// file based io forwarding
	if(config_param.ion_with_hash)
	{
    	uint64_t hv = hash(absolute_path, strlen(absolute_path) + 1);		
    	hv = hv % G_ION_NUM;
    	return hv * config_param.ion_cn_ratio;
	}
	// position based io forwarding
	else
	{
		return my_server_id - my_server_id % config_param.ion_cn_ratio;
	}
}


/********************** Config File End *******************************/



/********************************************************************/
/**********************   DARSHAN       ********************************/

int hack_open(char *path, int flags, ...)
{
    if(main_init() == -1)
    {
        return -1;
    }

    int mode = 0;
    int ret;
    int para_num;
    if (flags & O_CREAT)
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
        para_num = 3;
    }
    else
    {
        para_num = 2;
    }

    if(DEBUG >= 2)	gettimeofday(&start_time, NULL);

    char absolute_path[PATH_MAX] = {0};
    if(path[0] != '/')
    {
        getcwd(absolute_path, sizeof(absolute_path));
        strncat(absolute_path, "/", 1);
        strncat(absolute_path, path, strlen(path));
    }
    else
    {
        snprintf(absolute_path, strlen(path) + 1, "%s", path);
    }

    conn_cli* c;
    char *s;
    /*if the file path is in the exclusions, just excute the normal opreation*/
    int i;
    for(i = 0; (s = inclusions[i]) != NULL; i++)
    {
        if(!(strncmp(s, absolute_path, strlen(s))))
            break;
    }
    if(!s)
    {
        MAP_OR_FAIL(open);
        if(para_num == 2)
            return __real_open(path, flags);
        else
            return __real_open(path, flags, mode);
    }

    if(DEBUG) fprintf(stderr, "### hack open(%s)\n", absolute_path);

	int server_id;
	server_id = determine_ion_id(absolute_path, MY_SERVER_ID);
    char * server_name = G_SERVER_CONNS[server_id].name;

    // JAY: establish new coneections for every newly open file?
    // YES! It's convenient for different I/O processes to access data seperately.

    /*second: new a connection, TCP or UDP*/
    if(G_SERVER_CONNS[server_id].conn == NULL)
        G_SERVER_CONNS[server_id].conn = cli_socket(server_name, server_port, tcp_transport);		// JAY: different io thread share the same port? YES! cause server listen only at that port
    c = G_SERVER_CONNS[server_id].conn;
    if(c == NULL)
    {
        fprintf(stderr,"cli_socket for open failure because the return value is null\n");
        return -1;
    }
    G_SERVER_CONNS[server_id].sfd = c->sfd;

    /*third: send data to server*/

    int num_para = para_num;
    void *para_addr_array[num_para];
    uint64_t para_len_array[num_para];
    para_addr_array[0] = absolute_path;
    para_len_array[0] = strlen(absolute_path) + 1;
    para_addr_array[1] = &flags;
    para_len_array[1] = sizeof(flags);
    if(para_num == 3)
    {
        para_addr_array[2] = &mode;
        para_len_array[2] = sizeof(mode);
    }
    c = send_request(server_id, PROTOCOL_BINARY_CMD_OPEN,
                     num_para, para_addr_array, para_len_array);

    if(DEBUG >= 2) fprintf(stderr, "<<< send open cmd to MDS[%d]\n", server_id);

    /*fourth: wait for response, check integrity of the package */
    int resp_ret = cli_must_read_command(c);			// JAY TODO: not thread safe
    if(resp_ret != 0)
    {
        perror("must read command failed");
        return -1;
    }
    protocol_binary_response_header *resp;
    /*fifth: finish this I/O request*/

    if(c->frag_num == 1)
    {
        resp = (protocol_binary_response_header *)c->rbuf;
        if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_OPEN))
        {
            return -1;
        }
        memcpy(&ret, (char*)c->rbuf + sizeof(protocol_binary_response_header), resp->response.para1_len);

    }
    if(c->frag_num > 1)
    {
        resp = (protocol_binary_response_header *)c->assemb_buf;
        if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_OPEN))
        {
            return -1;
        }
        memcpy(&ret, (char*)c->assemb_buf + sizeof(protocol_binary_response_header), resp->response.para1_len);
    }


    if(DEBUG >= 2) fprintf(stderr, ">>> recv open cfd[%d]\n", ret);

    // record the server_name and sfd in the globle array
    fd_map *new_fd_map = calloc(1, sizeof(fd_map));
    new_fd_map->cfd = ret;
    new_fd_map->server_id = server_id;
    new_fd_map->sfd = c->sfd;
    new_fd_map->file_offset = 0;
    strcpy(new_fd_map->server_name, server_name);
    strcpy(new_fd_map->file_name, absolute_path);
    new_fd_map->open_arg.arg_num = para_num;
    new_fd_map->open_arg.flags = flags;
    if(para_num == 3)
        new_fd_map->open_arg.modes = mode;
    else
        new_fd_map->open_arg.modes = 0;

    memcpy(new_fd_map->open_arg.path, new_fd_map->file_name, PATH_MAX);

    new_fd_map->successive_recorder.is_successive = 0;
    new_fd_map->successive_recorder.last_accessed_block_id = -1;
    new_fd_map->successive_recorder.accumulated_data_size = 0;
    new_fd_map->successive_recorder.last_cached_server_id = -1;

    insert_fd_map(new_fd_map);

    // map real fd to cfd
    int real_fd = get_file_real_fd(absolute_path, para_num, flags, mode);
    create_mapping_of_realfd_cfd(real_fd, new_fd_map->cfd);

    if(DEBUG >= 2) gettimeofday(&end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ open time: %lu\n", 1000000 * ( end_time.tv_sec - start_time.tv_sec ) + end_time.tv_usec - start_time.tv_usec);

    return(real_fd);
}

int hack_close(int fd)
{
    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        if(DEBUG) fprintf(stderr, "### hack close()\n");
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_close()
        MAP_OR_FAIL(close);
        return __real_close(fd);
    }

    int cfd = get_mapping_of_realfd(fd);
    fd_map* my_fd_map = get_fd_map(cfd);
    if(my_fd_map == NULL)
    {
        fprintf(stderr, "Can't find this fd, use __real_close() instead\n");
        MAP_OR_FAIL(close);
        return __real_close(fd);
    }

    int num_para = 1;
    void *para_addr_array[num_para];
    uint64_t para_len_array[num_para];
    para_addr_array[0] = &cfd;
    para_len_array[0] = sizeof(cfd);
    conn_cli* c;
    c = send_request(my_fd_map->server_id, PROTOCOL_BINARY_CMD_CLOSE,
                     num_para, para_addr_array, para_len_array);

    int resp_ret = cli_must_read_command(c);
    if(resp_ret != 0)
    {
        perror("must read command failed");
        return -1;
    }
    protocol_binary_response_header *resp;
    int ret;
    if(c->frag_num == 1)
    {
        resp = (protocol_binary_response_header *)c->rbuf;
        if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_CLOSE))
        {
            return -1;
        }
        memcpy(&ret, (char*)c->rbuf + sizeof(protocol_binary_response_header), resp->response.para1_len);

    }
    if(c->frag_num > 1)
    {
        resp = (protocol_binary_response_header *)c->assemb_buf;
        if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_CLOSE))
        {
            return -1;
        }
        memcpy(&ret, (char*)c->assemb_buf + sizeof(protocol_binary_response_header), resp->response.para1_len);
    }

    // clean fd_map

    // do we need close this sfd? NO!
    // This is the sfd we connecte to OST in that server too!

    // clean real fd mapping
    MAP_OR_FAIL(close);
    __real_close(fd);
    clean_mapping_of_realfd(fd);

    return 0;
}

off64_t hack_lseek(int fd, off64_t offset, int whence)
{
    // lseek only send to OST on this server, because lseek need open a real file, and who initiate lseek is most likely to accese the file

    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        if(DEBUG) fprintf(stderr, "### hack lseek()\n");
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_close()
        MAP_OR_FAIL(lseek);
        return __real_lseek(fd, offset, whence);
    }


    int cfd = get_mapping_of_realfd(fd);
    fd_map* my_fd_map = get_fd_map(cfd);
    if(my_fd_map == NULL)
    {
        fprintf(stderr, "Can't find this fd, use __real_lseek() instead\n");
        MAP_OR_FAIL(lseek);
        return __real_lseek(fd, offset, whence);
    }

    uint64_t curr_offset = my_fd_map->file_offset;

    switch(whence)
    {
    case SEEK_SET:
        if(offset >= 0)
        {
            my_fd_map->file_offset = offset;
            return 0;
        }
        else
        {
            printf("illegal offset for SEEK_SET\n");
            return -1;
        }
    case SEEK_CUR:
        curr_offset += offset;
        if(curr_offset >= 0)
        {
            my_fd_map->file_offset = curr_offset;
            return 0;
        }
        else
        {
            printf("illegal offset for SEEK_CUR\n");
            return -1;
        }
    case SEEK_END:
        // need communicate with mds
        break;
    default:
        printf("illegal whence\n");
        return -1;
    }

    int num_para = 4;
    void *para_addr_array[5];
    uint64_t para_len_array[5];
    para_addr_array[0] = &cfd;
    para_len_array[0] = sizeof(cfd);
    para_addr_array[1] = &offset;
    para_len_array[1] = sizeof(offset);
    para_addr_array[2] = &whence;
    para_len_array[2] = sizeof(whence);
    para_addr_array[3] = &curr_offset;
    para_len_array[3] = sizeof(curr_offset);
    conn_cli* c = send_request(my_fd_map->server_id, PROTOCOL_BINARY_CMD_LSEEK,
                               num_para, para_addr_array, para_len_array);

    while(1)
    {
        int resp_ret = cli_must_read_command(c);			// JAY TODO: not thread safe
        if(resp_ret != 0)
        {
            perror("must read command failed");
            return -1;
        }
        protocol_binary_response_header *resp;
        if(c->frag_num == 1)
        {
            resp = (protocol_binary_response_header *)c->rbuf;

        }
        if(c->frag_num > 1)
        {
            resp = (protocol_binary_response_header *)c->assemb_buf;
        }

        if((resp->response.magic == PROTOCOL_BINARY_RES) && (resp->response.opcode == PROTOCOL_BINARY_CMD_OST_PATH_REQ))
        {
            ost_open_arg *open_arg = &(my_fd_map->open_arg);
            num_para = 5;
            para_addr_array[4] = open_arg;
            para_len_array[4] = sizeof(ost_open_arg);

            c = send_request(my_fd_map->server_id, PROTOCOL_BINARY_CMD_LSEEK,
                             num_para, para_addr_array, para_len_array);
            continue;

        }
        else
        {
            if((resp->response.magic != PROTOCOL_BINARY_RES) || (resp->response.opcode != PROTOCOL_BINARY_CMD_LSEEK))
            {
                fprintf(stderr, "Message failure, magic or opcode is wrong\n");
                return -1;
            }
            uint64_t ret;
            memcpy(&ret, (char*)resp + sizeof(protocol_binary_response_header), resp->response.para1_len);
            my_fd_map->file_offset = ret;
            break;
        }
    }


    return(my_fd_map->file_offset);
}


ssize_t hack_read(int fd, void *buf, size_t count)
{
    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        if(DEBUG) fprintf(stderr, "### hack read()\n");
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_func()
        MAP_OR_FAIL(read);
        return __real_read(fd, buf, count);
    }

    size_t ret = 0;
    int sfd;
    int connected;
    long curr_offset;
    conn_cli* c;
    int k = 0;
    char *s;

    int cfd = get_mapping_of_realfd(fd);;
    fd_map* my_fd_map = get_fd_map(cfd);

    /*if the file path is in the exclusions, just excute the normal opreation*/
    for(k = 0; (s = inclusions[k]) != NULL; k++)
    {
        if(!(strncmp(s, my_fd_map->file_name, strlen(s))))
            break;
    }
    if(!s)
    {
        MAP_OR_FAIL(read);
        return __real_read(fd, buf, count);
    }

    int all_already_read_bytes = 0;
    int cur_blk;
    int cur_ost;

    /*we do not intercept ftell, so do not to mapping ftell*/
    curr_offset = my_fd_map->file_offset;
    int start_block_index = curr_offset >> G_BLOCK_SIZE_SHIFT;
    int end_block_index = (curr_offset + count - 1) >> G_BLOCK_SIZE_SHIFT;
    int first_block_start_offset = curr_offset % G_BLOCK_SIZE_IN_BYTES;
    int last_block_end_offset = (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES;
    int num_block_metadata = end_block_index - start_block_index + 1;

    int block_metadata[num_block_metadata];	
    MetadataLocalCachedInfo meta_cache_info[num_block_metadata];
    memset(meta_cache_info, 0, sizeof(MetadataLocalCachedInfo) * num_block_metadata);
    int bytes_block_metadata = retrieve_block_metadata(my_fd_map, cfd, curr_offset, count, block_metadata, meta_cache_info, 1);

    int num_blocks_read_locally = 0;


    // have got the block metadata array
    // now read the data from each ost by nonblocking socket (func: select)

    // change 1,1,1,2,2,2 to 1,2,1,2,1,2, this can be more efficient
    scatter_block_info scatter_blocks[num_block_metadata];
    scatter_block_metadata(scatter_blocks, block_metadata, num_block_metadata, start_block_index, meta_cache_info);


    // go to server to retrieve data

    int num_blocks_access_remotely = num_block_metadata - num_blocks_read_locally;

    int num_block_reqs_of_ost[G_SERVER_NUM];
    int num_req_osts = 0;
    memset(num_block_reqs_of_ost, 0, sizeof(int) * G_SERVER_NUM);
    // initialize the requested osts
    for(cur_blk=0; cur_blk<num_block_metadata; cur_blk++)
    {
        if(scatter_blocks[cur_blk].need_access_remotely)
        {
            cur_ost = scatter_blocks[cur_blk].ost_id;
            if(num_block_reqs_of_ost[cur_ost] == 0)
                num_req_osts ++;
            num_block_reqs_of_ost[cur_ost]++;
        }
    }


    struct timeval sndreq_start_time, sndreq_end_time;
    if(DEBUG >= 2)	gettimeofday(&sndreq_start_time, NULL);

    int num_send_block_reqs = 0;
    int last_send_scatter_block_id = -1;
    // first send NUM_RDMA_MEM_BLK_PER_PROC of block reqs to different ost
    int num_first_batch_reqs = num_req_osts * 10;
    num_first_batch_reqs = num_block_metadata < num_first_batch_reqs ?
                           num_block_metadata : num_first_batch_reqs;
    // TODO: need verify that sending all reqs out is a good way
    //for(cur_blk=0; cur_blk<num_first_batch_reqs; cur_blk++)
    for(cur_blk=0; cur_blk<num_block_metadata; cur_blk++)
    {
        if(scatter_blocks[cur_blk].need_access_remotely == 0) continue;

        int start_offset = scatter_blocks[cur_blk].file_block_id == start_block_index ?
                           curr_offset % G_BLOCK_SIZE_IN_BYTES : 0;
        int end_offset = scatter_blocks[cur_blk].file_block_id == end_block_index ?
                         (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES : G_BLOCK_SIZE_IN_BYTES - 1;
        send_read_request_to_ost(cfd, &scatter_blocks[cur_blk], start_offset, end_offset);
        num_send_block_reqs++;
        last_send_scatter_block_id = cur_blk;
    }

    if(DEBUG >= 2)	gettimeofday(&sndreq_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ sndreq time: %lu\n", 1000000 * ( sndreq_end_time.tv_sec - sndreq_start_time.tv_sec ) + sndreq_end_time.tv_usec - sndreq_start_time.tv_usec);

    struct epoll_event event, events[num_req_osts];
    int epollfd = epoll_create(num_req_osts);	// maximum number of epoll fd is the number of the requested osts
    if(epollfd < 0) fprintf(stderr, "epoll_create error!\n");
    int estimate_max_rbuf = num_block_metadata * (sizeof(protocol_binary_response_header) + sizeof(block_data_req) + sizeof(rdma_data_req));
    for(cur_ost = 0; cur_ost<G_SERVER_NUM; cur_ost++)
    {
        if(num_block_reqs_of_ost[cur_ost] == 0)
            continue;

        // add the event
        bzero(&event, sizeof(event));
        event.events = EPOLLIN;
        event.data.u32 = cur_ost;
        if(epoll_ctl(epollfd, EPOLL_CTL_ADD, G_SERVER_CONNS[cur_ost].sfd, &event) < 0)
            fprintf(stderr, "epoll_ctl error!\n");

        // prepare the rbuf of conn
        if(G_SERVER_CONNS[cur_ost].conn->rsize < estimate_max_rbuf)
        {
            G_SERVER_CONNS[cur_ost].conn->rbuf = realloc(G_SERVER_CONNS[cur_ost].conn->rbuf, estimate_max_rbuf);
            G_SERVER_CONNS[cur_ost].conn->rsize = estimate_max_rbuf;
            G_SERVER_CONNS[cur_ost].conn->rcurr = G_SERVER_CONNS[cur_ost].conn->rbuf;
        }
    }

    register_rdma_buf(buf, count);

    struct timeval epoll_start_time, epoll_end_time;
    if(DEBUG >= 2) gettimeofday(&epoll_start_time, NULL);


    struct timeval wait_start_time, wait_end_time;
    if(DEBUG >= 2) gettimeofday(&wait_start_time, NULL);
    size_t total_wait_time = 0;

    int epoll_ret;
    int num_ack_block = 0;
    while(1)
    {
        epoll_ret = epoll_wait(epollfd, events, num_req_osts, -1);
        if(epoll_ret < 0)
        {
            fprintf(stderr, "epoll_wait error!\n");
            exit(0);
        }

        if(DEBUG >= 2) gettimeofday(&wait_end_time, NULL);
        if(DEBUG >= 2) fprintf(stderr, "~~~ wait time: %lu\n", 1000000 * ( wait_end_time.tv_sec - wait_start_time.tv_sec ) + wait_end_time.tv_usec - wait_start_time.tv_usec);
        total_wait_time += 1000000 * ( wait_end_time.tv_sec - wait_start_time.tv_sec ) + wait_end_time.tv_usec - wait_start_time.tv_usec;

        //if(DEBUG) fprintf(stderr, "epoll_wait get %d events\n", epoll_ret);
        int cur_evt;
        for(cur_evt=0; cur_evt<epoll_ret; cur_evt++)
        {
            if ((events[cur_evt].events & EPOLLERR) || (events[cur_evt].events & EPOLLHUP) || (!(events[cur_evt].events & EPOLLIN)))
            {
                //An error has occured on this fd, or the socket is not ready for reading (why were we notified then?)
                fprintf (stderr, "epoll error\n");
                exit(1);
            }
            if(events[cur_evt].events & EPOLLIN)
            {
                int epoll_read_ret;
                cur_ost = events[cur_evt].data.u32;
                epoll_read_ret = cli_try_read_command(G_SERVER_CONNS[cur_ost].conn);
                if(epoll_read_ret != 0)
                {
                    fprintf(stderr, "cli_try_read_command failed in epoll_wait\n");
                    return 0;
                }
                if(DEBUG >= 2) fprintf(stderr, "conn read %d bytes\n", G_SERVER_CONNS[cur_ost].conn->rbytes);

                char tmp_rbuf[G_SERVER_CONNS[cur_ost].conn->rbytes];
                if(G_SERVER_CONNS[cur_ost].conn->frag_num == 1)
                {
                    memcpy(tmp_rbuf, G_SERVER_CONNS[cur_ost].conn->rbuf, G_SERVER_CONNS[cur_ost].conn->rbytes);
                }
                else if(G_SERVER_CONNS[cur_ost].conn->frag_num > 1)
                {
                    memcpy(tmp_rbuf, G_SERVER_CONNS[cur_ost].conn->assemb_buf, G_SERVER_CONNS[cur_ost].conn->rbytes);
                }

                // cli_try_read_command() may read more than one response
                // because in our case, OST may send multiple responses back by only one sfd, so they would be received accumulatively
                int tmp_rbuf_len = G_SERVER_CONNS[cur_ost].conn->rbytes;
                int handled_bytes = 0;
                while(handled_bytes < tmp_rbuf_len)
                {
                    protocol_binary_response_header *ost_resp;
                    char *recv_data_buf;
                    int *block_data;
                    int bytes_block_data;
                    ost_resp = (protocol_binary_response_header *)(tmp_rbuf + handled_bytes);
                    if(handled_bytes != 0)	// not the first msghdr in rbuf
                    {
                        response_network_to_host(ost_resp);
                    }
                    handled_bytes += sizeof(protocol_binary_response_header);

                    // need resend the cmd
                    if((ost_resp->response.magic == PROTOCOL_BINARY_RES) && (ost_resp->response.opcode == PROTOCOL_BINARY_CMD_OST_PATH_REQ))
                    {
                        // add a ost_open_arg param, and resend cmd
                        ost_open_arg *open_arg = &(my_fd_map->open_arg);
                        block_data_req *re_blk_req = (block_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(block_data_req);

                        if(DEBUG >= 2) fprintf(stderr, ">>> send PATH for block[%d] to OST[%d]\n", re_blk_req->file_block_id, cur_ost);

                        // 2 params: block_data_req, open_arg
                        int num_para = 2;
                        void *para_addr_array[num_para];
                        uint64_t para_len_array[num_para];
                        para_addr_array[0] = re_blk_req;
                        para_len_array[0] = sizeof(block_data_req);
                        para_addr_array[1] = open_arg;
                        para_len_array[1] = sizeof(ost_open_arg);
                        G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_READ_OST,
                                                       num_para, para_addr_array, para_len_array);
                        // this will go to the end of for()
                    }
                    // no free mem_blk, resend this cmd
                    else if((ost_resp->response.magic == PROTOCOL_BINARY_RES) && (ost_resp->response.opcode == PROTOCOL_BINARY_CMD_OST_NO_FREE_MEM_BLK))
                    {
                        // add a ost_open_arg param, and resend cmd
                        block_data_req *re_blk_req = (block_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(block_data_req);

                        if(DEBUG >= 2) fprintf(stderr, ">>> RESEND req of block[%d] to OST[%d]\n", re_blk_req->file_block_id, cur_ost);

                        // 2 params: block_data_req, open_arg
                        int num_para = 1;
                        void *para_addr_array[num_para];
                        uint64_t para_len_array[num_para];
                        para_addr_array[0] = re_blk_req;
                        para_len_array[0] = sizeof(block_data_req);
                        G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_READ_OST,
                                                       num_para, para_addr_array, para_len_array);
                    }
                    // have retrieved the required data
                    else
                    {

                        if((ost_resp->response.magic != PROTOCOL_BINARY_RES) || (ost_resp->response.opcode != PROTOCOL_BINARY_CMD_RDMA_READ_OST))
                        {
                            fprintf(stderr, "Message failure, magic or opcode is wrong\n");
                            return 0;
                        }

                        assert(ost_resp->response.para1_len == sizeof(block_data_req));
                        assert(ost_resp->response.para2_len == sizeof(rdma_data_req));
                        block_data_req *recv_blk_req = (block_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(block_data_req);
                        rdma_data_req *recv_rdma_req = (rdma_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(rdma_data_req);


                        int offset_in_buf = recv_blk_req->file_block_id == start_block_index ?
                                            0 : G_BLOCK_SIZE_IN_BYTES - first_block_start_offset + G_BLOCK_SIZE_IN_BYTES * (recv_blk_req->file_block_id - start_block_index - 1);
                        pull_rdma_buf(recv_rdma_req, offset_in_buf);

                        all_already_read_bytes += recv_rdma_req->len;

                        if(DEBUG >= 2) fprintf(stderr, ">>> read %d bytes of block[%d] from OST[%d]\n", recv_rdma_req->len, recv_blk_req->file_block_id, cur_ost);

                        // send back ack to release occupied mem_blk in srv
                        // 1 params: mem_blk_id
                        int num_para = 1;
                        void *para_addr_array[num_para];
                        uint64_t para_len_array[num_para];
                        para_addr_array[0] = &(recv_rdma_req->mem_blk_id);
                        para_len_array[0] = sizeof(recv_rdma_req->mem_blk_id);
                        G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_READ_OST_COMPLT,
                                                       num_para, para_addr_array, para_len_array);

                        if(num_send_block_reqs < num_blocks_access_remotely)
                        {
                            // need send these left write reqs
                            int next_send_blk = -1;
                            int i;
                            for(i=last_send_scatter_block_id + 1; i<num_block_metadata; i++)
                            {
                                if(scatter_blocks[i].need_access_remotely)
                                {
                                    next_send_blk = i;
                                    last_send_scatter_block_id = i;
                                }
                            }
                            assert(next_send_blk >= 0);

                            int start_offset = scatter_blocks[next_send_blk].file_block_id == start_block_index ?
                                               curr_offset % G_BLOCK_SIZE_IN_BYTES : 0;
                            int end_offset = scatter_blocks[next_send_blk].file_block_id == end_block_index ?
                                             (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES : G_BLOCK_SIZE_IN_BYTES - 1;
                            send_read_request_to_ost(cfd, &scatter_blocks[next_send_blk], start_offset, end_offset);
                            num_send_block_reqs++;
                        }

                        num_ack_block ++;


                    }
                }
            }
        }

        // all blocks are retrieved
        if(num_ack_block == num_blocks_access_remotely)
        {
            break;
        }

        if(DEBUG >= 2) gettimeofday(&wait_start_time, NULL);
    }


    // clean up
    MAP_OR_FAIL(close);
    __real_close(epollfd);


    assert(all_already_read_bytes == count);

    if(DEBUG >= 2) fprintf(stderr, "~~~ total wait time: %lu\n", total_wait_time);

    if(DEBUG >= 2) gettimeofday(&epoll_end_time, NULL);
    if(DEBUG >= 2) fprintf(stderr, "~~~ ost time: %lu\n", 1000000 * ( epoll_end_time.tv_sec - epoll_start_time.tv_sec ) + epoll_end_time.tv_usec - epoll_start_time.tv_usec);

// END
    deregister_rdma_buf();
    my_fd_map->file_offset += all_already_read_bytes;
    return(all_already_read_bytes);
}

ssize_t hack_write(int fd, const void *buf, size_t count)
{
    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        if(DEBUG) fprintf(stderr, "### hack write()\n");
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_func()
        MAP_OR_FAIL(write);
        return __real_write(fd, buf, count);
    }

    size_t ret = 0;

    int sfd;
    int connected;
    int process_pid;
    long curr_offset;
    conn_cli* c;
    int path_look = 0;
    char *s;

    int cfd = get_mapping_of_realfd(fd);
    fd_map* my_fd_map = get_fd_map(cfd);

    for(path_look = 0; (s = inclusions[path_look]) != NULL; path_look++)
    {
        if(!(strncmp(s, my_fd_map->file_name, strlen(s))))
            break;
    }
    if(!s)
    {
        MAP_OR_FAIL(write);
        return __real_write(fd, buf, count);
    }


    int all_already_read_bytes = 0;
    int cur_blk;
    int cur_ost;


    curr_offset = my_fd_map->file_offset;
    int start_block_index = curr_offset >> G_BLOCK_SIZE_SHIFT;
    int end_block_index = (curr_offset + count - 1) >> G_BLOCK_SIZE_SHIFT;
    int first_block_start_offset = curr_offset % G_BLOCK_SIZE_IN_BYTES;
    int last_block_end_offset = (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES;
    int num_block_metadata = end_block_index - start_block_index + 1;

    int block_metadata[num_block_metadata];	// calloc inside retrieve_block_metadat(), need to free when finished
    MetadataLocalCachedInfo meta_cache_info[num_block_metadata];
    memset(meta_cache_info, 0, sizeof(MetadataLocalCachedInfo) * num_block_metadata);
    int bytes_block_metadata = retrieve_block_metadata(my_fd_map, cfd, curr_offset, count, block_metadata, meta_cache_info, 0);

    int num_blocks_read_locally = 0;



    // change 1,1,1,2,2,2 to 1,2,1,2,1,2, this can be more efficient
    scatter_block_info scatter_blocks[num_block_metadata];
    scatter_block_metadata(scatter_blocks, block_metadata, num_block_metadata, start_block_index, meta_cache_info);


    int num_blocks_access_remotely = num_block_metadata - num_blocks_read_locally;

    int num_block_reqs_of_ost[G_SERVER_NUM];
    int num_req_osts = 0;
    memset(num_block_reqs_of_ost, 0, sizeof(int) * G_SERVER_NUM);

    // initialize the requested osts
    for(cur_blk=0; cur_blk<num_block_metadata; cur_blk++)
    {
        if(scatter_blocks[cur_blk].need_access_remotely)
        {
            cur_ost = scatter_blocks[cur_blk].ost_id;
            if(num_block_reqs_of_ost[cur_ost] == 0)
                num_req_osts ++;
            num_block_reqs_of_ost[cur_ost]++;
        }
    }


    register_rdma_buf(buf, count);

    int num_send_block_reqs = 0;
    int last_send_scatter_block_id = -1;
    // first send NUM_RDMA_MEM_BLK_PER_PROC of block reqs to different ost
    int num_first_batch_reqs = num_block_metadata < 8 ?
                               num_block_metadata : 8;
    // TODO: need verify that sending all reqs out is a good way
    //for(cur_blk=0; cur_blk<num_first_batch_reqs; cur_blk++)
    for(cur_blk=0; cur_blk<num_block_metadata; cur_blk++)
    {
        if(scatter_blocks[cur_blk].need_access_remotely == 0) continue;

        int start_offset = scatter_blocks[cur_blk].file_block_id == start_block_index ?
                           curr_offset % G_BLOCK_SIZE_IN_BYTES : 0;
        int end_offset = scatter_blocks[cur_blk].file_block_id == end_block_index ?
                         (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES : G_BLOCK_SIZE_IN_BYTES - 1;
        int offset_in_buf = scatter_blocks[cur_blk].file_block_id == start_block_index ?
                            0 : G_BLOCK_SIZE_IN_BYTES - first_block_start_offset + G_BLOCK_SIZE_IN_BYTES * (scatter_blocks[cur_blk].file_block_id - start_block_index - 1);
        send_write_request_to_ost(cfd, &scatter_blocks[cur_blk], offset_in_buf, start_offset, end_offset);

        num_send_block_reqs++;
        last_send_scatter_block_id = cur_blk;
    }

    struct epoll_event event, events[num_req_osts];
    int epollfd = epoll_create(num_req_osts);	// maximum number of epoll fd is the number of the requested osts
    if(epollfd < 0) fprintf(stderr, "epoll_create error!\n");
    int estimate_max_rbuf = (sizeof(protocol_binary_response_header) + sizeof(block_data_req) + sizeof(rdma_data_req)) * num_block_metadata;
    for(cur_ost = 0; cur_ost<G_SERVER_NUM; cur_ost++)
    {
        if(num_block_reqs_of_ost[cur_ost] == 0)
            continue;

        // add the event
        bzero(&event, sizeof(event));
        event.events = EPOLLIN;
        event.data.u32 = cur_ost;
        if(epoll_ctl(epollfd, EPOLL_CTL_ADD, G_SERVER_CONNS[cur_ost].sfd, &event) < 0)
            fprintf(stderr, "epoll_ctl error!\n");

        // prepare the rbuf of conn
        if(G_SERVER_CONNS[cur_ost].conn->rsize < estimate_max_rbuf)
        {
            G_SERVER_CONNS[cur_ost].conn->rbuf = realloc(G_SERVER_CONNS[cur_ost].conn->rbuf, estimate_max_rbuf);
            G_SERVER_CONNS[cur_ost].conn->rsize = estimate_max_rbuf;
            G_SERVER_CONNS[cur_ost].conn->rcurr = G_SERVER_CONNS[cur_ost].conn->rbuf;
        }
    }

    if(DEBUG >= 2) gettimeofday(&start_time, NULL);

    int epoll_ret;
    int all_already_write_bytes = 0;
    int num_ack_block = 0;
    while(1)
    {
        epoll_ret = epoll_wait(epollfd, events, num_req_osts, -1);
        if(epoll_ret < 0)
        {
            fprintf(stderr, "epoll_wait error!\n");
            exit(0);
        }

        //if(DEBUG >= 2) fprintf(stderr, "epoll_wait get %d events\n", epoll_ret);
        int cur_evt;
        for(cur_evt=0; cur_evt<epoll_ret; cur_evt++)
        {
            if ((events[cur_evt].events & EPOLLERR) || (events[cur_evt].events & EPOLLHUP) || (!(events[cur_evt].events & EPOLLIN)))
            {
                //An error has occured on this fd, or the socket is not ready for reading (why were we notified then?)
                fprintf (stderr, "epoll error\n");
                continue;
            }
            if(events[cur_evt].events & EPOLLIN)
            {
                //if(DEBUG >= 2) fprintf(stderr, "OST[%d] EPOLLIN\n", events[cur_evt].data.u32);

                int epoll_read_ret;
                cur_ost = events[cur_evt].data.u32;
                epoll_read_ret = cli_try_read_command(G_SERVER_CONNS[cur_ost].conn);
                if(epoll_read_ret != 0)
                {
                    fprintf(stderr, "XXXXXXXXXXXX cli_try_read_command failed in epoll_wait XXXXXXXXXXXXX\n");
                    exit(1);
                }
                if(DEBUG >= 3) fprintf(stderr, "conn read %d bytes\n", G_SERVER_CONNS[cur_ost].conn->rbytes);

                char tmp_rbuf[G_SERVER_CONNS[cur_ost].conn->rbytes];
                if(G_SERVER_CONNS[cur_ost].conn->frag_num == 1)
                {
                    memcpy(tmp_rbuf, G_SERVER_CONNS[cur_ost].conn->rbuf, G_SERVER_CONNS[cur_ost].conn->rbytes);
                }
                else if(G_SERVER_CONNS[cur_ost].conn->frag_num > 1)
                {
                    memcpy(tmp_rbuf, G_SERVER_CONNS[cur_ost].conn->assemb_buf, G_SERVER_CONNS[cur_ost].conn->rbytes);
                }

                // cli_try_read_command() may read more than one response
                // because in our case, OST may send multiple responses back by only one sfd, so they would be received accumulatively
                int tmp_rbuf_len = G_SERVER_CONNS[cur_ost].conn->rbytes;
                int handled_bytes = 0;
                while(handled_bytes < tmp_rbuf_len)
                {
                    protocol_binary_response_header *ost_resp;
                    int *block_data;
                    int bytes_block_data;
                    ost_resp = (protocol_binary_response_header *)(tmp_rbuf + handled_bytes);
                    if(handled_bytes != 0)	// not the first msghdr in rbuf
                    {
                        response_network_to_host(ost_resp);
                    }
                    handled_bytes += sizeof(protocol_binary_response_header);

                    // need resend the cmd
                    if((ost_resp->response.magic == PROTOCOL_BINARY_RES) && (ost_resp->response.opcode == PROTOCOL_BINARY_CMD_OST_PATH_REQ))
                    {
                        assert(ost_resp->response.para_num == 2);
                        // add a ost_open_arg param, and resend cmd
                        ost_open_arg *open_arg = &(my_fd_map->open_arg);
                        block_data_req *re_blk_req = (block_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(block_data_req);
                        rdma_data_req *re_rdma_req = (rdma_data_req*)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(rdma_data_req);

                        if(DEBUG >= 2) fprintf(stderr, ">>> send PATH for block[%d] to OST[%d]\n", re_blk_req->file_block_id, cur_ost);

                        // 3 params: block_data_req, rdma_data_req, open_arg
                        int num_para = 3;
                        void *para_addr_array[num_para];
                        uint64_t para_len_array[num_para];
                        para_addr_array[0] = re_blk_req;
                        para_len_array[0] = sizeof(block_data_req);
                        para_addr_array[1] = re_rdma_req;
                        para_len_array[1] = sizeof(rdma_data_req);
                        para_addr_array[2] = open_arg;
                        para_len_array[2] = sizeof(ost_open_arg);
                        G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_WRITE_OST,
                                                       num_para, para_addr_array, para_len_array);
                        // this will go to the end of for()
                    }
                    // no free mem_blk, resend this cmd
                    else if((ost_resp->response.magic == PROTOCOL_BINARY_RES) && (ost_resp->response.opcode == PROTOCOL_BINARY_CMD_OST_NO_FREE_MEM_BLK))
                    {
                        assert(ost_resp->response.para_num == 2);
                        block_data_req *re_blk_req = (block_data_req *)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(block_data_req);
                        rdma_data_req *re_rdma_req = (rdma_data_req*)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(rdma_data_req);

                        if(DEBUG >= 2) fprintf(stderr, ">>> RESEND req of block[%d] to OST[%d]\n", re_blk_req->file_block_id, cur_ost);

                        // 2 params: block_data_req, rdma_data_req
                        int num_para = 2;
                        void *para_addr_array[num_para];
                        uint64_t para_len_array[num_para];
                        para_addr_array[0] = re_blk_req;
                        para_len_array[0] = sizeof(block_data_req);
                        para_addr_array[1] = re_rdma_req;
                        para_len_array[1] = sizeof(rdma_data_req);
                        G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_WRITE_OST,
                                                       num_para, para_addr_array, para_len_array);
                    }
                    // have retrieved the required data
                    else
                    {
                        if((ost_resp->response.magic != PROTOCOL_BINARY_RES) || (ost_resp->response.opcode != PROTOCOL_BINARY_CMD_RDMA_WRITE_OST))
                        {
                            fprintf(stderr, "Message failure, magic or opcode is wrong\n");
                            return 0;
                        }
                        assert(ost_resp->response.para_num == 3);

                        assert(ost_resp->response.para1_len == sizeof(int));
                        assert(ost_resp->response.para2_len == sizeof(int));
                        assert(ost_resp->response.para3_len == sizeof(int));
                        int write_len = *(int*)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(int);
                        int mem_blk_id = *(int*)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(int);
                        int recv_file_blk_id = *(int*)((char*)tmp_rbuf + handled_bytes);
                        handled_bytes += sizeof(int);
                        //release_mem_block(mem_blk_id);

                        if(DEBUG >= 2) fprintf(stderr, ">>> write %d bytes of block[%d] to OST[%d]\n", write_len, recv_file_blk_id, cur_ost);


                        if(num_send_block_reqs < num_blocks_access_remotely)
                        {
                            // need send these left write reqs
                            int next_send_blk = -1;
                            int i;
                            for(i=last_send_scatter_block_id + 1; i<num_block_metadata; i++)
                            {
                                if(scatter_blocks[i].need_access_remotely)
                                {
                                    next_send_blk = i;
                                    last_send_scatter_block_id = i;
                                }
                            }
                            assert(next_send_blk >= 0);

                            int start_offset = scatter_blocks[next_send_blk].file_block_id == start_block_index ?
                                               curr_offset % G_BLOCK_SIZE_IN_BYTES : 0;
                            int end_offset = scatter_blocks[next_send_blk].file_block_id == end_block_index ?
                                             (curr_offset + count - 1) % G_BLOCK_SIZE_IN_BYTES : G_BLOCK_SIZE_IN_BYTES - 1;
                            int offset_in_buf = scatter_blocks[next_send_blk].file_block_id == start_block_index ?
                                                0 : G_BLOCK_SIZE_IN_BYTES - first_block_start_offset + G_BLOCK_SIZE_IN_BYTES * (scatter_blocks[next_send_blk].file_block_id - start_block_index - 1);
                            send_write_request_to_ost(cfd, &scatter_blocks[next_send_blk], offset_in_buf, start_offset, end_offset);
                            num_send_block_reqs++;
                        }
                        // need modify event?
                        else
                        {
                            // why???
                            // Maybe need send a seudo msg out??
                            G_SERVER_CONNS[cur_ost].conn = send_request(cur_ost, PROTOCOL_BINARY_CMD_RDMA_WRITE_OST_COMPLT,
                                                           0, NULL, NULL);
                        }


                        all_already_write_bytes += write_len;
                        num_ack_block ++;

                    }
                }
            }
        }

        // all blocks are retrieved
        if(num_ack_block == num_blocks_access_remotely)
        {
            break;
        }
    }

    my_fd_map->file_offset += all_already_write_bytes;

    MAP_OR_FAIL(close);
    __real_close(epollfd);
    deregister_rdma_buf();
    return(count);
}

// change all fopen to open
FILE * hack_fopen(const char *path, const char *mode)
{
    char absolute_path[PATH_MAX] = {0};
    if(path[0] != '/')
    {
        getcwd(absolute_path, sizeof(absolute_path));
        strncat(absolute_path, "/", 1);
        strncat(absolute_path, path, strlen(path));
    }
    else
    {
        snprintf(absolute_path, strlen(path) + 1, "%s", path);
    }

    char *s;
    /*if the file path is in the exclusions, just excute the normal opreation*/
    int i;
    for(i = 0; (s = inclusions[i]) != NULL; i++)
    {
        if(!(strncmp(s, absolute_path, strlen(s))))
            break;
    }
    if(!s)
    {
        MAP_OR_FAIL(fopen);
        return __real_fopen(path, mode);
    }

    // hack
    if(DEBUG) fprintf(stderr, "### hack fopen(%s)\n", path);

    // map fopen mode to open mode
    int num_open_param = 0;
    int open_flag = 0, open_mode = 0;
    int mode_len = strlen(mode);
    if(mode_len == 1)
    {
        if(strcmp(mode, "r") == 0)
        {
            open_flag = O_RDONLY;
            num_open_param = 1;
        }
        else if(strcmp(mode, "w") == 0)
        {
            open_flag = O_WRONLY | O_CREAT | O_TRUNC;
            open_mode = 0666;
            num_open_param = 2;
        }
        else if(strcmp(mode, "a") == 0)
        {
            open_flag = O_WRONLY | O_CREAT | O_APPEND;
            open_mode = 0666;
            num_open_param = 2;
        }
        else
        {
            fprintf(stderr, "ERROR: unknown fopen mode!\n");
            exit(0);
        }
    }
    else if(mode_len == 2)
    {
        if(strcmp(mode, "r+") == 0)
        {
            open_flag = O_RDWR;
            num_open_param = 1;
        }
        else if(strcmp(mode, "w+") == 0)
        {
            open_flag =  O_RDWR | O_CREAT | O_TRUNC;
            open_mode = 0666;
            num_open_param = 2;
        }
        else if(strcmp(mode, "a+") == 0)
        {
            open_flag = O_RDWR | O_CREAT | O_APPEND;
            open_mode = 0666;
            num_open_param = 2;
        }
        else
        {
            fprintf(stderr, "ERROR: unknown fopen mode!\n");
            exit(0);
        }
    }
    else
    {
        fprintf(stderr, "ERROR: unknown fopen mode!\n");
        exit(0);
    }

    assert(num_open_param == 1 || num_open_param == 2);

    if(num_open_param == 1) return fdopen(hack_open(path, open_flag), mode);
    else if(num_open_param == 2) return fdopen(hack_open(path, open_flag, open_mode), mode);

}

int hack_fclose(FILE *stream)
{
    int fd = fileno(stream);
    if(check_is_hacked_fd(fd))
    {
        // the FILE* stream is not linked to a real fd, which means this is a hacked cfd
        if(DEBUG) fprintf(stderr, "### hack fclose()\n");
        return hack_close(fd);
    }
    else
    {
        //  the FILE* stream is linked to a real fd, which means this is a __real_fopened stream
        MAP_OR_FAIL(fclose);
        return __real_fclose(stream);
    }
}

int hack_fseek(FILE *stream, long offset, int whence)
{
    int fd = fileno(stream);
    if(check_is_hacked_fd(fd))
    {
        // the FILE* stream is not linked to a real fd, which means this is a hacked cfd
        if(DEBUG) fprintf(stderr, "### hack fseek()\n");
        return hack_lseek(fd, offset, whence);
    }
    else
    {
        //  the FILE* stream is linked to a real fd, which means this is a __real_fopened stream
        MAP_OR_FAIL(fseek);
        return __real_fseek(stream, offset, whence);
    }
}

size_t hack_fread(void *buffer, size_t size, size_t count, FILE *stream)
{
    int fd = fileno(stream);
    if(check_is_hacked_fd(fd))
    {
        // the FILE* stream is not linked to a real fd, which means this is a hacked cfd
        if(DEBUG) fprintf(stderr, "### hack fread()\n");
        size_t tol_size = size * count;
        return hack_read(fd, buffer, tol_size) / size;
    }
    else
    {
        //  the FILE* stream is linked to a real fd, which means this is a __real_fopened stream
        MAP_OR_FAIL(fread);
        return __real_fread(buffer, size, count, stream);
    }
}

size_t hack_fwrite(void *buffer, size_t size, size_t count, FILE *stream)
{
    // any stream get into tis func, is a hacked cfd
    size_t tol_size = size * count;
    int fd = fileno(stream);
    return hack_write(fd, buffer, tol_size) / size;
}

int DARSHAN_DECL(fclose)(FILE *fp)
{
    return hack_fclose(fp);
}

int DARSHAN_DECL(open64)(const char* path, int flags, ...)
{
    if(DEBUG) fprintf(stderr, "### hack open64()\n");

    int mode = 0;
    int ret;
    int para_num;

    if (flags & O_CREAT)
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
        para_num = 3;
    }
    else
    {
        para_num = 2;
    }

    // The open64() function is a part of the large file extensions, and is equivalent to calling open() with the O_LARGEFILE flag.
    flags |= O_LARGEFILE;

    if(para_num == 2)
        return hack_open(path, flags);
    else if(para_num == 3)
        return hack_open(path, flags, mode);
    else
    {
        fprintf(stderr, "wrong params!");
        exit(0);
    }
    return -1;
}

int DARSHAN_DECL(open)(const char *path, int flags, ...)
{
    int mode = 0;
    int ret;
    int para_num;

    if (flags & O_CREAT)
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
        para_num = 3;
    }
    else
    {
        para_num = 2;
    }

    if(para_num == 2)
        return hack_open(path, flags);
    else if(para_num == 3)
        return hack_open(path, flags, mode);
    else
    {
        fprintf(stderr, "wrong params!");
        exit(0);
    }
}

int DARSHAN_DECL(close)(int fd)
{
    return hack_close(fd);
}

ssize_t DARSHAN_DECL(read)(int fd, void *buf, size_t count)
{
    return hack_read(fd, buf, count);
}

ssize_t DARSHAN_DECL(write)(int fd, const void *buf, size_t count)
{
    return hack_write(fd, buf, count);
}

off64_t DARSHAN_DECL(lseek64)(int fd, off64_t offset, int whence)
{
    if(DEBUG) fprintf(stderr, "### hack lseek64()\n");

    return hack_lseek(fd, offset, whence);
}

off_t DARSHAN_DECL(lseek)(int fd, off_t offset, int whence)
{
    return hack_lseek(fd, offset, whence);
}

FILE* DARSHAN_DECL(fopen)(const char *path, const char *mode)
{
    return hack_fopen(path, mode);
}

int DARSHAN_DECL(fseek)(FILE *stream, long offset, int whence)
{
    return hack_fseek(stream, offset, whence);
}

size_t DARSHAN_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    return hack_fread(ptr, size, nmemb, stream);
}

size_t DARSHAN_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    int fd = fileno(stream);
    if(check_is_hacked_fd(fd))
    {
        // the FILE* stream is not linked to a real fd, which means this is a hacked cfd
        if(DEBUG) fprintf(stderr, "### hack fwrite()\n");
        return hack_fwrite(ptr, size, nmemb, stream);
    }
    else
    {
        //  the FILE* stream is linked to a real fd, which means this is a __real_fopened stream
        MAP_OR_FAIL(fwrite);
        return __real_fwrite(ptr, size, nmemb, stream);
    }

}


int DARSHAN_DECL(unlink)(const char* path)
{
    fprintf(stderr, "unhack function: unlink()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(fsync)(int fd)
{
    fprintf(stderr, "unhack function: fsync()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(fdatasync)(int fd)
{
    fprintf(stderr, "unhack function: fdatasync()\n");
    int ret;
    return(ret);
}

void* DARSHAN_DECL(mmap64)(void *addr, size_t length, int prot, int flags,
                           int fd, off64_t offset)
{
    fprintf(stderr, "unhack function: mmap64()\n");
    void* ret;
    return(ret);
}

void* DARSHAN_DECL(mmap)(void *addr, size_t length, int prot, int flags,
                         int fd, off_t offset)
{
    fprintf(stderr, "unhack function: mmap()\n");
    void* ret;
    return(ret);
}

int DARSHAN_DECL(creat)(const char* path, mode_t mode)
{
    fprintf(stderr, "unhack function: creat()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(creat64)(const char* path, mode_t mode)
{
    fprintf(stderr, "unhack function: creat64()\n");
    int ret;
    return(ret);
}

FILE* DARSHAN_DECL(fopen64)(const char *path, const char *mode)
{
    return hack_fopen(path, mode);
}

int DARSHAN_DECL(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    fprintf(stderr, "unhack function: __xstat64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    fprintf(stderr, "unhack function: __lxstat64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    fprintf(stderr, "unhack function: __fxstat64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(__xstat)(int vers, const char *path, struct stat *buf)
{
    fprintf(stderr, "unhack function: xstat()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(__lxstat)(int vers, const char *path, struct stat *buf)
{
    fprintf(stderr, "unhack function: __lxstat()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(__fxstat)(int vers, int fd, struct stat *buf)
{
    fprintf(stderr, "unhack function: __fxstat()\n");
    int ret;
    return(ret);
}

ssize_t DARSHAN_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    fprintf(stderr, "unhack function: pread64()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(pread)(int fd, void *buf, size_t count, off_t offset)
{
    fprintf(stderr, "unhack function: pread()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    fprintf(stderr, "unhack function: pwrite()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    fprintf(stderr, "unhack function: pwrite64()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    fprintf(stderr, "unhack function: readv()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    fprintf(stderr, "unhack function: writev()\n");
    ssize_t ret;
    return(ret);
}

ssize_t DARSHAN_DECL(aio_return64)(struct aiocb64 *aiocbp)
{
    fprintf(stderr, "unhack function: aio_return64()\n");
    int ret;
    return(ret);
}

ssize_t DARSHAN_DECL(aio_return)(struct aiocb *aiocbp)
{
    fprintf(stderr, "unhack function: aio_return()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(lio_listio)(int mode, struct aiocb *const aiocb_list[],
                             int nitems, struct sigevent *sevp)
{
    fprintf(stderr, "unhack function: lio_listio()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(lio_listio64)(int mode, struct aiocb64 *const aiocb_list[],
                               int nitems, struct sigevent *sevp)
{
    fprintf(stderr, "unhack function: lio_listio64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(aio_write64)(struct aiocb64 *aiocbp)
{
    fprintf(stderr, "unhack function: aio_write64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(aio_write)(struct aiocb *aiocbp)
{
    fprintf(stderr, "unhack function: aio_write()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(aio_read64)(struct aiocb64 *aiocbp)
{
    fprintf(stderr, "unhack function: aio_read64()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(aio_read)(struct aiocb *aiocbp)
{
    fprintf(stderr, "unhack function: aio_read()\n");
    int ret;
    return(ret);
}

int DARSHAN_DECL(fstat)(int fd, struct stat *buf)
{
    fprintf(stderr, "??? fstat()\n");
    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        fprintf(stderr, "??? unhack function: fstat(), direct to __real_fstat()\n");
        return __real_fstat(fd, buf);	// means success
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_close()
        MAP_OR_FAIL(fstat);
        return __real_fstat(fd, buf);
    }
}

int DARSHAN_DECL(ioctl)(int fd, int command, char * argstruct)
{
    //fprintf(stderr, "??? ioctl()\n");
    if(check_is_hacked_fd(fd))
    {
        // is cfd, go to my handle routine
        fprintf(stderr, "??? unhack function: ioctl(), direct to __real_iotcl()\n");
        return __real_ioctl(fd, command, argstruct);	// means success
    }
    else
    {
        // is real fd, which is not open by my routine, so hand it over to real_close()
        MAP_OR_FAIL(ioctl);
        return __real_ioctl(fd, command, argstruct);
    }
}


