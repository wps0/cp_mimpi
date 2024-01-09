/**
 * This file is for implementation of MIMPI library.
 * */

#include <bits/stdint-uintn.h>
#include <malloc.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <memory.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define MAX_PDU_DATA_LENGTH 256
#define CHANNELS_MAX_ATOMIC_DATA_CHUNK 512
#define PDU_SIZE_WITHOUT_DATA_LENGTH (sizeof(*pdu) - MAX_PDU_DATA_LENGTH)
#define EMPTY_MESSAGE_ID 0

/// Specifies how many programs wake other
#define BARRIER_CONCURRENCY_FACTOR 4

#define _TAG_BARRIER_ENTER -2
#define _TAG_BARRIER_LEAVE -3
#define _TAG_BARRIER_DATA -4
#define _TAG_CURRENT_TRANSFER_FAILED -5

#define _TAG_RANGE_BARRIER -100


// ----- structures
typedef uint32_t pdu_seq_t;
typedef int tag_t;
typedef uint8_t rank_t;

/// Message id
typedef uint32_t mid_t;

typedef struct MIMPI_Msg {
    mid_t id;
    tag_t tag;
    rank_t src;
    size_t size;
    size_t received_size;
    uint8_t *data;
} MIMPI_Msg;

typedef struct MIMPI_Msg_Buffer {
    size_t last_pos;
    size_t sz;
    MIMPI_Msg *data;
} MIMPI_Msg_Buffer;

typedef struct MIMPI_If {
    mid_t next_mid;
    int inbound_fd, outbound_fd;
} MIMPI_If;

// TODO: włączyć sequence_Number w message id?
typedef struct MIMPI_PDU {
    rank_t src;
    tag_t tag;
    /// Message id
    mid_t id;
    /// Sequence number, as in TCP
    pdu_seq_t seq;
    size_t total_length;
    uint16_t length;
    uint8_t data[MAX_PDU_DATA_LENGTH];
} MIMPI_PDU;

typedef struct MIMPI_Instance {
    bool deadlock_detection;
    int rank;
    int world_size;
    int barrier_count;
    int finished;

    MIMPI_If *ifaces;
    MIMPI_Msg_Buffer *inbound_buf;
} MIMPI_Instance;

// ----- program data
static MIMPI_Instance *instance;


// -----------------------
// ----- helper functions

inline static void *safe_calloc(size_t nmemb, size_t size) {
    void *mem = calloc(nmemb, size);
    ASSERT_ERRNO_OK
    assert(mem);
    return mem;
}

// ----- Network stack
// --- Buffer
static void init_buffer(MIMPI_Msg_Buffer *buf) {
    buf->last_pos = 0;
    buf->sz = 4;
    buf->data = safe_calloc(buf->sz, sizeof(MIMPI_Msg));
}

static void free_buffer(MIMPI_Msg_Buffer *buf) {
    for (int i = 0; i < buf->sz; ++i)
        if (buf->data[i].id != 0)
            free(buf->data[i].data);
    free(buf->data);
}

static void init_message_with_pdu(MIMPI_Msg *msg, MIMPI_PDU *pdu) {
    msg->id = pdu->id;
    msg->tag = pdu->tag;
    msg->src = pdu->src;
    msg->size = pdu->total_length;
    msg->received_size = 0;

    msg->data = safe_calloc(msg->size, sizeof(uint8_t));
}

inline static void insert_pdu_into_message(MIMPI_Msg *msg, MIMPI_PDU *pdu) {
    memcpy(msg->data + pdu->seq * MAX_PDU_DATA_LENGTH, pdu->data, pdu->length);
    msg->received_size += pdu->length;
}

inline static void buffer_enlarge(MIMPI_Msg_Buffer *buf) {
    void *mem = realloc(buf->data, buf->sz * sizeof(MIMPI_Msg) * 2);
    ASSERT_ERRNO_OK
    assert(mem);

    buf->sz *= 2;
    buf->data = mem;
}

static MIMPI_Msg *buffer_find_msg(MIMPI_Msg_Buffer *buf, rank_t src, mid_t id) {
    for (size_t i = 0; i < buf->sz; i++)
        if (buf->data[i].id == id && buf->data[id].src == src)
            return &buf->data[i];
    return NULL;
}

static MIMPI_Msg *buffer_find_free(MIMPI_Msg_Buffer *buf) {
    size_t starting_pos = buf->last_pos;
    size_t *i = &buf->last_pos;

    do {
        if (buf->data[*i].id == 0)
            return &buf->data[*i];

        *i = (*i + 1) % buf->sz;
    } while (*i != starting_pos);

    return NULL;
}

static void buffer_put(MIMPI_Msg_Buffer *buf, MIMPI_PDU *pdu) {
    MIMPI_Msg *msg = buffer_find_msg(buf, pdu->src, pdu->id);
    if (msg == NULL) {
        msg = buffer_find_free(buf);
        if (msg == NULL) {
            // The buffer has to be enlarged.
            buffer_enlarge(buf);
        }

        msg = buffer_find_free(buf);
        init_message_with_pdu(msg, pdu);
    }

    insert_pdu_into_message(msg, pdu);
}

inline static void buffer_del(MIMPI_Msg *msg) {
    msg->id = EMPTY_MESSAGE_ID;
    free(msg->data);
    msg->data = NULL;
}

// --- Receiver

//static void *inbound_iface_handler(void *arg) {
//    fd_set inbound_ifs;
//    FD_ZERO(&inbound_ifs);
//    while (true) {
//        // select(...)
//    }
//}

// --- Generic code

static MIMPI_Retcode send_if(MIMPI_If *iface, MIMPI_PDU *pdu) {
    int nbytes = chsend(iface->outbound_fd, pdu, sizeof(MIMPI_PDU));
    ASSERT_SYS_OK(nbytes);
    assert(nbytes == sizeof(*pdu));
    return MIMPI_SUCCESS;
}

static MIMPI_Retcode validate_sendrecv_args() {
    return MIMPI_SUCCESS;
}

// ----- group functions
#define PARENT(v) ((v) / 2)
#define LEFT(v) (2*(v))
#define RIGHT(v) (2*(v) + 1)

inline static uint8_t apply_op(uint8_t a, uint8_t b, MIMPI_Op op) {
    switch (op) {
        case MIMPI_MAX:
            return max(a, b);
        case MIMPI_MIN:
            return min(a, b);
        case MIMPI_SUM:
            return a + b;
        case MIMPI_PROD:
            return a * b;
        default:
            assert(false);
    }
    return 0;
}

inline static int barrier_tag(int barrier_count) {
    return _TAG_RANGE_BARRIER - barrier_count;
}

static void reduce(uint8_t *dest, uint8_t *source, MIMPI_Op op, int count) {
    for (int i = 0; i < count; ++i) {
        dest[i] = apply_op(dest[i], source[i], op);
    }
}

static void propagate_barrier_data(rank_t child_rank, MIMPI_Retcode *status, void *data, int count) {
    LOG("%d is sending a barrier leave notification to %d\n", instance->rank, child_rank);
    assert(MIMPI_Send(status, sizeof(MIMPI_Retcode), child_rank, _TAG_BARRIER_LEAVE) == MIMPI_SUCCESS);

    if (data != NULL && *status == MIMPI_SUCCESS) {
        LOG("%d is sending barrier data (%d byes) to %d\n", instance->rank, count, child_rank);
        assert(MIMPI_Send(data, count, child_rank, _TAG_BARRIER_DATA) == MIMPI_SUCCESS);
    }
}

static MIMPI_Retcode internal_barrier(rank_t root, void *data, int count) {
    LOG("%d is entering the barrier...\n", instance->rank);

    MIMPI_Retcode status = MIMPI_SUCCESS;
    rank_t v = instance->rank + 1;
    int parent = PARENT(v) - 1;

    if (instance->rank == root) {
        for (int i = 0; i < instance->world_size; ++i) {
            if (i == root)
                continue;
            LOG("The root of the barrier (%d) is waiting for %d to enter.\n", root, i);
            // TODO: odbierać od dowolnych, co jak i=0 wejdzie ostatnie?
            MIMPI_Retcode ret = MIMPI_Recv(NULL, 0, i, _TAG_BARRIER_ENTER);
            if (ret != MIMPI_SUCCESS)
                status = ret;
            LOG("The root of the barrier (%d) acknowledged that %d has entered the barrier or is offline (ret: %d).\n", root, i, ret);
        }

        if (root != 0)
            propagate_barrier_data(0, &status, data, count);
    } else {
        MIMPI_Send(NULL, 0, root, _TAG_BARRIER_ENTER);

        int vparent = instance->rank == 0 ? root : parent;
        MIMPI_Retcode ret = MIMPI_Recv(&status, sizeof(MIMPI_Retcode), vparent, _TAG_BARRIER_LEAVE);
        LOG("%d has received the barrier leave notification.\n", instance->rank);
        if (ret != MIMPI_SUCCESS)
            status = ret;

        if (data != NULL) {
            LOG("%d is waiting for data from %d.\n", instance->rank, vparent);
            assert(MIMPI_Recv(data, count, vparent, _TAG_BARRIER_DATA) == MIMPI_SUCCESS);
        }
    }

    // propagate the messages down the tree
    int lchild = LEFT(v) - 1, rchild = RIGHT(v) - 1;
//    LOG("LEFT(%d) = %d, RIGHT = %d", v, lchild, rchild);
    if (lchild != root && lchild < instance->world_size)
        propagate_barrier_data(lchild, &status, data, count);
    if (rchild != root && rchild < instance->world_size)
        propagate_barrier_data(rchild, &status, data, count);

    LOG("%d is leaving the barrier with status %d...\n", instance->rank, status);
    return status;
}

inline static bool rank_exists(rank_t r) {
    return r < instance->world_size;
}

inline static bool is_ok(MIMPI_Retcode ret) {
    return ret == MIMPI_SUCCESS;
}

inline static void swap(MIMPI_Retcode *a, MIMPI_Retcode *b) {
    MIMPI_Retcode c = *a;
    *a = *b;
    *b = c;
}

static MIMPI_Retcode internal_reduce(    void const *send_data,
                                         void *recv_data,
                                         int count,
                                         MIMPI_Op op,
                                         int root_dest
) {
    const rank_t rank = instance->rank;
    const int btag = barrier_tag(++instance->barrier_count);
    LOG("%d is entering the reduction barrier...\n", rank);

    uint8_t *data = safe_calloc(count, sizeof(*data));
    uint8_t *buf = safe_calloc(count, sizeof(*buf));
    assert(memcpy(data, send_data, count));

    MIMPI_Retcode retl, retr, retp;
    rank_t lch = LEFT(rank+1)-1, rch = RIGHT(rank+1)-1, par = PARENT(rank+1)-1;
    retl = retr = retp = MIMPI_SUCCESS;

    if (rank_exists(lch)) {
        retl = MIMPI_Recv(buf, count, lch, btag);
        if (is_ok(retl))
            reduce(data, buf, op, count);
    }
    if (rank_exists(rch)) {
        retr = MIMPI_Recv(buf, count, rch, btag);
        if (is_ok(retr))
            reduce(data, buf, op, count);
    }

    if (!is_ok(retr))
        swap(&retr, &retl);
    if (!is_ok(retl)) {
        // TODO: dziecko failuje, wysyła do rodzica, rodzic failuje, wysyła do dziecka, w dziecku jest przerywane inne mimpi_recv
        MIMPI_Send(&retl, sizeof(retl), lch, _TAG_CURRENT_TRANSFER_FAILED);
        MIMPI_Send(&retl, sizeof(retl), rch, _TAG_CURRENT_TRANSFER_FAILED);

        if (rank != 0)
            // TODO: obsługa w MIMPI_Recv tagu current transfer failed
            MIMPI_Send(&retl, sizeof(retl), par, _TAG_CURRENT_TRANSFER_FAILED);
        return retl;
    } else if (rank != 0) {
        retp = MIMPI_Send(data, count, par, btag);
        if (!is_ok(retp)) {
            MIMPI_Send(&retp, sizeof(retp), lch, _TAG_CURRENT_TRANSFER_FAILED);
            MIMPI_Send(&retp, sizeof(retp), rch, _TAG_CURRENT_TRANSFER_FAILED);

            return retp;
        }
    }

    if (rank != 0 && rank != root_dest) {
        LOG("%d is waiting for leave notification\n", rank);
        MIMPI_Retcode ret;
        ASSERT_MIMPI_OK(MIMPI_Recv(&ret, sizeof(ret), par, _TAG_BARRIER_LEAVE));
        MIMPI_Send(&ret, sizeof(ret), lch, btag);
        MIMPI_Send(&ret, sizeof(ret), rch, btag);
        LOG("%d has received the leave notification\n", rank);
        return ret;
    }

    if (root_dest != 0) {
        if (rank == 0) {
            LOG("%d sending data to root %d\n", rank, root_dest);
            MIMPI_Send(data, count, root_dest, btag);
        } else if (root_dest == rank) {
            MIMPI_Recv(data, count, 0, btag);
            LOG("%d has received root data\n", rank);
        }
    }

    if (rank == 0) {
        MIMPI_Retcode ret = MIMPI_SUCCESS;
        MIMPI_Send(&ret, sizeof(ret), lch, btag);
        MIMPI_Send(&ret, sizeof(ret), rch, btag);
    }

    if (rank == root_dest)
        memcpy(recv_data, data, count);
    LOG("%d is leaving the reduction barrier\n", instance->rank);
    return MIMPI_SUCCESS;
}


// ----- instance & interfaces management

void free_iface(MIMPI_If *iface) {
    close(iface->outbound_fd);
    close(iface->inbound_fd);
    iface->next_mid = 0;
    iface->inbound_fd = -1;
    iface->outbound_fd = -1;
}

void init_ifaces(MIMPI_If *ifaces) {
    for (int i = 0; i < instance->world_size; i++) {
        if (i != instance->rank) {
            ifaces[i].next_mid = 1;
            ifaces[i].inbound_fd = INBOUND_IF_FD(i);
            ifaces[i].outbound_fd = OUTBOUND_IF_FD(i);
        }
    }
}

static inline bool is_closed(MIMPI_If *iface) {
    return iface->next_mid == 0;
}

void make_instance(bool enable_deadlock_detection) {
    instance = safe_calloc(1, sizeof(MIMPI_Instance));
    instance->deadlock_detection = enable_deadlock_detection;
    instance->barrier_count = 0;

    char *env_ptr;
    env_ptr = getenv(MIMPI_ENV_RANK);
    assert(env_ptr != NULL);
    instance->rank = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    env_ptr = getenv(MIMPI_ENV_WORLD_SIZE);
    assert(env_ptr != NULL);
    instance->world_size = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    instance->ifaces = safe_calloc(instance->world_size, sizeof(MIMPI_If));
    instance->inbound_buf = safe_calloc(1, sizeof(MIMPI_Msg_Buffer));
    init_buffer(instance->inbound_buf);
}

void free_instance(MIMPI_Instance **mimpiInstance) {
    MIMPI_Instance *inst = *mimpiInstance;

    // free interfaces
    if (inst->ifaces != NULL) {
        for (size_t i = 0; i < inst->world_size; i++)
            free_iface(&inst->ifaces[i]);
        free(inst->ifaces);
    }

    free_buffer((*mimpiInstance)->inbound_buf);
    free((*mimpiInstance)->inbound_buf);
    free(*mimpiInstance);
    *mimpiInstance = NULL;
}


// -----------------------
// ----- API

void MIMPI_Init(bool enable_deadlock_detection) {
    errno = 0;
    LOG("Initialising process with rank %s\n", getenv(MIMPI_ENV_RANK));
    channels_init();

    make_instance(enable_deadlock_detection);
    init_ifaces(instance->ifaces);
}

void MIMPI_Finalize() {
    free_instance(&instance);

    channels_finalize();
}

int MIMPI_World_size() {
    // TODO: mutex?
    return instance->world_size;
}

int MIMPI_World_rank() {
    // TODO: mutex?
    return instance->rank;
}

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag) {
    if (destination < 0 || destination >= instance->world_size)
        return MIMPI_ERROR_NO_SUCH_RANK;
    assert(data == NULL && count == 0 || count != 0 && data != NULL);

    pdu_seq_t seq = 0;
    int offset = 0;
    MIMPI_Retcode status = MIMPI_SUCCESS;
    MIMPI_If *iface = &instance->ifaces[destination];

    do {
        int chunk_size = min(MAX_PDU_DATA_LENGTH, count - offset);
        MIMPI_PDU pdu = {
                instance->rank,
                tag,
                iface->next_mid,
                seq,
                count,
                chunk_size
        };

        if (data != NULL)
            memcpy(&pdu.data, (char const*) data + offset, chunk_size);
        if (chunk_size < MAX_PDU_DATA_LENGTH)
            memset(&pdu.data[chunk_size], 0, MAX_PDU_DATA_LENGTH - chunk_size);

        status = send_if(iface, &pdu);
        if (status != MIMPI_SUCCESS) {
            break;
        }

        seq++;
        offset += chunk_size;
    } while (offset < count);

    ++iface->next_mid;
    return status;
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
    assert(data == NULL && count == 0 || count != 0 && data != NULL);

    int offset = 0;
    do {
        MIMPI_PDU pdu;
        int nbytes = chrecv(instance->ifaces[source].inbound_fd, &pdu, sizeof(MIMPI_PDU));
        ASSERT_SYS_OK(nbytes);
        assert(nbytes == sizeof(MIMPI_PDU));
        assert(pdu.src == source);

        if (pdu.tag == _TAG_CURRENT_TRANSFER_FAILED) {
            LOG("Transfer to %d, souce=%d tag=%d interrupted by %d\n", instance->rank, source, tag, pdu.src);
            MIMPI_Retcode code;
            memcpy(&code, pdu.data, sizeof(code));
            return code;
        }

        assert(pdu.tag == tag);

        if (data != NULL) {
            memcpy((char*) data + offset, pdu.data, pdu.length);
            assert((char*)data + offset <= (char*) data + count);
        }
        offset += MAX_PDU_DATA_LENGTH;
    } while (offset < count);

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier(void) {
    if (instance->world_size == 1)
        return MIMPI_SUCCESS;

    return internal_barrier(0, NULL, 0);
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    return internal_barrier(root, data, count);
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    return internal_reduce(send_data, recv_data, count, op, root);
}
