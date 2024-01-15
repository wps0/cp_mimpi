/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdint.h>
#include <malloc.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <memory.h>
#include <pthread.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define MAX_PDU_DATA_LENGTH 384
#define EMPTY_MESSAGE_ID 0
#define BUFFER_INITIAL_SIZE 4
#define BUFFER_GROW_FACTOR 2

#define _TAG_BARRIER_ENTER -2
#define _TAG_BARRIER_LEAVE -3
#define _TAG_BARRIER_DATA -4
#define _TAG_EMPTY_MESSAGE -32


#define FOR_IFACE(i) for (MIMPI_If *(i) = instance->ifaces; (i) != instance->ifaces+instance->world_size; ++(i))
#define FOR_IFACE_IT(i, it) for (MIMPI_If *(i) = instance->ifaces; (i) != instance->ifaces+instance->world_size; ++(i), ++(it))


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
    MIMPI_Msg **data;
} MIMPI_Msg_Buffer;

typedef struct MIMPI_If {
    mid_t next_mid;
    int inbound_fd, outbound_fd;
} MIMPI_If;

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

    volatile bool finished;
    pthread_mutex_t mutex;
    pthread_cond_t reader_event;
    // requested message parameters
    bool pending_request;
    rank_t req_src;
    tag_t req_tag;
    int req_count;
    MIMPI_Retcode req_status;
    MIMPI_Msg *req_response;
    pthread_t reader_thread;

    MIMPI_If *ifaces;
    /// Assumption: a ready message is being changed by at most 1 thread
    /// at the same time.
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

// --- Interface management
void free_iface(MIMPI_If *iface) {
    ASSERT_SYS_OK(close(iface->outbound_fd));
    ASSERT_SYS_OK(close(iface->inbound_fd));
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

static inline bool iface_is_open(MIMPI_If *iface) {
    return iface->next_mid > 0 && iface->inbound_fd > 0 && iface->outbound_fd > 0;
}

// --- Buffer

inline static bool is_msg_ready(MIMPI_Msg *msg) {
    return msg != NULL && msg->received_size == msg->size;
}

static void buffer_init(MIMPI_Msg_Buffer *buf) {
    buf->last_pos = 0;
    buf->sz = BUFFER_INITIAL_SIZE;
    buf->data = safe_calloc(buf->sz, sizeof(MIMPI_Msg*));

    for (int i = 0; i < buf->sz; ++i) {
        buf->data[i] = NULL;
    }
}

static void buffer_free(MIMPI_Msg_Buffer *buf) {
    for (int i = 0; i < buf->sz; ++i)
        if (buf->data[i] != NULL) {
            free(buf->data[i]->data);
            free(buf->data[i]);
        }
    free(buf->data);
}

static void init_message_with_pdu(MIMPI_Msg **slot, MIMPI_PDU *pdu) {
    *slot = safe_calloc(1, sizeof(MIMPI_Msg));
    MIMPI_Msg *msg = *slot;
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
    size_t new_sz = buf->sz * BUFFER_GROW_FACTOR;
    MIMPI_Msg **mem = realloc(buf->data, new_sz * sizeof(MIMPI_Msg*));
    ASSERT_ERRNO_OK
    assert(mem);

    for (size_t i = buf->sz; i < new_sz; ++i) {
        mem[i] = NULL;
    }

    buf->sz = new_sz;
    buf->data = mem;
}

static MIMPI_Msg *buffer_find_msg_by_id(MIMPI_Msg_Buffer *buf, rank_t src, mid_t id) {
    size_t starting_pos = buf->last_pos;
    size_t *i = &buf->last_pos;

    do {
        MIMPI_Msg *msg = buf->data[*i];
        if (msg != NULL && msg->id == id && msg->src == src)
            return msg;

        *i = (*i + 1) % buf->sz;
    } while (*i != starting_pos);

    return NULL;
}

static MIMPI_Msg *buffer_find_oldest_msg_by_tag(MIMPI_Msg_Buffer *buf, rank_t src, tag_t tag, int count) {
    size_t starting_pos = buf->last_pos;
    size_t *i = &buf->last_pos;

    MIMPI_Msg *found = NULL;
    do {
        MIMPI_Msg *entry = buf->data[*i];
        if (is_msg_ready(entry) && (entry->tag == tag || tag == MIMPI_ANY_TAG) && entry->src == src
            && entry->size == count)
            if (found == NULL || found->id > entry->id)
                found = entry;

        *i = (*i + 1) % buf->sz;
    } while (*i != starting_pos);

    return found;
}

static MIMPI_Msg **buffer_find_free(MIMPI_Msg_Buffer *buf) {
    size_t starting_pos = buf->last_pos;
    size_t *i = &buf->last_pos;

    do {
        if (buf->data[*i] == NULL)
            return &buf->data[*i];

        *i = (*i + 1) % buf->sz;
    } while (*i != starting_pos);

    return NULL;
}

static MIMPI_Msg *buffer_put(MIMPI_Msg_Buffer *buf, MIMPI_PDU *pdu) {
    MIMPI_Msg *msg = buffer_find_msg_by_id(buf, pdu->src, pdu->id);
//    LOG("%p\n", msg);
    if (msg == NULL) {
        MIMPI_Msg **slot = buffer_find_free(buf);
//        LOG("ff %p\n", msg);
        if (slot == NULL) {
            // The buffer has to be enlarged.
            buffer_enlarge(buf);
            slot = buffer_find_free(buf);
        }

        init_message_with_pdu(slot, pdu);
        msg = *slot;
    }

    insert_pdu_into_message(msg, pdu);
//    LOG("%d %d %d %d\n", msg->id, msg->src, msg->received_size, msg->size);
    return msg;
}

inline static void buffer_del(MIMPI_Msg *msg) {
    if (msg == NULL) {
        return;
    }
    msg->id = EMPTY_MESSAGE_ID;
    free(msg->data);
    msg->data = NULL;
    msg->received_size = 0;
    msg->src = MAX_RANK + 1;
    msg->tag = _TAG_EMPTY_MESSAGE;
}


// --- Receiver

static void reset_request(void) {
    instance->pending_request = false;
    instance->req_count = 0;
    instance->req_src = 0;
    instance->req_tag = _TAG_EMPTY_MESSAGE;
    instance->req_status = MIMPI_SUCCESS;
    instance->req_response = NULL;
}

/// Has to be called with mutex held
static int fill_fdset(fd_set *fds) {
    FD_ZERO(fds);

    int maxfd = 0;
    FOR_IFACE(iface) {
        if (iface_is_open(iface)) {
            FD_SET(iface->inbound_fd, fds);
            maxfd = max(maxfd, iface->inbound_fd);
        }
    }

    return maxfd;
}

/// Captures data from inbound interfaces and puts it in
/// the instance buffer.
static void *inbound_iface_handler(void *arg) {
    MIMPI_PDU pdu;
    fd_set in_fds;

    while (!instance->finished) {
        FD_ZERO(&in_fds);

        pthread_mutex_lock(&instance->mutex);
        int maxfd = fill_fdset(&in_fds);
        pthread_mutex_unlock(&instance->mutex);

        if (maxfd == 0) {
            LOG("Reader: no interfaces open, exiting...\n", "");
            break;
        }

        int st = select(maxfd + 1, &in_fds, NULL, NULL, NULL);
        if (st == -1) {
            continue;
        }

        int i = 0;
        FOR_IFACE_IT(iface, i) {
            if (!FD_ISSET(iface->inbound_fd, &in_fds))
                continue;

            int nbytes = chrecv(iface->inbound_fd, &pdu, sizeof(MIMPI_PDU));
            if (nbytes == 0) {
                // end-of-file - pipe closed
                LOG("%d received EOF on %d\n", instance->rank, i);
                pthread_mutex_lock(&instance->mutex);
                if (iface_is_open(iface))
                    free_iface(iface);

                if (instance->pending_request && instance->req_src == i) {
                    reset_request();
                    instance->req_status = MIMPI_ERROR_REMOTE_FINISHED;
                    instance->pending_request = false;

                    pthread_cond_signal(&instance->reader_event);
                }

                pthread_mutex_unlock(&instance->mutex);
                continue;
            }

            if (nbytes == -1) {
                // The main thread has closed an fd from which
                // this thread was reading.
                LOG("nbytes == -1\n", "");
                break;
            }

            LOG("PDU src=%d tag=%d id=%d seq=%d len=%d total_len=%d\n", pdu.src, pdu.tag, pdu.id, pdu.seq, pdu.length, pdu.total_length);
            assert(nbytes == sizeof(MIMPI_PDU));
            assert(pdu.src == i);

            pthread_mutex_lock(&instance->mutex);
            MIMPI_Msg *msg = buffer_put(instance->inbound_buf, &pdu);
//            LOG("requested: src=%d tag=%d count=%d\n", instance->req_src, instance->req_tag, instance->req_count);
            if (instance->pending_request && pdu.src == instance->req_src && (pdu.tag == instance->req_tag || instance->req_tag == MIMPI_ANY_TAG)
                && pdu.total_length == instance->req_count && is_msg_ready(msg)) {
//                LOG("Packet matches!\n", "");
                instance->req_response = msg;
                instance->pending_request = false;

                pthread_cond_signal(&instance->reader_event);
            }
            pthread_mutex_unlock(&instance->mutex);
        }
    }

    pthread_mutex_lock(&instance->mutex);
    if (instance->pending_request) {
        reset_request();
        instance->req_status = MIMPI_ERROR_REMOTE_FINISHED;
        pthread_cond_signal(&instance->reader_event);
    }
    pthread_mutex_unlock(&instance->mutex);

    LOG("Reader thread has finished.\n", "");
    return NULL;
}

// --- Generic code

static MIMPI_Retcode send_if(MIMPI_If *iface, MIMPI_PDU *pdu) {
    int nbytes = chsend(iface->outbound_fd, pdu, sizeof(MIMPI_PDU));
    if (nbytes == -1) {
        assert(errno == EBADF || errno == EPIPE);
        LOG("Error (errno: %d) while sending data to fd %d\n", errno, iface->outbound_fd);

        pthread_mutex_lock(&instance->mutex);
        if (iface_is_open(iface))
            free_iface(iface);
        pthread_mutex_unlock(&instance->mutex);

        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    ASSERT_SYS_OK(nbytes);
    assert(nbytes == sizeof(*pdu));
    return MIMPI_SUCCESS;
}

static MIMPI_Retcode validate_sendrecv_args(int rank) {
    if (rank == instance->rank)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (rank < 0 || rank >= instance->world_size)
        return MIMPI_ERROR_NO_SUCH_RANK;
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

static void reduce(uint8_t *dest, uint8_t *source, MIMPI_Op op, int count) {
    for (int i = 0; i < count; ++i) {
        dest[i] = apply_op(dest[i], source[i], op);
    }
}

inline static MIMPI_Retcode get_failing(MIMPI_Retcode a, MIMPI_Retcode b, MIMPI_Retcode c, MIMPI_Retcode d) {
    if (a != MIMPI_SUCCESS)
        return a;
    if (b != MIMPI_SUCCESS)
        return b;
    if (c != MIMPI_SUCCESS)
        return c;
    if (d != MIMPI_SUCCESS)
        return d;
    return MIMPI_SUCCESS;
}

inline static MIMPI_Retcode get_failing2(MIMPI_Retcode a, MIMPI_Retcode b) {
    if (a != MIMPI_SUCCESS)
        return a;
    if (b != MIMPI_SUCCESS)
        return b;
    return MIMPI_SUCCESS;
}

static MIMPI_Retcode internal_barrier() {
    LOG("%d is entering the barrier...\n", instance->rank);

    rank_t v = instance->rank + 1;
    int lchild = LEFT(v) - 1, rchild = RIGHT(v) - 1, parent = PARENT(v) - 1;

    MIMPI_Retcode status, lst, rst, lchst, rchst;
    status = lst = rst = lchst = rchst = MIMPI_SUCCESS;

    if (lchild < instance->world_size)
        lst = MIMPI_Recv(&lchst, sizeof(MIMPI_Retcode), lchild, _TAG_BARRIER_ENTER);
    if (rchild < instance->world_size)
        rst = MIMPI_Recv(&rchst, sizeof(MIMPI_Retcode), rchild, _TAG_BARRIER_ENTER);

    status = get_failing(lst, lchst, rst, rchst);

    if (v != 1) {
        MIMPI_Retcode pst = MIMPI_Send(&status, sizeof(MIMPI_Retcode), parent, _TAG_BARRIER_ENTER);
        if (pst != MIMPI_SUCCESS)
            status = pst;
        else {
            MIMPI_Retcode ret = MIMPI_Recv(&pst, sizeof(MIMPI_Retcode), parent, _TAG_BARRIER_LEAVE);
            status = get_failing2(get_failing2(ret, pst), status);
            fflush(stdout);
        }
    }

    if (lchild < instance->world_size)
        MIMPI_Send(&status, sizeof(MIMPI_Retcode), lchild, _TAG_BARRIER_LEAVE);
    if (rchild < instance->world_size)
        MIMPI_Send(&status, sizeof(MIMPI_Retcode), rchild, _TAG_BARRIER_LEAVE);


    LOG("%d is leaving the barrier with status %d...\n", instance->rank, status);
    return status;
}


// ----- instance management

void make_instance(bool enable_deadlock_detection) {
    instance = safe_calloc(1, sizeof(MIMPI_Instance));
    instance->deadlock_detection = enable_deadlock_detection;
    instance->barrier_count = 0;
    reset_request();

    // rank & world_size
    char *env_ptr;
    env_ptr = getenv(MIMPI_ENV_RANK);
    assert(env_ptr != NULL);
    instance->rank = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    env_ptr = getenv(MIMPI_ENV_WORLD_SIZE);
    assert(env_ptr != NULL);
    instance->world_size = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    // interfaces
    instance->ifaces = safe_calloc(instance->world_size, sizeof(MIMPI_If));
    instance->inbound_buf = safe_calloc(1, sizeof(MIMPI_Msg_Buffer));
    buffer_init(instance->inbound_buf);

    // mutexes
    ASSERT_ZERO(pthread_mutex_init(&instance->mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&instance->reader_event, NULL));
}

void free_instance(MIMPI_Instance **mimpiInstance) {
    MIMPI_Instance *inst = *mimpiInstance;

    inst->finished = true;

    // free interfaces
    if (inst->ifaces != NULL) {
        pthread_mutex_lock(&inst->mutex);
        FOR_IFACE(iface)
            if (iface_is_open(iface))
                free_iface(iface);
        pthread_mutex_unlock(&inst->mutex);
    }

    ASSERT_ZERO(pthread_join(instance->reader_thread, NULL));

    // destroy mutexes
    pthread_mutex_destroy(&inst->mutex);
    pthread_cond_destroy(&inst->reader_event);

    if (inst->ifaces != NULL) {
        free(inst->ifaces);
    }

    buffer_free((*mimpiInstance)->inbound_buf);
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

    ASSERT_ZERO(pthread_create(&instance->reader_thread, NULL, inbound_iface_handler, NULL));
}

void MIMPI_Finalize() {
    int rank = instance->rank;
    LOG("Finalizing %d...\n", rank);
    free_instance(&instance);

    channels_finalize();
    LOG("Process %d has finished.\n", rank);
}

int MIMPI_World_size() {
    return instance->world_size;
}

int MIMPI_World_rank() {
    return instance->rank;
}

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag) {
    assert((data == NULL && count == 0) || (count != 0 && data != NULL));
    if (validate_sendrecv_args(destination) != MIMPI_SUCCESS)
        return validate_sendrecv_args(destination);

    pdu_seq_t seq = 0;
    int offset = 0;
    MIMPI_Retcode status = MIMPI_SUCCESS;

    pthread_mutex_lock(&instance->mutex);
    if (!iface_is_open(&instance->ifaces[destination])) {
        pthread_mutex_unlock(&instance->mutex);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    MIMPI_If *iface = &instance->ifaces[destination];
    mid_t next_mid = iface->next_mid;
    pthread_mutex_unlock(&instance->mutex);

    do {
        int chunk_size = min(MAX_PDU_DATA_LENGTH, count - offset);
        MIMPI_PDU pdu = {
                instance->rank,
                tag,
                next_mid,
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

    if (status == MIMPI_SUCCESS) {
        // Required, because iface_is_open uses next_mid to determine
        // if an interface is open!
        pthread_mutex_lock(&instance->mutex);
        ++iface->next_mid;
        pthread_mutex_unlock(&instance->mutex);
    }

    return status;
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {
//    LOG("Waiting for count %d src %d tag %d\n", count, source, tag);
    assert((data == NULL && count == 0) || (count != 0 && data != NULL));
    if (validate_sendrecv_args(source) != MIMPI_SUCCESS)
        return validate_sendrecv_args(source);

    MIMPI_Retcode status = MIMPI_SUCCESS;

    pthread_mutex_lock(&instance->mutex);
    MIMPI_Msg *msg = buffer_find_oldest_msg_by_tag(instance->inbound_buf, source, tag, count);
    if (msg == NULL) {
//        LOG("Msg with src %d is null\n", source);
        if (!iface_is_open(&instance->ifaces[source])) {
            // All interfaces are closed, and therefore the reading thread
            // doesn't exist.
//            LOG("Iface isn't open!", "");
            status = MIMPI_ERROR_REMOTE_FINISHED;
        } else {
            instance->pending_request = true;
            instance->req_src = source;
            instance->req_tag = tag;
            instance->req_count = count;

            pthread_cond_wait(&instance->reader_event, &instance->mutex);


            msg = instance->req_response;
            status = instance->req_status;

//            LOG("Received reader response: msg %p status %d for req from %d\n", msg, instance->req_status, source);
            reset_request();
        }
    } else {
//        LOG("Msg with src %d is not null!!\n", source);
    }

    pthread_mutex_unlock(&instance->mutex);

    if (status == MIMPI_SUCCESS) {
        assert(msg->src == source);
        if (tag != MIMPI_ANY_TAG)
            assert(msg->tag == tag);
        assert(msg->size == count);
        // Even if count = 0, memcpy behaves correctly.
        memcpy(data, msg->data, count);
        buffer_del(msg);
    }

    return status;
}

MIMPI_Retcode MIMPI_Barrier(void) {
    if (instance->world_size == 1)
        return MIMPI_SUCCESS;

    return internal_barrier();
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    if (root < 0 || root >= instance->world_size)
        return MIMPI_ERROR_NO_SUCH_RANK;
    if (instance->world_size == 1)
        return MIMPI_SUCCESS;

    MIMPI_Retcode ret = MIMPI_Barrier();
    if (ret != MIMPI_SUCCESS)
        return ret;

    if (instance->rank == root) {
        for (int i = 0; i < instance->world_size; ++i)
            if (i != root)
                MIMPI_Send(data, count, i, _TAG_BARRIER_DATA);
    } else {
        MIMPI_Recv(data, count, root, _TAG_BARRIER_DATA);
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    if (root < 0 || root >= instance->world_size)
        return MIMPI_ERROR_NO_SUCH_RANK;
    if (instance->world_size == 1)
        return MIMPI_SUCCESS;

    MIMPI_Retcode ret = MIMPI_Barrier();
    if (ret != MIMPI_SUCCESS)
        return ret;

    if (instance->rank == root) {
        memcpy(recv_data, send_data, count);

        uint8_t *buf = safe_calloc(count, sizeof(uint8_t));
        for (int i = 0; i < instance->world_size; ++i)
            if (i != root) {
                MIMPI_Recv(buf, count, i, _TAG_BARRIER_DATA);
                reduce(recv_data, buf, op, count);
            }
    } else {
        MIMPI_Send(send_data, count, root, _TAG_BARRIER_DATA);
    }

    return MIMPI_SUCCESS;
}
