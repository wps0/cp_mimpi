/**
 * This file is for implementation of MIMPI library.
 * */

#include <bits/stdint-uintn.h>
#include <malloc.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define MAX_PDU_DATA_LENGTH 256
#define CHANNELS_MAX_ATOMIC_DATA_CHUNK 512


// ----- structures

typedef struct MIMPI_If {
    int inbound_fd, outbound_fd;
    // TODO: Queues
} MIMPI_If;

typedef struct MIMPI_PDU {
    uint8_t src, dst;
    int tag;
    // Sequence number, as in TCP
    uint32_t seq;
    uint16_t length;
    uint8_t data[MAX_PDU_DATA_LENGTH];
} MIMPI_PDU;

typedef struct MIMPI_Instance {
    bool deadlock_detection;
    int rank;
    int world_size;

    MIMPI_If *ifaces;
} MIMPI_Instance;

// ----- program data
static MIMPI_Instance *instance;

// ----- helper functions

void free_iface(MIMPI_If *iface) {
    close(iface->outbound_fd);
    close(iface->inbound_fd);
}

void init_ifaces(MIMPI_If *ifaces) {
    for (int i = 0; i < instance->world_size; i++) {
        if (i != instance->rank) {
            ifaces[i].inbound_fd = INBOUND_IF_FD(i);
            ifaces[i].outbound_fd = OUTBOUND_IF_FD(i);
        }
    }
}

void make_instance(bool enable_deadlock_detection) {
    instance = calloc(1, sizeof(MIMPI_Instance));
    instance->deadlock_detection = enable_deadlock_detection;

    char *env_ptr;
    env_ptr = getenv(MIMPI_ENV_RANK);
    assert(env_ptr != NULL);
    instance->rank = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    env_ptr = getenv(MIMPI_ENV_WORLD_SIZE);
    assert(env_ptr != NULL);
    instance->world_size = (int) strtol(env_ptr, NULL, 10);
    ASSERT_ERRNO_OK

    instance->ifaces = calloc(instance->world_size, sizeof(MIMPI_If));
    assert(instance->ifaces);
}

void free_instance(MIMPI_Instance **mimpiInstance) {
    MIMPI_Instance *inst = *mimpiInstance;

    // free interfaces
    if (inst->ifaces != NULL) {
        for (size_t i = 0; i < inst->world_size; i++)
            free_iface(&inst->ifaces[i]);
        free(inst->ifaces);
    }

    free(*mimpiInstance);
    *mimpiInstance = NULL;
}

inline static size_t real_pdu_size(MIMPI_PDU const* pdu) {
    static const size_t PDU_WITHOUT_DATA_LENGTH = sizeof(*pdu) - MAX_PDU_DATA_LENGTH;
    return PDU_WITHOUT_DATA_LENGTH + pdu->length;
}

static void send_if(MIMPI_If *iface, MIMPI_PDU *restrict pdu) {
    size_t pdu_size = real_pdu_size(pdu);
    assert(pdu_size <= CHANNELS_MAX_ATOMIC_DATA_CHUNK);
    ASSERT_SYS_OK(chsend(iface->outbound_fd, pdu, pdu_size));
}

/// PDU has to be at least MAX_PDU_DATA_LENGTH wide.
static void recv_if(MIMPI_If *iface, MIMPI_PDU *pdu) {
    ASSERT_SYS_OK(chrecv(iface->inbound_fd, pdu, MAX_PDU_DATA_LENGTH));
}

// ----- API

void MIMPI_Init(bool enable_deadlock_detection) {
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

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}