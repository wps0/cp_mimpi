/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include "mimpi_common.h"
#include "channel.h"

int*** chn;

static int number_length(int n) {
    int len = 1;
    while (n >= 10) {
        n /= 10;
        len++;
    }

    return len;
}

static void init_channels(int workers) {
    chn = calloc(sizeof(int**), workers);
    for (int i = 0; i < workers; ++i) {
        chn[i] = calloc(sizeof(int*), workers);
        for (int j = 0; j < workers; ++j) {
            chn[i][j] = calloc(sizeof(int), 2);

            channel(chn[i][j]);
        }
    }
}

static void cleanup_channels(int workers) {
    for (int i = 0; i < workers; ++i) {
        for (int j = 0; j < workers; ++j) {
            for (int k = 0; k < 2; k++)
                close(chn[i][j][k]);
            free(chn[i][j]);
        }

        free(chn[i]);
    }

    free(chn);
}

static void prepare_environment(int rank, int workers) {
    const int buf_len = number_length(MAX_RANK) + 1;
    char buffer[buf_len];
    ASSERT_NON_NEGATIVE(snprintf(buffer, buf_len, "%d", rank));
    ASSERT_SYS_OK(setenv(MIMPI_ENV_RANK, buffer, 1));
    ASSERT_NON_NEGATIVE(snprintf(buffer, buf_len, "%d", workers));
    ASSERT_SYS_OK(setenv(MIMPI_ENV_WORLD_SIZE, buffer, 1));

    // prepare the necessary
    for (int i = 0; i < workers; ++i) {
        if (i != rank) {
            dup2(chn[rank][i][1], OUTBOUND_IF_FD(i));
            dup2(chn[i][rank][0], INBOUND_IF_FD(i));
        }
    }

    // close the not relevant fds & free memory
    cleanup_channels(workers);
}

static void replace_process(char* prog, char** argv) {
    ASSERT_SYS_OK(execvp(prog, argv));
}

static void run_workers(int count, char* prog_path, char** prog_argv) {
    for (int i = 0; i < count; i++) {
        LOG("Running worker %d...\n", i);
        int w_pid = fork();
        ASSERT_SYS_OK(w_pid);

        if (w_pid == 0) {
            // child
            prepare_environment(i, count);
            print_open_descriptors();
            replace_process(prog_path, prog_argv);
        }
    }
}

static void wait_for_all(int count) {
    while (count--) {
        int ret_code;
        int pid = wait(&ret_code);
        ASSERT_SYS_OK(pid);
        LOG("Child with pid=%d returned %d\n", pid, WEXITSTATUS(ret_code));
    }
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fatal("Too little arguments.");
    }

    int workers = atoi(argv[1]);
    init_channels(workers);
    run_workers(workers, argv[2], &argv[2]);
    cleanup_channels(workers);

    wait_for_all(workers);

    return 0;
}