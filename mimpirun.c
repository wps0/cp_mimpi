/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include "mimpi_common.h"

static int number_length(int n) {
    int len = 1;
    while (n >= 10) {
        n /= 10;
        len++;
    }

    return len;
}

static void prepare_environment(int rank, int workers) {
    const int buf_len = number_length(MAX_RANK) + 1;
    char buffer[buf_len];

    ASSERT_NON_NEGATIVE(snprintf(buffer, buf_len, "%d", rank));
    ASSERT_SYS_OK(setenv(MIMPI_ENV_RANK, buffer, 1));
    ASSERT_NON_NEGATIVE(snprintf(buffer, buf_len, "%d", workers));
    ASSERT_SYS_OK(setenv(MIMPI_ENV_WORLD_SIZE, buffer, 1));
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
    run_workers(workers, argv[2], &argv[2]);
    wait_for_all(workers);

    return 0;
}