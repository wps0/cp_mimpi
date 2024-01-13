/**
 * This file is for implementation of common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#include "mimpi_common.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <dirent.h>

const char* MIMPI_ENV_RANK = "MIMPI_ENV_RANK";
const char* MIMPI_ENV_WORLD_SIZE = "MIMPI_ENV_WORLD_SIZE";

_Noreturn void syserr(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);
    fprintf(stderr, " (%d; %s)\n", errno, strerror(errno));
    exit(1);
}

_Noreturn void fatal(const char* fmt, ...)
{
    va_list fmt_args;

    fprintf(stderr, "ERROR: ");

    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt, fmt_args);
    va_end(fmt_args);

    fprintf(stderr, "\n");
    exit(1);
}

/////////////////////////////////////////////////
// Put your implementation here

void log_info(const char* fmt, ...) {
    static const char* fmt_prefix = "MIMPI =%d= (%s at %s:%d): ";

    char* fmt_buf = malloc(sizeof(char) * (strlen(fmt_prefix) + strlen(fmt)) + 1);
    strcpy(fmt_buf, fmt_prefix);
    strcat(fmt_buf, fmt);

    va_list fmt_args;
    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt_buf, fmt_args);
    va_end(fmt_args);

    free(fmt_buf);

    fflush(stderr);
}

#define MAX_PATH_LENGTH 1024

/* Print (to stderr) information about all open descriptors in current process. */
void print_open_descriptors(void)
{
    const char* path = "/proc/self/fd";

    // Iterate over all symlinks in `path`.
    // They represent open file descriptors of our process.
    DIR* dr = opendir(path);
    if (dr == NULL)
        fatal("Could not open dir: %s", path);

    struct dirent* entry;
    while ((entry = readdir(dr)) != NULL) {
        if (entry->d_type != DT_LNK)
            continue;

        // Make a c-string with the full path of the entry.
        char subpath[MAX_PATH_LENGTH];
        int ret = snprintf(subpath, sizeof(subpath), "%s/%s", path, entry->d_name);
        if (ret < 0 || ret >= (int)sizeof(subpath))
            fatal("Error in snprintf");

        // Read what the symlink points to.
        char symlink_target[MAX_PATH_LENGTH];
        ssize_t ret2 = readlink(subpath, symlink_target, sizeof(symlink_target) - 1);
        ASSERT_SYS_OK(ret2);
        symlink_target[ret2] = '\0';

        // Skip an additional open descriptor to `path` that we have until closedir().
        if (strncmp(symlink_target, "/proc", 5) == 0)
            continue;

        fprintf(stderr, "Pid %d file descriptor %3s -> %s\n",
                getpid(), entry->d_name, symlink_target);
    }

    closedir(dr);
    fflush(stderr);
    fflush(stdout);
}

int min(int a, int b) {
    return a < b ? a : b;
}

int max(int a, int b) {
    return a > b ? a : b;
}
