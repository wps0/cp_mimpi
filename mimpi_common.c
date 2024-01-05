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

const char* MIMPI_ENV_RANK = "MIMPI_ENV_RANK";
const char* MIMPI_ENV_WORLD_SIZE = "MIMPI_ENV_WORLD_SIZE";
const int MAX_RANK = 16;

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
    static const char* fmt_prefix = "MIMPI (%s at %s:%d): ";

    char* fmt_buf = malloc(sizeof(char) * (strlen(fmt_prefix) + strlen(fmt)) + 1);
    strcpy(fmt_buf, fmt_prefix);
    strcat(fmt_buf, fmt);

    va_list fmt_args;
    va_start(fmt_args, fmt);
    vfprintf(stderr, fmt_buf, fmt_args);
    va_end(fmt_args);

    free(fmt_buf);
}
