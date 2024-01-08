/**
 * This file is for declarations of  common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#ifndef MIMPI_COMMON_H
#define MIMPI_COMMON_H

#include <assert.h>
#include <stdbool.h>
#include <stdnoreturn.h>

/*
    Assert that expression doesn't evaluate to -1 (as almost every system function does in case of error).

    Use as a function, with a semicolon, like: ASSERT_SYS_OK(close(fd));
    (This is implemented with a 'do { ... } while(0)' block so that it can be used between if () and else.)
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "system command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                  \
    do {                                                                                   \
        int const _errno = (expr);                                                         \
        if (_errno != 0)                                                                   \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

/* Prints (like printf) and quits. */
_Noreturn extern void fatal(const char* fmt, ...);

#define TODO fatal("UNIMPLEMENTED function %s", __func__);


/////////////////////////////////////////////
// Put your declarations here

#define ASSERT_NON_NEGATIVE(expr) \
    {                                                                                    \
        int const _errno = (expr);                                                       \
        if (_errno < 0)                                                                  \
            fatal(                                                                       \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",              \
                #expr, __func__, __FILE__, __LINE__                                      \
            );                                                                           \
    }

#define ASSERT_ERRNO_OK                                                                  \
    {                                                                                    \
        if (errno != 0)                                                                  \
            syserr(                                                                       \
                "Failed: \n\tIn function %s() in %s line %d.\n\tErrno:\n",      \
                __func__, __FILE__, __LINE__                     \
            );                                                                           \
    }

#define LOG(fmt, ...) log_info(fmt, getpid(), __func__, __FILE__, __LINE__, __VA_ARGS__)
//#define LOG(fmt, ...) asm("nop")
#define INBOUND_IF_FD(rank) (INTERFACES_FD_OFFSET + (rank) * 2 + 1)
#define OUTBOUND_IF_FD(rank) (INTERFACES_FD_OFFSET + (rank) * 2)

#define MAX_RANK 16
#define MAX_FD_NUMBER 1023
#define INTERFACES_FD_OFFSET (MAX_FD_NUMBER - (MAX_RANK) * 2)

extern const char* MIMPI_ENV_RANK;
extern const char* MIMPI_ENV_WORLD_SIZE;


void log_info(const char* fmt, ...);
void print_open_descriptors(void);
int min(int, int);



#endif // MIMPI_COMMON_H