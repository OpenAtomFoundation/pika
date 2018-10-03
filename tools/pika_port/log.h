#ifndef LOG_H__
#define LOG_H__

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

#define pline(fmt, ...)     printf(fmt "\n", ##__VA_ARGS__)

#define pinfo(fmt, ...)     \
    printf("\033[1;34;40m%s-%s-%d: " fmt "\033[0m\n", ((char*)__FILE__), \
            (char*)__func__, (int)__LINE__, ##__VA_ARGS__)

#define perr(fmt, ...)     \
    fprintf(stderr, "\033[1;31;40m%s-%s-%d: error: " fmt "\033[0m\n", \
            (char*)__FILE__, (char*)__func__, (int)__LINE__, ##__VA_ARGS__)

#define pfatal(fmt, ...)  do {   \
    fprintf(stderr, "\033[1;31;40m%s-%s-%d: error: " fmt "\033[0m\n", \
            (char*)__FILE__, (char*)__func__, (int)__LINE__, ##__VA_ARGS__); \
    kill(getpid(),SIGINT); \
  } while(0)

// exit(1)

#endif

