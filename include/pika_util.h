#ifndef __PIKA_UTIL_H__
#define __PIKA_UTIL_H__
#include <cstdint>

int Setnonblocking(int sockfd);

void Calcmirror(int64_t &start, int64_t &stop, int64_t t_size);

void Fitlimit(int64_t &count, int64_t &offset, int64_t size);

#endif
