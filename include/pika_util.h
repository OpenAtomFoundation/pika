#ifndef __PIKA_UTIL_H__
#define __PIKA_UTIL_H__

#include "pika_define.h"

int Setnonblocking(int sockfd);

int SetBlockType(const int fd, int& flags, BlockType type);

int SafeClose(int fd);
#endif
