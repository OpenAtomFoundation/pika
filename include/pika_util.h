#ifndef __TICK_UTIL_H__
#define __TICK_UTIL_H__

#include "tick_define.h"

int Setnonblocking(int sockfd);

int SetBlockType(const int fd, int& flags, BlockType type);

int SafeClose(int fd);
#endif
