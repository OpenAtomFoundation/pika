#ifndef __TICK_UTIL_H__
#define __TICK_UTIL_H__

#include "tick_define.h"

int Setnonblocking(int sockfd);

int SetBlockType(int fd, int& flags, BlockType type);
#endif
