#include "tick_util.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>


int Setnonblocking(int sockfd)
{
    int flags;
    if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0) {
        close(sockfd);
        return -1;
    }
    flags |= O_NONBLOCK;
    if (fcntl(sockfd, F_SETFL, flags) < 0) {
        close(sockfd);
        return -1;
    }
    return flags;
}

int SetBlockType(int fd, int& flags, BlockType type)
{
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0) {
        close(fd);
        return -1;
    }
    if (type == kBlock) {
        flags &= (~O_NONBLOCK);
    } else if (type == kNonBlock) {
        flags |= O_NONBLOCK;
    }
    if (fcntl(fd, F_SETFL, flags) < 0) {
        close(fd);
        return -1;
    }
    return 0;
}

int SafeClose(int fd)
{
    if (fd >= 0) {
        close(fd);
        fd = -1;
        return 0;
    }
    return -1;
}
