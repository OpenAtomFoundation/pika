#include "tick_util.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>


int Setnonblocking(int sockfd)
{
    if ((flags = fcntl(sockfd, F_GETFL, 0)) < 0) {
        close(sockfd);
        return -1;
    }
    if (type == kBlock) {
        flags &= (~O_NONBLOCK);
    } else if (type == kNonBlock) {
        flags |= O_NONBLOCK;
    }
    if (fcntl(sockfd, F_SETFL, flags) < 0) {
        close(sockfd);
        return -1;
    }
    return flags;
}

