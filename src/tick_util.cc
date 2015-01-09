#include "tick_util.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>


int setnonblocking(int sockfd)
{
    if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFD, 0)|O_NONBLOCK) == -1) {
        return -1;
    }
    return 0;
}
