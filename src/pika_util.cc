#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include "pika_util.h"
#include "pika_define.h"


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

void Calcmirror(int64_t &start, int64_t &stop, int64_t t_size) {
    int64_t t_start = stop >= 0 ? stop : t_size + stop;
    int64_t t_stop = start >= 0 ? start : t_size + start;
    t_start = t_size - 1 - t_start;
    t_stop = t_size - 1 - t_stop;
    start = t_start >= 0 ? t_start : t_start - t_size;
    stop = t_stop >= 0 ? t_stop : t_stop - t_size;
}

void Fitlimit(int64_t &count, int64_t &offset, int64_t size) {
    count = count >= 0  ? count : size;  
    offset = (offset >= 0 && offset < size) ? offset : size;  
    count = offset + count <= size ? count : size - offset;    
}


