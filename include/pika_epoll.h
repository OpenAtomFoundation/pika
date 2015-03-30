#ifndef __PIKA_EPOLL_H__
#define __PIKA_EPOLL_H__
#include "sys/epoll.h"
#include "status.h"

typedef struct PikaFiredEvent {
    int fd_;
    int mask_;
}PikaFiredEvent;

class PikaEpoll
{

public:
    PikaEpoll();
    ~PikaEpoll();
    Status PikaAddEvent(int fd, int mask);
    void PikaDelEvent(int fd);
    Status PikaModEvent(int fd, int oMask, int mask);

    int PikaPoll();

    PikaFiredEvent *firedevent() { return firedevent_; }

private:

    int epfd_;
    struct epoll_event *events_;
    int timeout_;
    PikaFiredEvent *firedevent_;
};

#endif
