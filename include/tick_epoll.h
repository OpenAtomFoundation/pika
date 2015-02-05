#ifndef __TICK_EPOLL_H__
#define __TICK_EPOLL_H__
#include "sys/epoll.h"
#include "status.h"

typedef struct TickFiredEvent {
    int fd_;
    int mask_;
}TickFiredEvent;

class TickEpoll
{

public:
    TickEpoll();
    ~TickEpoll();
    Status TickAddEvent(int fd, int mask);
    Status TickDelEvent(int fd);

    int TickPoll();

    TickFiredEvent *firedevent() { return firedevent_; }

private:

    int epfd_;
    struct epoll_event *events_;
    int timeout_;
    TickFiredEvent *firedevent_;
};

#endif
