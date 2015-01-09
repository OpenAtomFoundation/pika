#include "tick_epoll.h"
#include "tick_define.h"
#include "xdebug.h"
#include "status.h"

TickEpoll::TickEpoll()
{
    epfd_ = epoll_create(1024); 
    events_ = (struct epoll_event *)malloc(sizeof(struct epoll_event) * TICK_MAX_CLIENTS);
    if (!events_) {
        log_err("init epoll_event error");
    }
    timeout_ = 1000;

    firedevent_ = (TickFiredEvent *)malloc(sizeof(TickFiredEvent) * TICK_MAX_CLIENTS);
}

TickEpoll::~TickEpoll()
{
    free(events_);
    close(epfd_);
}

Status TickEpoll::TickAddEvent(int fd, int mask)
{
    struct epoll_event ee;
    ee.data.fd = fd;
    ee.events = mask;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ee);
    return Status::OK();
}

int TickEpoll::TickPoll()
{
    int retval, numevents = 0;
    retval = epoll_wait(epfd_, events_, TICK_MAX_CLIENTS, timeout_);
    if (retval > 0) {
        numevents = retval;
        for (int i = 0; i < numevents; i++) {
            int mask = 0;
            firedevent_[i].fd_ = (events_ + i)->data.fd;
            if ((events_ + i)->events & EPOLLIN) {
                mask |= kReadable;
            }
            if ((events_ + i)->events & EPOLLOUT) {
                mask |= kWriteable;
            }
            if ((events_ + i)->events & EPOLLERR) {
                mask |= kWriteable;
            }
            if ((events_ + i)->events & EPOLLHUP) {
                mask |= kWriteable;
            }
            firedevent_[i].mask_ = mask;
        }
    }
    return numevents;
}
