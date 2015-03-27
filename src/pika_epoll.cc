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
    log_info("TickAddEvent mask %d", mask);
    ee.events = mask;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ee) == -1) {
        log_info("Epoll add error");
        return Status::Corruption("epollAdd error");
    }
    return Status::OK();
}


Status TickEpoll::TickModEvent(int fd, int oMask, int mask)
{
    log_info("TickModEvent mask %d %d", fd, (oMask | mask));
    struct epoll_event ee;
    ee.data.u64 = 0;
    ee.data.fd = fd;
    ee.events = (oMask | mask);
    if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ee) == -1) {
        log_info("Epoll ctl error");
        return Status::Corruption("epollCtl error");
    }
    return Status::OK();
}

void TickEpoll::TickDelEvent(int fd)
{
    /*
     * Kernel < 2.6.9 need a non null event point to EPOLL_CTL_DEL
     */
    struct epoll_event ee;
    ee.data.fd = fd;
    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, &ee);
}

int TickEpoll::TickPoll()
{
    int retval, numevents = 0;
    retval = epoll_wait(epfd_, events_, TICK_MAX_CLIENTS, 500);
    if (retval > 0) {
        numevents = retval;
        for (int i = 0; i < numevents; i++) {
            int mask = 0;
            firedevent_[i].fd_ = (events_ + i)->data.fd;
            log_info("events + i events %d", (events_ + i)->events);
            if ((events_ + i)->events & EPOLLIN) {
                mask |= EPOLLIN;
            }
            if ((events_ + i)->events & EPOLLOUT) {
                mask |= EPOLLOUT;
            }
            if ((events_ + i)->events & EPOLLERR) {
                mask |= EPOLLERR;
            }
            if ((events_ + i)->events & EPOLLHUP) {
                mask |= EPOLLHUP;
            }
            firedevent_[i].mask_ = mask;
        }
    }
    return numevents;
}
