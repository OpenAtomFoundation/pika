#include "pika_epoll.h"
#include "pika_define.h"
#include "xdebug.h"
#include "status.h"

PikaEpoll::PikaEpoll()
{
    epfd_ = epoll_create(1024); 
    events_ = (struct epoll_event *)malloc(sizeof(struct epoll_event) * PIKA_MAX_CLIENTS);
    if (!events_) {
        log_err("init epoll_event error");
    }
    timeout_ = 1000;

    firedevent_ = (PikaFiredEvent *)malloc(sizeof(PikaFiredEvent) * PIKA_MAX_CLIENTS);
}

PikaEpoll::~PikaEpoll()
{
    free(events_);
    free(firedevent_);
    close(epfd_);
}

Status PikaEpoll::PikaAddEvent(int fd, int mask)
{
    struct epoll_event ee;
    ee.data.fd = fd;
//    log_info("PikaAddEvent mask %d", mask);
    ee.events = mask;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ee) == -1) {
//        log_info("Epoll add error");
        return Status::Corruption("epollAdd error");
    }
    return Status::OK();
}


Status PikaEpoll::PikaModEvent(int fd, int oMask, int mask)
{
//    log_info("PikaModEvent mask %d %d", fd, (oMask | mask));
    struct epoll_event ee;
    ee.data.u64 = 0;
    ee.data.fd = fd;
    ee.events = (oMask | mask);
    if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ee) == -1) {
//        log_info("Epoll ctl error");
        return Status::Corruption("epollCtl error");
    }
    return Status::OK();
}

void PikaEpoll::PikaDelEvent(int fd)
{
    /*
     * Kernel < 2.6.9 need a non null event point to EPOLL_CTL_DEL
     */
    struct epoll_event ee;
    ee.data.fd = fd;
    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, &ee);
}

int PikaEpoll::PikaPoll(int timeout)
{
    int retval, numevents = 0;
    retval = epoll_wait(epfd_, events_, PIKA_MAX_CLIENTS, timeout);
    if (retval > 0) {
        numevents = retval;
        for (int i = 0; i < numevents; i++) {
            int mask = 0;
            firedevent_[i].fd_ = (events_ + i)->data.fd;
//            log_info("events + i events %d", (events_ + i)->events);
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
