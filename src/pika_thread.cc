#include "tick_thread.h"
#include "mutexlock.h"
#include "port.h"
#include "tick_epoll.h"
#include "tick_define.h"
#include "csapp.h"
#include "tick_conf.h"
#include <glog/logging.h>
#include "status.h"
#include "tick_conn.h"

extern TickConf* g_tickConf;

TickThread::TickThread()
{
    thread_id_ = pthread_self();

    /*
     * inital the tickepoll object, add the notify_receive_fd to epoll
     */
    tickEpoll_ = new TickEpoll();
    int fds[2];
    if (pipe(fds)) {
        LOG(FATAL) << "Can't create notify pipe";
    }
    notify_receive_fd_ = fds[0];
    notify_send_fd_ = fds[1];
    tickEpoll_->TickAddEvent(notify_receive_fd_, EPOLLIN | EPOLLERR | EPOLLHUP);

}

TickThread::~TickThread()
{
    delete(tickEpoll_);
    mutex_.Unlock();
    close(notify_send_fd_);
    close(notify_receive_fd_);
}

void TickThread::RunProcess()
{
    /*
     * These parameters used to get peer host and port
     */
    struct sockaddr_in peer;
    socklen_t pLen = sizeof(peer);
    char buff[TICK_NAME_LEN];


    struct sockaddr_in servaddr_;
    thread_id_ = pthread_self();
    int nfds;
    TickFiredEvent *tfe = NULL;
    char bb[1];
    TickItem ti;
    TickConn *inConn;
    for (;;) {
        nfds = tickEpoll_->TickPoll();
        /*
         * log_info("nfds %d", nfds);
         */
        for (int i = 0; i < nfds; i++) {
            tfe = (tickEpoll_->firedevent()) + i;
            log_info("tfe->fd_ %d tfe->mask_ %d", tfe->fd_, tfe->mask_);
            if (tfe->fd_ == notify_receive_fd_ && (tfe->mask_ & EPOLLIN)) {
                read(notify_receive_fd_, bb, 1);
                {
                MutexLock l(&mutex_);
                ti = conn_queue_.front();
                conn_queue_.pop();
                }
                TickConn *tc = new TickConn(ti.fd());
                tc->SetNonblock();
                conns_[ti.fd()] = tc;

                tickEpoll_->TickAddEvent(ti.fd(), EPOLLIN);
                log_info("receive one fd %d", ti.fd());
                /*
                 * tc->set_thread(this);
                 */
            }
            int shouldClose = 0;
            if (tfe->mask_ & EPOLLIN) {
                inConn = conns_[tfe->fd_];
                getpeername(tfe->fd_, (sockaddr*) &peer, &pLen);
                inet_ntop(AF_INET, &peer.sin_addr, buff, sizeof(buff));

                log_info("buff %s port %d", buff, peer.sin_port);
                log_info("come if readable %d", (inConn == NULL));
                if (inConn == NULL) {
                    continue;
                }
                if (inConn->TickGetRequest() == 0) {
                    tickEpoll_->TickModEvent(tfe->fd_, 0, EPOLLOUT);
                } else {
                    delete(inConn);
                    shouldClose = 1;
                }
            }
            log_info("tfe mask %d %d %d", tfe->mask_, EPOLLIN, EPOLLOUT);
            if (tfe->mask_ & EPOLLOUT) {
                log_info("Come in the EPOLLOUT branch");
                inConn = conns_[tfe->fd_];
                if (inConn == NULL) {
                    continue;
                }
                if (inConn->TickSendReply() == 0) {
                    log_info("SendReply ok");
                    tickEpoll_->TickModEvent(tfe->fd_, 0, EPOLLIN);
                }
            }
            if ((tfe->mask_  & EPOLLERR) || (tfe->mask_ & EPOLLHUP)) {
                log_info("close tfe fd here");
                close(tfe->fd_);
            }
            if (shouldClose) {
                log_info("close tfe fd here");
                close(tfe->fd_);
            }
        }
    }
}
