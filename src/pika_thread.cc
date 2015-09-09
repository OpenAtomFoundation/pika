#include "pika_thread.h"
#include "mutexlock.h"
#include "port.h"
#include "pika_epoll.h"
#include "pika_define.h"
#include "csapp.h"
#include "pika_conf.h"
#include <glog/logging.h>
#include "status.h"
#include "pika_conn.h"

extern PikaConf* g_pikaConf;

PikaThread::PikaThread()
{
    thread_id_ = pthread_self();

    /*
     * inital the pikaepoll object, add the notify_receive_fd to epoll
     */
    pikaEpoll_ = new PikaEpoll();
    int fds[2];
    if (pipe(fds)) {
        LOG(FATAL) << "Can't create notify pipe";
    }
    notify_receive_fd_ = fds[0];
    notify_send_fd_ = fds[1];
    pikaEpoll_->PikaAddEvent(notify_receive_fd_, EPOLLIN | EPOLLERR | EPOLLHUP);

}

PikaThread::~PikaThread()
{
    delete(pikaEpoll_);
    mutex_.Unlock();
    close(notify_send_fd_);
    close(notify_receive_fd_);
}

void PikaThread::RunProcess()
{
    thread_id_ = pthread_self();
    int nfds;
    PikaFiredEvent *tfe = NULL;
    char bb[1];
    PikaItem ti;
    PikaConn *inConn;
    for (;;) {
        nfds = pikaEpoll_->PikaPoll();
//        log_info("nfds %d", nfds);
        for (int i = 0; i < nfds; i++) {
            tfe = (pikaEpoll_->firedevent()) + i;
//            log_info("tfe->fd_ %d tfe->mask_ %d", tfe->fd_, tfe->mask_);
            if (tfe->fd_ == notify_receive_fd_ && (tfe->mask_ & EPOLLIN)) {
                read(notify_receive_fd_, bb, 1);
                {
                MutexLock l(&mutex_);
                ti = conn_queue_.front();
                conn_queue_.pop();
                }
                PikaConn *tc = new PikaConn(ti.fd());
                tc->SetNonblock();
                conns_[ti.fd()] = tc;

                pikaEpoll_->PikaAddEvent(ti.fd(), EPOLLIN);
//                log_info("receive one fd %d", ti.fd());
                /*
                 * tc->set_thread(this);
                 */
            }
            if (tfe->mask_ & EPOLLIN) {
                inConn = conns_[tfe->fd_];
                // log_info("come if readable %d", (inConn == NULL));
                if (inConn == NULL) {
                    continue;
                }
                int ret = inConn->PikaGetRequest();
                if (ret == 1) {
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
                } else if (ret == 0) {
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN);
                } else {
                    inConn->CloseAfterReply();
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
                }
            }
//            log_info("tfe mask %d %d %d", tfe->mask_, EPOLLIN, EPOLLOUT);
            if (tfe->mask_ & EPOLLOUT) {
//                log_info("Come in the EPOLLOUT branch");
                inConn = conns_[tfe->fd_];
                if (inConn == NULL) {
                    continue;
                }
                if (inConn->PikaSendReply() == 0) {
//                    log_info("SendReply ok");
                    if (inConn->ShouldCloseAfterReply()) {
                        close(inConn->Fd());
                        delete(inConn);
                    } else {
                        pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN);
                    }
                }
            }
            if ((tfe->mask_  & EPOLLERR) || (tfe->mask_ & EPOLLHUP)) {
//                log_info("close tfe fd here");
                close(tfe->fd_);
                delete(inConn);
            }
        }
    }
}
