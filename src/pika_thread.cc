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

extern PikaConf* gPikaConf;

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

void PikaThread::CreateThread(pthread_t &pid, PikaThread* pikaThread)
{
    pthread_create(&pid, NULL, &(PikaThread::StartThread), pikaThread);
    return ;
}

void* PikaThread::StartThread(void* arg)
{
    reinterpret_cast<PikaThread*>(arg)->RunProcess();
    return NULL;
}

void PikaThread::RunProcess()
{
    /*
     * These parameters used to get peer host and port
     */
    struct sockaddr_in peer;
    socklen_t pLen = sizeof(peer);
    char buff[PIKA_NAME_LEN];


    struct sockaddr_in servaddr_;
    thread_id_ = pthread_self();
    int nfds;
    PikaFiredEvent *tfe = NULL;
    char bb[1];
    PikaItem ti;
    PikaConn *inConn;
    for (;;) {
        nfds = pikaEpoll_->PikaPoll();
        /*
         * log_info("nfds %d", nfds);
         */
        for (int i = 0; i < nfds; i++) {
            tfe = (pikaEpoll_->firedevent()) + i;
            log_info("tfe->fd_ %d tfe->mask_ %d", tfe->fd_, tfe->mask_);
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
                if (inConn->PikaGetRequest() == 0) {
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
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
                if (inConn->PikaSendReply() == 0) {
                    log_info("SendReply ok");
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN);
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

