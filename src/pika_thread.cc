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

PikaThread::PikaThread(int thread_index)
{
    thread_index_ = thread_index;
    thread_id_ = pthread_self();

    /*
     * inital the pikaepoll object, add the notify_receive_fd to epoll
     */
    pthread_rwlock_init(&rwlock_, NULL);
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
    pthread_rwlock_destroy(&rwlock_);
    close(notify_send_fd_);
    close(notify_receive_fd_);
}

int PikaThread::ProcessTimeEvent(struct timeval* target) {
    gettimeofday(target, NULL);
    struct timeval t = *target;
    target->tv_sec++;
    if (conns_.size() == 0) {
        return 0;
    }
    std::map<int, PikaConn*>::iterator iter;
    std::map<std::string, client_info>::iterator iter_clientlist;
    int i = 0;
    iter = conns_.begin();
    while (iter != conns_.end()) {
        iter_clientlist = clients_.find(iter->second->ip_port());
        if ((iter_clientlist != clients_.end() && iter_clientlist->second.is_killed == true ) || 
        (t.tv_sec*1000000+t.tv_usec) - ((iter->second)->tv().tv_sec*1000000+(iter->second)->tv().tv_usec) >= g_pikaConf->timeout() * 1000000LL) {

            {
                RWLock l(&rwlock_, true);
                if (iter_clientlist == clients_.end()) {
                    iter_clientlist = clients_.find(iter->second->ip_port());
                }
                if (iter_clientlist != clients_.end()) {
                    LOG(INFO) << "Remove (Idle or Killed) Client: " << iter_clientlist->first;
                    clients_.erase(iter_clientlist);
                }

            }

            pikaEpoll_->PikaDelEvent(iter->second->fd());
            close(iter->second->fd());
            delete iter->second;
            iter = conns_.erase(iter);
            i++;
        } else {
            iter++;
        }
    }
    
    return i;
}

void PikaThread::RunProcess()
{
    thread_id_ = pthread_self();
    int nfds;
    PikaFiredEvent *tfe = NULL;
    char bb[1];
    PikaItem ti;
    PikaConn *inConn;
    struct timeval target;
    struct timeval now;
    gettimeofday(&target, NULL);
    target.tv_sec++;
    int timeout = 1000;
    std::map<int, PikaConn*>::iterator it;
    std::map<std::string, client_info>::iterator it_clientlist;
    for (;;) {
        gettimeofday(&now, NULL);
//        LOG(INFO) << "now sec: "<< now.tv_sec << " now use " << now.tv_usec;
//        LOG(INFO) << "target sec: "<< target.tv_sec << " target use " << target.tv_usec;
        if (target.tv_sec > now.tv_sec || (target.tv_sec == now.tv_sec && target.tv_usec - now.tv_usec > 1000)) {
            timeout = (target.tv_sec-now.tv_sec)*1000 + (target.tv_usec-now.tv_usec)/1000;
        } else {
//            LOG(INFO) << "fire TimeEvent";
            ProcessTimeEvent(&target);
            timeout = 1000;
        }
//        LOG(INFO) << "PikaPoll Timeout: " << timeout;
        nfds = pikaEpoll_->PikaPoll(timeout);
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
                PikaConn *tc = new PikaConn(ti.fd(), ti.ip_port());
                tc->SetNonblock();
                conns_[ti.fd()] = tc;
                LOG(INFO) << "Add New Client: " << tc->ip_port();
                {
                    RWLock l(&rwlock_, true);
                    clients_[tc->ip_port()] = { ti.fd(), false };
                }
                pikaEpoll_->PikaAddEvent(ti.fd(), EPOLLIN);
//                log_info("receive one fd %d", ti.fd());
                /*
                 * tc->set_thread(this);
                 */
            }

            it = conns_.find(tfe->fd_) ;
            if (it == conns_.end()) {
                continue;
            } else {
                inConn = it->second;
            }

            if (tfe->mask_ & EPOLLIN) {
                // log_info("come if readable %d", (inConn == NULL));
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
                if (inConn->PikaSendReply() == 0) {
//                    log_info("SendReply ok");
                    if (inConn->ShouldCloseAfterReply()) {
                        it = conns_.find(inConn->fd());
                        if (it != conns_.end()) {
                            conns_.erase(it);
                        }
                        close(inConn->fd());

                        it_clientlist = clients_.find(inConn->ip_port());
                        if (it_clientlist != clients_.end()) {
                            LOG(INFO) << "Remove Client: " << it_clientlist->first;
                            {
                                RWLock l(&rwlock_, true); 
                                clients_.erase(it_clientlist);
                            }
                        }
                        delete(inConn);
                    } else {
                        pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN);
                    }
                }
            }

            inConn->UpdateTv(now);

            if ((tfe->mask_  & EPOLLERR) || (tfe->mask_ & EPOLLHUP)) {
//                log_info("close tfe fd here");
                it = conns_.find(tfe->fd_);
                if (it != conns_.end()) {
                    conns_.erase(it);
                }
                close(tfe->fd_);
                delete(inConn);
            }
        }
    }
}
