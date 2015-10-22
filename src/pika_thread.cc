#include "pika_thread.h"
#include "mutexlock.h"
#include "port.h"
#include "pika_epoll.h"
#include "pika_define.h"
#include "pika_server.h"
#include "util.h"
#include "csapp.h"
#include "pika_conf.h"
#include <glog/logging.h>
#include "status.h"
#include "pika_conn.h"

extern PikaConf* g_pikaConf;
extern PikaServer *g_pikaServer;

PikaThread::PikaThread(int thread_index)
{
    thread_index_ = thread_index;
    thread_id_ = pthread_self();
    is_master_thread_ = false;
    is_first_ = false;
    crontimes_ = 0;

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
    crontimes_ = (crontimes_+1)%10;
    while (iter != conns_.end()) {
        if (crontimes_ == 0 && iter->second->role() != PIKA_SINGLE) {
            LOG(INFO)<<"Send Ping";
            iter->second->append_wbuf("*1\r\n$4\r\nPING\r\n");
        }
        iter_clientlist = clients_.find(iter->second->ip_port());
        if ((iter_clientlist != clients_.end() && iter_clientlist->second.is_killed == true ) ||  
        ((t.tv_sec*1000000+t.tv_usec) - ((iter->second)->tv().tv_sec*1000000+(iter->second)->tv().tv_usec) >= g_pikaConf->timeout() * 1000000LL)) {

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
            if (iter->second->role() == PIKA_MASTER) {
                LOG(INFO) << "Remove Timeout Master";
                MutexLock l(g_pikaServer->Mutex());
                g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
//            } else if (iter->second->role() == PIKA_SLAVE) {
//                MutexLock l(g_pikaServer->Mutex());
//                std::map<std::string, SlaveItem>::iterator ii = g_pikaServer->slaves()->begin();
//                while (ii != g_pikaServer->slaves()->end()) {
//                    LOG(INFO) << ii->first << " " << (ii->second).ip << " " <<  (ii->second).port <<" "<< (ii->second).state;
//                    ii++;
//                }
//                std::map<std::string, SlaveItem>::iterator iter_slave = g_pikaServer->slaves()->find(iter->second->ip_port());
//                if (iter_slave != g_pikaServer->slaves()->end()) {
//                    g_pikaServer->slaves()->erase(iter_slave);
//                    LOG(INFO) << "Remove Timeout SLave";
//                }
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
                PikaConn *tc = new PikaConn(ti.fd(), ti.ip_port(), ti.role());
                tc->SetNonblock();
                if (ti.role() == PIKA_MASTER) {
                    is_master_thread_ = true;
                    is_first_ = true;
                }
                conns_[ti.fd()] = tc;
                {
                    RWLock l(&rwlock_, true);
                    if (ti.role() == PIKA_MASTER) {
                        LOG(INFO) << "Add Master " << tc->ip_port();
                        clients_[tc->ip_port()] = { ti.fd(), false, CLIENT_MASTER };
                    } else {
                        LOG(INFO) << "Add New Client: " << tc->ip_port();
                        clients_[tc->ip_port()] = { ti.fd(), false, CLIENT_NORMAL };
                    }
                }
                if (ti.role() != PIKA_MASTER) {
                    pikaEpoll_->PikaAddEvent(ti.fd(), EPOLLIN);
                } else {
                    pikaEpoll_->PikaAddEvent(ti.fd(), EPOLLOUT);
                }
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
                inConn->UpdateTv(now);
                // log_info("come if readable %d", (inConn == NULL));
//                if (inConn->role() != PIKA_SINGLE) {
//                    inConn->UpdateLastInteraction();
//                }
                int ret = inConn->PikaGetRequest();
                if (ret == 0) {
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
                } else {
                    inConn->CloseAfterReply();
                    pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
                }
            }
//            log_info("tfe mask %d %d %d", tfe->mask_, EPOLLIN, EPOLLOUT);
            if (tfe->mask_ & EPOLLOUT) {
//                log_info("Come in the EPOLLOUT branch");
                if (inConn->role() == PIKA_MASTER && is_master_thread_ && is_first_) {
                    is_first_ = false;
//                    char buf[32];
//                    char buf_len[32];
//                    std::string serverip = g_pikaServer->GetServerIp();
//                    int serverport = g_pikaServer->GetServerPort();
//                    int len = ll2string(buf, sizeof(buf), serverport); 
//                    std::string str = "*5\r\n$8\r\npikasync\r\n";
//                    snprintf(buf, sizeof(buf), "$%lu\r\n", serverip.size());
//                    str.append(buf);
//                    str.append(serverip.data(), serverip.size());
//
//                    snprintf(buf_len, sizeof(buf_len), "\r\n$%d\r\n", len);
//                    str.append(buf_len);
//
//                    snprintf(buf, sizeof(buf), "%d\r\n", serverport);
//                    str.append(buf);
//                    str.append("$1\r\n0\r\n$1\r\n0\r\n");
                    std::string str = "*3\r\n$8\r\npikasync\r\n$1\r\n0\r\n$1\r\n0\r\n";
                    LOG(INFO)<<str;
                    inConn->append_wbuf(str);
                }
                if (inConn->PikaSendReply() == 0) {
//                    log_info("SendReply ok");
                    if (inConn->ShouldCloseAfterReply()) {
                        it = conns_.find(inConn->fd());
                        int role = PIKA_SINGLE;
                        if (it != conns_.end()) {
                            role = it->second->role();
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
                        if (role == PIKA_MASTER) {
                            LOG(INFO) << "Remove Master";
                            MutexLock l(g_pikaServer->Mutex());
                            g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
//                        } else if (role == PIKA_SLAVE) {
//                            MutexLock l(g_pikaServer->Mutex());
//                            std::map<std::string, SlaveItem>::iterator ii = g_pikaServer->slaves()->begin();
//                            while (ii != g_pikaServer->slaves()->end()) {
//                                LOG(INFO) << ii->first << " " << (ii->second).ip << " " <<  (ii->second).port <<" "<< (ii->second).state;
//                                ii++;
//                            }
//                            LOG(INFO) << "inConn->ip_port(): " << inConn->ip_port();
//                            std::map<std::string, SlaveItem>::iterator iter_slave = g_pikaServer->slaves()->find(inConn->ip_port());
//                            if (iter_slave != g_pikaServer->slaves()->end()) {
//                                g_pikaServer->slaves()->erase(iter_slave);
//                                LOG(INFO) << "Remove Slave";
//                            }
                        }
                        delete(inConn);
                    } else {
                        if (inConn->role() != PIKA_SLAVE) {
                            pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN);
                        } else {
                            pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN | EPOLLOUT);
                        }
                    }
                } else {
                    if (inConn->role() != PIKA_SLAVE) {
                        pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLOUT);
                    } else {
                        pikaEpoll_->PikaModEvent(tfe->fd_, 0, EPOLLIN | EPOLLOUT);
                    }
                }
            }


            if ((tfe->mask_  & EPOLLERR) || (tfe->mask_ & EPOLLHUP)) {
//                log_info("close tfe fd here");
                it = conns_.find(tfe->fd_);
                int role = PIKA_SINGLE;
                if (it != conns_.end()) {
                    role = it->second->role();
                    conns_.erase(it);
                }
                close(tfe->fd_);
                it_clientlist = clients_.find(inConn->ip_port());
                if (it_clientlist != clients_.end()) {
                    LOG(INFO) << "Remove Client: " << it_clientlist->first;
                    {
                        RWLock l(&rwlock_, true);
                        clients_.erase(it_clientlist);
                    }
                }
                if (role == PIKA_MASTER) {
                    LOG(INFO) << "Remove Master";
                    MutexLock l(g_pikaServer->Mutex());
                    g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
//                } else if (role == PIKA_SLAVE) {
//                    MutexLock l(g_pikaServer->Mutex());
//                    std::map<std::string, SlaveItem>::iterator ii = g_pikaServer->slaves()->begin();
//                    while (ii != g_pikaServer->slaves()->end()) {
//                        LOG(INFO) << ii->first << " " << (ii->second).ip << " " <<  (ii->second).port <<" "<< (ii->second).state;
//                        ii++;
//                    }
//                    std::map<std::string, SlaveItem>::iterator iter_slave = g_pikaServer->slaves()->find(inConn->ip_port());
//                    if (iter_slave != g_pikaServer->slaves()->end()) {
//                        g_pikaServer->slaves()->erase(iter_slave);
//                        LOG(INFO) << "Remove Slave";
//                    }
                }
                delete(inConn);
            }
        }
    }
}
