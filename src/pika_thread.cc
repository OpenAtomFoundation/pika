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
#include "mario.h"

extern PikaConf* g_pikaConf;
extern PikaServer *g_pikaServer;
extern mario::Mario *g_pikaMario;

PikaThread::PikaThread(int thread_index)
{
    thread_index_ = thread_index;
    thread_id_ = pthread_self();
    is_master_thread_ = false;
    is_first_ = false;

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
    gettimeofday(&last_ping_time_, NULL);
    querynums_ = 0;
    last_sec_querynums_ = 0;
    accumulative_querynums_ = 0;
}

PikaThread::~PikaThread()
{
    delete(pikaEpoll_);
//    mutex_.Unlock();
    pthread_rwlock_destroy(&rwlock_);
    close(notify_send_fd_);
    close(notify_receive_fd_);
}

int PikaThread::ProcessTimeEvent(struct timeval* target) {
    gettimeofday(target, NULL);
    struct timeval t = *target;
    target->tv_sec++;

    {
      //RWLock l(&rwlock_, true);
      last_sec_querynums_ = {querynums_.load()};
      accumulative_querynums_ += querynums_;
      querynums_ = 0;
      //LOG(WARNING) << "current=" << current << ", last_sec_querynums=" << last_sec_querynums_
      //    << ", accumulative=" << accumulative_querynums_ << ", querynums_ should be 0, = " << querynums_;
    }

  //  if (conns_.size() == 0) {
  //      return 0;
  //  }

    std::map<int, PikaConn*>::iterator iter;
    std::map<std::string, client_info>::iterator iter_clientlist;
    int i = 0;
    iter = conns_.begin();
    bool should_ping = false;
    if (t.tv_sec - last_ping_time_.tv_sec >= 10) {
        last_ping_time_.tv_sec = t.tv_sec;
        should_ping = true;
    }
    while (iter != conns_.end()) {

        //querynums_ += iter->second->querynums();
        //iter->second->clear_querynums();

        //if (should_ping && thread_index_ >= g_pikaConf->thread_num()) {
            //std::cout << "iter->second->role() == PIKA_MASTER: " << (iter->second->role() == PIKA_MASTER) << std::endl;
            //std::cout << "g_pikaServer->ms_state_ == PIKA_REP_CONNECTED: " << (g_pikaServer->ms_state_ == PIKA_REP_CONNECTED) << std::endl;
            //std::cout << "iter->second->role() == PIKA_SLAVE: " << (iter->second->role() == PIKA_SLAVE) << std::endl;
        if (should_ping && ((iter->second->role() == PIKA_MASTER && (g_pikaServer->ms_state_ == PIKA_REP_CONNECTED || g_pikaServer->ms_state_ == PIKA_REP_CONNECTING)) || (iter->second->role() == PIKA_SLAVE))) {
            LOG(INFO)<<"Send Ping to " << iter->second->ip_port();
            iter->second->append_wbuf_nowait("*1\r\n$4\r\nPING\r\n");
            LOG(INFO) << "length of rbuf_: " << iter->second->rbuflen();
            LOG(INFO) << "length of wbuf_: " << iter->second->wbuflen();
        }
        //}
        iter_clientlist = clients_.find(iter->second->ip_port());

        // graceful shutdown
        if (g_pikaServer->shutdown && iter_clientlist != clients_.end()) {
          iter_clientlist->second.is_killed = true;
        }

        if ((iter_clientlist != clients_.end() && iter_clientlist->second.is_killed == true ) ||  
        ((iter->second->role() == PIKA_SINGLE) && ((t.tv_sec*1000000+t.tv_usec) - ((iter->second)->tv().tv_sec*1000000+(iter->second)->tv().tv_usec) >= g_pikaConf->timeout() * 1000000LL)) || 
        ((iter->second->role() != PIKA_SINGLE) && ((t.tv_sec*1000000+t.tv_usec) - ((iter->second)->tv().tv_sec*1000000+(iter->second)->tv().tv_usec) >= 30 * 1000000LL))) {
            
            bool is_killed = iter_clientlist->second.is_killed;
            {
                RWLock l(&rwlock_, true);
                if (iter_clientlist == clients_.end()) {
                    iter_clientlist = clients_.find(iter->second->ip_port());
                }
                if (iter_clientlist != clients_.end()) {
                    LOG(INFO) << "Remove (Idle or Killed) Client: " << iter_clientlist->first;
                    clients_.erase(iter_clientlist);
                    g_pikaServer->client_num_--;
                }

            }
            if (iter->second->role() == PIKA_MASTER) {
                LOG(WARNING) << "Remove Timeout/killed Master";
                MutexLock l(g_pikaServer->Mutex());
                if (is_killed) {
                    g_pikaServer->ms_state_ = PIKA_REP_SINGLE;
                } else {
                    g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
                }
            } else if (iter->second->role() == PIKA_SLAVE) {
                LOG(WARNING) << "Remove Slave Consumer Thread";
                if (!iter->second->ShouldCloseAfterReply()) {
                    iter->second->CloseAfterReply(); //avoid dead lock
                }
                g_pikaMario->RemoveConsumer(iter->second->fd());
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
    
    if (g_pikaServer->shutdown) {
      LOG(INFO) << "Shutdown a worker thread with tid: " << pthread_self();
      g_pikaServer->worker_num--;
      pthread_exit(NULL);
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
                PikaConn *tc = new PikaConn(this, ti.fd(), ti.ip_port(), ti.role());
                tc->SetNonblock();
                if (ti.role() == PIKA_MASTER) {
                    is_master_thread_ = true;
                    is_first_ = true;
                }
                conns_[ti.fd()] = tc;
                {
                    RWLock l(&rwlock_, true);
                    if (ti.role() == PIKA_MASTER) {
                        LOG(WARNING) << "Add Master " << tc->ip_port();
                        clients_[tc->ip_port()] = { ti.fd(), false, PIKA_MASTER };
                    } else {
                        LOG(INFO) << "Add New Client: " << tc->ip_port();
                        clients_[tc->ip_port()] = { ti.fd(), false, PIKA_SINGLE };
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
//                LOG(INFO) << "read event";
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
//                LOG(INFO) << "write event";
                if (inConn->role() == PIKA_MASTER && is_master_thread_ && is_first_) {
                    is_first_ = false;
                    char buf[32];
                    char buf_len[32];
                    std::string str;
                    std::string auth = g_pikaConf->requirepass();
                    if (auth.size() == 0) {
                        str = "*5\r\n$8\r\npikasync\r\n";
                    } else {
                        str = "*2\r\n$9\r\nslaveauth\r\n";
                        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", auth.size());
                        str.append(buf_len);
                        str.append(auth);
                        str.append("\r\n*5\r\n$8\r\npikasync\r\n");
                    }
                    uint32_t filenum = 0;
                    uint64_t pro_offset = 0;
                    g_pikaMario->GetProducerStatus(&filenum, &pro_offset);

                    int len = ll2string(buf, sizeof(buf), filenum); 
                    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                    str.append(buf_len);
                    snprintf(buf, sizeof(buf), "%d\r\n", filenum);
                    str.append(buf);

                    len = ll2string(buf, sizeof(buf), pro_offset); 
                    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                    str.append(buf_len);
                    snprintf(buf, sizeof(buf), "%lu\r\n", pro_offset);
                    str.append(buf);

                    /*
                    std::string slave_db_sync_path = g_pikaConf->slave_db_sync_path();
                    if (slave_db_sync_path.at(0) == '.') {
                        char cwd_buf[100];
                        getcwd(cwd_buf, sizeof(cwd_buf));
                        if (cwd_buf[strlen(cwd_buf)-1] == '/') {
                            cwd_buf[strlen(cwd_buf)-1] = '\0';
                        }
                        slave_db_sync_path.erase(slave_db_sync_path.begin());
                        slave_db_sync_path = std::string(cwd_buf, strlen(cwd_buf)) + slave_db_sync_path;
                    }
                    if(slave_db_sync_path.back() == '/' && slave_db_sync_path.size() > 1) {
                        slave_db_sync_path.erase(slave_db_sync_path.size()-1);
                    }
                    */
                    char pid_str[10];
                    snprintf(pid_str, sizeof(pid_str), "%d", getpid());
                    std::string slave_db_sync_path = "pika_slave_db_sync_path_";
                    slave_db_sync_path.append(pid_str);

                    len = slave_db_sync_path.size();
                    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                    str.append(buf_len);
                    slave_db_sync_path.append("\r\n");
                    str.append(slave_db_sync_path);

                    uint32_t rsync_port = g_pikaConf->port() + 300;
                    len = ll2string(buf, sizeof(buf), rsync_port);
                    snprintf(buf_len, sizeof(buf_len), "$%d\r\n", len);
                    str.append(buf_len);
                    snprintf(buf, sizeof(buf), "%u\r\n", rsync_port);
                    str.append(buf);

                    LOG(WARNING)<<str;
                    inConn->append_wbuf_nowait(str);
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
                        pikaEpoll_->PikaDelEvent(inConn->fd());

                        it_clientlist = clients_.find(inConn->ip_port());
                        if (it_clientlist != clients_.end()) {
                            LOG(INFO) << "Remove Client: " << it_clientlist->first << " " << inConn->ip_port();
                            {
                                RWLock l(&rwlock_, true); 
                                clients_.erase(it_clientlist);
                            }
                            g_pikaServer->client_num_--;
                            LOG(INFO) << "Remove Client OK";
                        }
                        if (role == PIKA_MASTER) {
                            LOG(WARNING) << "Remove Master";
                            MutexLock l(g_pikaServer->Mutex());
                            g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
                        } else if (role == PIKA_SLAVE) {
                            LOG(WARNING) << "Remove Slave Consumer Thread";
                            mario::Status s = g_pikaMario->RemoveConsumer(inConn->fd());
                            LOG(WARNING) << s.ToString();
                        }
                        close(inConn->fd());
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
                LOG(INFO) << "error event happen: " << tfe->mask_;
                it = conns_.find(tfe->fd_);
                int role = PIKA_SINGLE;
                if (it != conns_.end()) {
                    role = it->second->role();
                    conns_.erase(it);
                } else {
                    continue;
                }
                pikaEpoll_->PikaDelEvent(tfe->fd_);
                it_clientlist = clients_.find(inConn->ip_port());
                if (it_clientlist != clients_.end()) {
                    LOG(INFO) << "Remove Client: " << it_clientlist->first;
                    {
                        RWLock l(&rwlock_, true);
                        clients_.erase(it_clientlist);
                    }
                }
                if (role == PIKA_MASTER) {
                    LOG(WARNING) << "Remove Master";
                    MutexLock l(g_pikaServer->Mutex());
                    g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
                } else if (role == PIKA_SLAVE) {
                    LOG(WARNING) << "Remove Slave Consumer Thread";
                    if (!inConn->ShouldCloseAfterReply()) {
                        inConn->CloseAfterReply(); //avoid dead lock
                    }
                    mario::Status s = g_pikaMario->RemoveConsumer(inConn->fd());
                    LOG(WARNING) << s.ToString();
                }
                close(tfe->fd_);
                delete(inConn);
            }
        }
    }
}
