#include "pika_define.h"
#include "pika_util.h"
#include "util.h"
#include "pika_epoll.h"
#include "pika_item.h"
#include "pika_thread.h"
#include "pika_conf.h"
#include "pika_conn.h"
#include "mutexlock.h"
#include "pika_server.h"
#include "mario_handler.h"
#include <glog/logging.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <poll.h>
#include <iostream>
#include <fstream>

extern PikaConf *g_pikaConf;
extern mario::Mario *g_pikaMario;

Status PikaServer::SetBlockType(BlockType type)
{
    Status s;
    if ((flags_ = fcntl(sockfd_, F_GETFL, 0)) < 0) {
        s = Status::Corruption("F_GETFEL error");
        close(sockfd_);
        return s;
    }
    if (type == kBlock) {
        flags_ &= (~O_NONBLOCK);
    } else if (type == kNonBlock) {
        flags_ |= O_NONBLOCK;
    }
    if (fcntl(sockfd_, F_SETFL, flags_) < 0) {
        s = Status::Corruption("F_SETFL error");
        close(sockfd_);
        return s;
    }
    return Status::OK();
}

PikaServer::PikaServer()
{
    // init statistics variables
  //  stat_numcommands = 0;
  //  stat_numconnections = 0;
  //  
  //  stat_keyspace_hits = 0;
  //  stat_keyspace_misses = 0;

    // init sock
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr_, 0, sizeof(servaddr_));
    int yes = 1;
    if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        LOG(FATAL) << "setsockopt SO_REUSEADDR: " << strerror(errno);
    }
    port_ = g_pikaConf->port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(port_);

    int ret = bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
    if (ret < 0) {
        LOG(FATAL) << "bind error: "<< strerror(errno);
    }
    listen(sockfd_, 10);

    SetBlockType(kNonBlock);

    // init pika epoll
    pikaEpoll_ = new PikaEpoll();
    pikaEpoll_->PikaAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);


    last_thread_ = 0;
    for (int i = 0; i < g_pikaConf->thread_num(); i++) {
        pikaThread_[i] = new PikaThread(i);
    }

    ms_state_ = PIKA_REP_SINGLE;
    repl_state_ = PIKA_SINGLE;
    dump_filenum_ = 0;
    dump_pro_offset_ = 0;
    bgsaving_ = false;
//    options_.create_if_missing = true;
//    options_.write_buffer_size = 1500000000;
//    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
//    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
//    db_ = new nemo::Nemo("/tmp/testdb");
    nemo::Options option;
    option.write_buffer_size = g_pikaConf->write_buffer_size();
    LOG(WARNING) << "Prepare DB...";
    db_ = new nemo::Nemo(g_pikaConf->db_path(), option);
    LOG(WARNING) << "DB Success";
//    if (!s.ok()) {
//        log_err("Open db failed");
//    }

    // start the pikaThread_ thread
    for (int i = 0; i < g_pikaConf->thread_num(); i++) {
        pthread_create(&(pikaThread_[i]->thread_id_), NULL, &(PikaServer::StartThread), pikaThread_[i]);
    }

}

PikaServer::~PikaServer()
{
    for (int i = 0; i < g_pikaConf->thread_num(); i++) {
        delete(pikaThread_[i]);
    }
    delete(pikaEpoll_);
    close(sockfd_);
    pthread_rwlock_destroy(&rwlock_);
}

std::string PikaServer::GetServerIp() {
    struct ifreq ifr;
    strcpy(ifr.ifr_name, "eth0");
    if (ioctl(sockfd_, SIOCGIFADDR, &ifr) !=  0) {
        LOG(FATAL) << "ioctl error";
    }
    return std::string(inet_ntoa(((struct sockaddr_in*)&(ifr.ifr_addr))->sin_addr));
}

int PikaServer::GetServerPort() {
    return port_;
}

bool PikaServer::LoadDb(std::string& path) {
    RWLock l(&rwlock_, true);
    nemo::Options option;
    option.write_buffer_size = g_pikaConf->write_buffer_size();
    LOG(WARNING) << "Prepare open new db...";
    nemo::Nemo *t_db = new nemo::Nemo(path, option);
    LOG(WARNING) << "open new db success";
    nemo::Nemo *t = db_;
    db_ = t_db;
    delete t;
    return true;
}

void PikaServer::Dump() {
    MutexLock l(&mutex_);
    if (bgsaving_) {
        return;
    }
    nemo::Snapshots snapshots;
    {
        RWLock l(&rwlock_, true);
        g_pikaMario->GetProducerStatus(&dump_filenum_, &dump_pro_offset_);
        db_->BGSaveGetSnapshot(snapshots);
        bgsaving_ = true;
    }
    bgsaving_start_time_ = time(NULL);
    strftime(dump_time_, sizeof(dump_time_), "%Y%m%H%M%S",localtime(&bgsaving_start_time_)); 
//    LOG(INFO) << tmp;
    dump_args *arg = new dump_args;
    arg->p = (void*)this;
    arg->snapshots = snapshots;
    pthread_create(&dump_thread_id_, NULL, &(PikaServer::StartDump), arg);
}

void* PikaServer::StartDump(void* arg) {
    PikaServer* p = (PikaServer*)(((dump_args*)arg)->p);
    nemo::Snapshots s = ((dump_args*)arg)->snapshots;
    std::string dump_path("./dump/");
    dump_path.append(g_pikaConf->dump_prefix());
    dump_path.append(p->dump_time_);
    LOG(INFO) << dump_path;
    p->GetHandle()->BGSave(s, dump_path);
    std::ofstream out;
    out.open(dump_path + "/info", std::ios::in | std::ios::trunc);
    if (out.is_open()) {
        out << p->GetServerIp() << "\r\n";
        out << p->GetServerPort() << "\r\n";
        out << p->dump_filenum_ << "\r\n";
        out << p->dump_pro_offset_ << "\r\n";
        out.close();
    }
    {
    MutexLock l(p->Mutex());
    p->bgsaving_ = false;
    }
    delete (dump_args*)arg;
    return NULL;
}

void PikaServer::Slaveofnoone() {
    MutexLock l(&mutex_);
    if (repl_state_ == PIKA_SLAVE) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", masterport_);
        std::string masterport(buf);
        std::string masteripport = masterhost_ + ":" + masterport;
        ClientKill(masteripport);
        repl_state_ = PIKA_SINGLE;
    }
    ms_state_ = PIKA_SINGLE;
    masterhost_ = "";
    masterport_ = 0;
}

std::string PikaServer::is_bgsaving() {
    MutexLock l(&mutex_);
    std::string s;
    if (bgsaving_) {
        s = "Yes, ";
        s.append(dump_time_);
        time_t delta = time(NULL) - bgsaving_start_time_;
        char buf[32];
        snprintf(buf, sizeof(buf), "%lu", delta);
        s.append(", ");
        s.append(buf);
    } else {
        s = "No, ";
        s.append(dump_time_);
        s.append(", 0");

    }
    return s;
}

void PikaServer::ProcessTimeEvent(struct timeval* target) {
    std::string ip_port;
    char buf[32];
    target->tv_sec++;
    {
    MutexLock l(&mutex_);
    if (ms_state_ == PIKA_REP_CONNECT) {
        //connect
        LOG(INFO) << "try to connect with master: " << masterhost_ << ":" << masterport_;
        struct sockaddr_in s_addr;
        int connfd = socket(AF_INET, SOCK_STREAM, 0);
        if (connfd == -1) {
            LOG(WARNING) << "socket error " << strerror(errno);
            return ;
        }
        memset(&s_addr, 0, sizeof(s_addr));
        if (connfd == -1) {
            return ;
        }
        int flags = fcntl(connfd, F_GETFL, 0);
        fcntl(connfd, F_SETFL, flags | O_NONBLOCK);
        int yes = 1;
        if (setsockopt(connfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
            LOG(WARNING) << "setsockopt SO_REUSEADDR: " << strerror(errno);
            return ;
        }
        if (setsockopt(connfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
            LOG(WARNING) << "setsockopt SO_KEEPALIVE: " << strerror(errno);
            return ;
        }
        s_addr.sin_family = AF_INET;
        s_addr.sin_addr.s_addr = inet_addr(masterhost_.c_str());
        s_addr.sin_port = htons(masterport_);
        if (-1 == connect(connfd, (struct sockaddr*)(&s_addr), sizeof(s_addr))) {
            if (errno == EINPROGRESS) {
                struct pollfd   wfd[1];
                wfd[0].fd     = connfd;
                wfd[0].events = POLLOUT;

                int res;
                if ((res = poll(wfd, 1, 600)) == -1) {
                    close(connfd);
                    LOG(WARNING) << "The target host cannot be reached";
                    return ;
                } else if (res == 0) {
                    errno = ETIMEDOUT;
                    close(connfd);
                    LOG(WARNING) << "The target host connect timeout";
                    return ;
                }
                int err = 0;
                socklen_t errlen = sizeof(err);

                if (getsockopt(connfd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
                    LOG(WARNING) << "The target host cannot be reached";
                    return ; 
                }

                if (err) {
                    errno = err;
                    LOG(WARNING) << "The target host cannot be reached";
                    return ;
                }
            }
        }
         
        char ipAddr[INET_ADDRSTRLEN] = "";
        ip_port = inet_ntop(AF_INET, &s_addr.sin_addr, ipAddr, sizeof(ipAddr));
        ip_port.append(":");
        ll2string(buf, sizeof(buf), ntohs(s_addr.sin_port));
        ip_port.append(buf);
        std::queue<PikaItem> *q = &(pikaThread_[last_thread_]->conn_queue_);
        PikaItem ti(connfd, ip_port, PIKA_MASTER);
        {
            MutexLock l(&pikaThread_[last_thread_]->mutex_);
            q->push(ti);
        }
        write(pikaThread_[last_thread_]->notify_send_fd(), "", 1);
        repl_state_ = PIKA_SLAVE;
        ms_state_ = PIKA_REP_CONNECTING;
    }
    }
}

void PikaServer::DisconnectFromMaster() {
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", masterport_);
    std::string str_port(buf);
    std::string master_ip_port = masterhost_ + ":" + str_port;
    ClientKill(master_ip_port);
    {
    MutexLock l(&mutex_);
    masterhost_ = "";
    masterport_ = 0;
    repl_state_ = PIKA_SINGLE;
    ms_state_ = PIKA_REP_SINGLE;
    }
}

int PikaServer::TrySync(/*std::string &ip, std::string &str_port,*/ int fd, uint64_t filenum, uint64_t offset) {
//    std::string ip_port = ip + ":" + str_port;
//    std::map<std::string, SlaveItem>::iterator iter = slaves_.find(ip_port);
//    int64_t port;
//    string2l(str_port.data(), str_port.size(), &port);
//    if (iter != slaves_.end()) {
//        return PIKA_REP_STRATEGY_ALREADY;
//    }
    std::map<int, PikaConn*>::iterator iter_fd;
    PikaConn* conn = NULL;
    int i = 0;
    for (i = 0; i < g_pikaConf->thread_num(); i++) {
        iter_fd = pikaThread_[i]->conns()->find(fd);
        if (iter_fd != pikaThread_[i]->conns()->end()) {
            conn = iter_fd->second;
            break;
        }
    }
    if (conn == NULL) {
        return PIKA_REP_STRATEGY_ERROR;
    }

    std::map<std::string, client_info>::iterator iter_cl;
    MarioHandler* h = new MarioHandler(conn);
    mario::Status s = g_pikaMario->AddConsumer(filenum, offset, h, fd);
    if (s.ok()) {
        {
        MutexLock l(&mutex_);
        set_repl_state(PIKA_MASTER);
        }
//        {
//        MutexLock l(&mutex_);
//        SlaveItem ss;
//        ss.ip = ip;
//        ss.port = port;
//        ss.state = PIKA_REP_CONNECTED;
//        slaves_[ip_port] = ss;
//        std::map<std::string, SlaveItem>::iterator ii = slaves_.begin();
//        while (ii != slaves_.end()) {
//            LOG(INFO) << ii->first << " " << (ii->second).ip << " " <<  (ii->second).port <<" "<< (ii->second).state;
//            ii++;
//        }
//        }
        {
        RWLock l(pikaThread_[i]->rwlock(), true);
        iter_cl = pikaThread_[i]->clients()->find(iter_fd->second->ip_port());
        if (iter_cl != pikaThread_[i]->clients()->end()) {
            LOG(INFO) << "Set client role to slave";
            iter_cl->second.role = PIKA_SLAVE;
        }
        }
        conn->set_role(PIKA_SLAVE);
        return PIKA_REP_STRATEGY_PSYNC;
    } else {
        return PIKA_REP_STRATEGY_ERROR;
    }
}

//void PikaServer::Offline(std::string ip_port) {
//    std::map<std::string, SlaveItem>::iterator iter = slaves_.find(ip_port);
//    if (iter != slaves_.end()) {
//        iter->second.state = PIKA_REP_OFFLINE;
//    }
//}

int PikaServer::ClientList(std::string &res) {
    int client_num = 0;
    std::map<std::string, client_info>::iterator iter;
    res = "+";
    char buf[32];
    for (int i = 0; i < g_pikaConf->thread_num(); i++) {

        {
            RWLock l(pikaThread_[i]->rwlock(), false);
            iter = pikaThread_[i]->clients()->begin();
            while (iter != pikaThread_[i]->clients()->end()) {
                res.append("addr=");
                res.append(iter->first);
                res.append(" fd=");
                ll2string(buf,sizeof(buf), (iter->second).fd);
                res.append(buf);
                res.append("\n");
                client_num++;
                iter++;
            }
        }
    }
    res.append("\r\n");

    return client_num;
}

int PikaServer::ClientKill(std::string &ip_port) {
    int i = 0;
    std::map<std::string, client_info>::iterator iter;
    for (i = 0; i < g_pikaConf->thread_num(); i++) {
        {
            RWLock l(pikaThread_[i]->rwlock(), true);
            iter = pikaThread_[i]->clients()->find(ip_port);
            if (iter != pikaThread_[i]->clients()->end()) {
                (iter->second).is_killed = true;
                break;
            }
        }
    }
    if (i < g_pikaConf->thread_num()) {
        return 1;
    } else {
        return 0;
    }

}

//int PikaServer::ClientRole(int fd, int role) {
//    int i = 0;
//    std::map<int, PikaConn*>::iterator iter_fd;
//    std::map<std::string, client_info>::iterator iter;
//    for (i = 0; i < g_pikaConf->thread_num(); i++) {
//
//        if (role == CLIENT_MASTER) {
//            RWLock l(pikaThread_[i]->rwlock(), false);
//            iter = pikaThread_[i]->clients()->begin();
//            while (iter != pikaThread_[i]->clients()->end()) {
//                if (iter->second.role == CLIENT_MASTER && iter->second.fd != fd) {
//                    iter->second.role = CLIENT_NORMAL;
//                    iter->second.is_killed = true;
//                    break;
//                }
//                iter++;
//            }
//        }
//
//        {
//            iter_fd = pikaThread_[i]->conns()->find(fd);
//            if (iter_fd != pikaThread_[i]->conns()->end()) {
//                RWLock l(pikaThread_[i]->rwlock(), true);
//                iter = pikaThread_[i]->clients()->find(iter_fd->second->ip_port());
//                if (iter != pikaThread_[i]->clients()->end()) {
//                    (iter->second).role = role;
//                    break;
//                }
//            }
//        }
//    }
//    if (i < g_pikaConf->thread_num()) {
//        return 1;
//    } else {
//        return 0;
//    }
//
//}

void* PikaServer::StartThread(void* arg)
{
    reinterpret_cast<PikaThread*>(arg)->RunProcess();
    return NULL;
}


void PikaServer::RunProcess()
{
    int nfds;
    PikaFiredEvent *tfe;
    Status s;
    struct sockaddr_in cliaddr;
    socklen_t clilen = sizeof(cliaddr);
    int fd, connfd;
    char ipAddr[INET_ADDRSTRLEN] = "";
    std::string ip_port;
    char buf[32];

    struct timeval target;
    struct timeval now;
    gettimeofday(&target, NULL);
    target.tv_sec++;
    int timeout = 1000;
    for (;;) {
        gettimeofday(&now, NULL);
        if (target.tv_sec > now.tv_sec || (target.tv_sec == now.tv_sec && target.tv_usec - now.tv_usec > 1000)) {
            timeout = (target.tv_sec-now.tv_sec)*1000 + (target.tv_usec-now.tv_usec)/1000;
        } else {
            ProcessTimeEvent(&target);
            timeout = 1000;
        }
        nfds = pikaEpoll_->PikaPoll(timeout);
        tfe = pikaEpoll_->firedevent();
        for (int i = 0; i < nfds; i++) {
            fd = (tfe + i)->fd_;
            if (fd == sockfd_ && ((tfe + i)->mask_ & EPOLLIN)) {
                connfd = accept(sockfd_, (struct sockaddr *) &cliaddr, &clilen);
//                LOG(INFO) << "Accept new connection, fd: " << connfd << " ip: " << inet_ntop(AF_INET, &cliaddr.sin_addr, ipAddr, sizeof(ipAddr)) << " port: " << ntohs(cliaddr.sin_port);
                ip_port = inet_ntop(AF_INET, &cliaddr.sin_addr, ipAddr, sizeof(ipAddr));
                ip_port.append(":");
                ll2string(buf, sizeof(buf), ntohs(cliaddr.sin_port));
                ip_port.append(buf);
                std::queue<PikaItem> *q = &(pikaThread_[last_thread_]->conn_queue_);
                PikaItem ti(connfd, ip_port);
                {
                    MutexLock l(&pikaThread_[last_thread_]->mutex_);
                    q->push(ti);
                }
                write(pikaThread_[last_thread_]->notify_send_fd(), "", 1);
                last_thread_++;
                last_thread_ %= g_pikaConf->thread_num();
            } else if ((tfe + i)->mask_ & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                LOG(WARNING) << "Epoll timeout event";
            }
        }
    }
}
