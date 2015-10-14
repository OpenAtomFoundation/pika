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

//    options_.create_if_missing = true;
//    options_.write_buffer_size = 1500000000;
//    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
//    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
//    db_ = new nemo::Nemo("/tmp/testdb");
    nemo::Options option;
    option.write_buffer_size = g_pikaConf->write_buffer_size();
    db_ = new nemo::Nemo(g_pikaConf->db_path(), option);
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

int PikaServer::TrySync(std::string &ip, std::string &str_port) {
    MutexLock l(&mutex_);
    std::string ip_port = ip + ":" + str_port;
    std::map<std::string, SlaveItem>::iterator iter = slaves_.find(ip_port);
    int64_t port;
    string2l(str_port.data(), str_port.size(), &port);
    if (iter != slaves_.end()) {
        if (iter->second.state != PIKA_REP_OFFLINE) {
            return PIKA_REP_STRATEGY_ALREADY;
        } else {
            iter->second.state = PIKA_REP_CONNECTED;
        }
    } else { 
        SlaveItem ss;
        ss.ip = ip;
        ss.port = port;
        slaves_[ip_port] = ss;
    }
    MarioHandler* h = new MarioHandler(ip, port);
    if (h->pika_connect() != 0) {
//        delete h;
        slaves_[ip_port].state = PIKA_REP_OFFLINE;
        return PIKA_REP_STRATEGY_ERROR;
    };
    mario::Status s = g_pikaMario->AddConsumer(0, 0, h);
    if (s.ok()) {
        slaves_[ip_port].state = PIKA_REP_CONNECTED;
        set_repl_state(PIKA_MASTER);
        return PIKA_REP_STRATEGY_PSYNC;
    } else {
        return PIKA_REP_STRATEGY_ERROR;
    }
}

void PikaServer::Offline(std::string ip_port) {
    std::map<std::string, SlaveItem>::iterator iter = slaves_.find(ip_port);
    if (iter != slaves_.end()) {
        iter->second.state = PIKA_REP_OFFLINE;
    }
}

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

int PikaServer::ClientRole(int fd, int role) {
    int i = 0;
    std::map<int, PikaConn*>::iterator iter_fd;
    std::map<std::string, client_info>::iterator iter;
    for (i = 0; i < g_pikaConf->thread_num(); i++) {

        if (role == CLIENT_MASTER) {
            RWLock l(pikaThread_[i]->rwlock(), false);
            iter = pikaThread_[i]->clients()->begin();
            while (iter != pikaThread_[i]->clients()->end()) {
                if (iter->second.role == CLIENT_MASTER && iter->second.fd != fd) {
                    iter->second.role = CLIENT_NORMAL;
                    iter->second.is_killed = true;
                    break;
                }
                iter++;
            }
        }

        {
            iter_fd = pikaThread_[i]->conns()->find(fd);
            if (iter_fd != pikaThread_[i]->conns()->end()) {
                RWLock l(pikaThread_[i]->rwlock(), true);
                iter = pikaThread_[i]->clients()->find(iter_fd->second->ip_port());
                if (iter != pikaThread_[i]->clients()->end()) {
                    (iter->second).role = role;
                    break;
                }
            }
        }
    }
    if (i < g_pikaConf->thread_num()) {
        return 1;
    } else {
        return 0;
    }

}

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
    for (;;) {
        nfds = pikaEpoll_->PikaPoll();
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
