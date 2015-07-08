#include "pika_define.h"
#include "pika_util.h"
#include "pika_epoll.h"
#include "pika_item.h"
#include "pika_thread.h"
#include "pika_conf.h"
#include "mutexlock.h"
#include "pika_server.h"
#include <glog/logging.h>

extern PikaConf *g_pikaConf;

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
    // init sock
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr_, 0, sizeof(servaddr_));

    port_ = g_pikaConf->port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(port_);

    bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
    listen(sockfd_, 10);

    SetBlockType(kNonBlock);

    // init pika epoll
    pikaEpoll_ = new PikaEpoll();
    pikaEpoll_->PikaAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);


    last_thread_ = 0;
    for (int i = 0; i < g_pikaConf->thread_num(); i++) {
        pikaThread_[i] = new PikaThread();
    }

    options_.create_if_missing = true;
    options_.write_buffer_size = 1500000000;
//    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
    leveldb::Status s = leveldb::DB::Open(options_, "/tmp/testdb", &db_);
    if (!s.ok()) {
        log_err("Open db failed");
    }

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
    socklen_t clilen;
    int fd, connfd;
    for (;;) {
        nfds = pikaEpoll_->PikaPoll();
        tfe = pikaEpoll_->firedevent();
        for (int i = 0; i < nfds; i++) {
            fd = (tfe + i)->fd_;
            if (fd == sockfd_ && ((tfe + i)->mask_ & EPOLLIN)) {
                connfd = accept(sockfd_, (struct sockaddr *) &cliaddr, &clilen);
                log_info("Accept new fd %d", connfd);
                std::queue<PikaItem> *q = &(pikaThread_[last_thread_]->conn_queue_);
                log_info("Tfe must happen");
                PikaItem ti(connfd);
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
