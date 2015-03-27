#include "tick_server.h"
#include "tick_define.h"
#include "tick_util.h"
#include "tick_epoll.h"
#include "tick_item.h"
#include "tick_thread.h"
#include "tick_conf.h"
#include "mutexlock.h"
#include <glog/logging.h>
#include "status.h"

extern TickConf *g_tickConf;

Status TickServer::SetBlockType(BlockType type)
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

TickServer::TickServer()
{
    // init sock
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr_, 0, sizeof(servaddr_));

	/*
	 * The usual bind, listen process
	 */
    port_ = g_tickConf->port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(port_);
    bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
    listen(sockfd_, 10);

    SetBlockType(kNonBlock);


    // init tick epoll
    tickEpoll_ = new TickEpoll();
    tickEpoll_->TickAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);


    last_thread_ = 0;
    for (int i = 0; i < g_tickConf->thread_num(); i++) {
        tickThread_[i] = new TickThread();
    }

    options_.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options_, g_tickConf->data_path(), &db_);
    if (!s.ok()) {
        log_err("Open db failed");
    }

    // start the tickThread_ thread
    for (int i = 0; i < g_tickConf->thread_num(); i++) {
        pthread_create(&(tickThread_[i]->thread_id_), NULL, &(TickServer::StartThread), tickThread_[i]);
    }

}

TickServer::~TickServer()
{
    for (int i = 0; i < TICK_THREAD_NUM; i++) {
        delete(tickThread_[i]);
    }
    delete(tickEpoll_);
    close(sockfd_);
}

void* TickServer::StartThread(void* arg)
{
    reinterpret_cast<TickThread*>(arg)->RunProcess();
    return NULL;
}


void TickServer::RunProcess()
{
    int nfds;
    TickFiredEvent *tfe;
    Status s;
    struct sockaddr_in cliaddr;
    socklen_t clilen;
    int fd, connfd;
    for (;;) {
        nfds = tickEpoll_->TickPoll();
        tfe = tickEpoll_->firedevent();
        for (int i = 0; i < nfds; i++) {
            fd = (tfe + i)->fd_;
            if (fd == sockfd_ && ((tfe + i)->mask_ & EPOLLIN)) {
                connfd = accept(sockfd_, (struct sockaddr *) &cliaddr, &clilen);
                log_info("Accept new fd %d", connfd);
                std::queue<TickItem> *q = &(tickThread_[last_thread_]->conn_queue_);
                log_info("Tfe must happen");
                TickItem ti(connfd);
                {
                    MutexLock l(&tickThread_[last_thread_]->mutex_);
                    q->push(ti);
                }
                write(tickThread_[last_thread_]->notify_send_fd(), "", 1);
                last_thread_++;
                last_thread_ %= TICK_THREAD_NUM;
            } else if ((tfe + i)->mask_ & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                LOG(WARNING) << "Epoll timeout event";
            }
        }
    }
}
