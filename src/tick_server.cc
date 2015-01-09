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
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&servaddr_, 0, sizeof(servaddr_));

    port_ = g_tickConf->port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(port_);

    bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));

    SetBlockType(kNonBlock);

    // init tick epoll
    tickEpoll_ = new TickEpoll();
    tickEpoll_->TickAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);

    // init the rbuf
    rbuf_ = (char *)malloc(sizeof(char) * TICK_MAX_MESSAGE);

    last_thread_ = 0;
    for (int i = 0; i < TICK_THREAD_NUM; i++) {
        tickThread_[i] = new TickThread();
    }

    // start the tickThread_ thread
    for (int i = 0; i < TICK_THREAD_NUM; i++) {
        pthread_create(&(tickThread_[i]->thread_id_), NULL, &(TickServer::StartThread), tickThread_[i]);
    }

}

TickServer::~TickServer()
{
    for (int i = 0; i < TICK_THREAD_NUM; i++) {
        delete(tickThread_[i]);
    }
    free(rbuf_);
    delete(tickEpoll_);
    close(sockfd_);
}

void* TickServer::StartThread(void* arg)
{
    reinterpret_cast<TickThread*>(arg)->RunProcess();
    return NULL;
}

Status TickServer::TickReadHeader(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread;
    header_len_ = 0;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_HEADER_LENGTH);
        // log_info("nread %d", nread);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read command header error");
                return s;
            }
        } else if (nread == 0){
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    memcpy((char *)(&integer), buf, sizeof(int32_t));
    header_len_ = ntohl(integer);
    return Status::OK();
}


Status TickServer::TickReadCode(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread = 0;
    r_opcode_ = 0;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_CODE_LENGTH);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read command code error");
                return s;
            }
        } else if (nread == 0){
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    memcpy((char *)(&integer), buf, sizeof(int32_t));
    r_opcode_ = ntohl(integer);
    return Status::OK();
}

Status TickServer::TickReadPacket(rio_t *rio)
{
    Status s;
    int nread = 0;
    if (header_len_ < 4) {
        return Status::Corruption("The packet no integrity");
    }
    while (1) {
        nread = rio_readnb(rio, (void *)(rbuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH), header_len_ - 4);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read data error");
                return s;
            }
        } else if (nread == 0) {
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    rbuf_len_ = nread;
    return Status::OK();
}

Status TickServer::BuildObuf()
{
    uint32_t code_len = COMMAND_CODE_LENGTH + rbuf_len_;
    uint32_t u;

    u = htonl(code_len);
    memcpy(rbuf_, &u, sizeof(uint32_t));
    u = htonl(r_opcode_);
    memcpy(rbuf_ + COMMAND_CODE_LENGTH, &u, sizeof(uint32_t));

    return Status::OK();
}

void TickServer::RunProcess()
{
    int nfds;
    TickFiredEvent *tfe;
    Status s;
    for (;;) {
        nfds = tickEpoll_->TickPoll();
        tfe = tickEpoll_->firedevent();
        for (int i = 0; i < nfds; i++) {
            if (tfe->mask_ & EPOLLIN) {
                rio_t rio;
                rio_readinitb(&rio, sockfd_);
                s = TickReadHeader(&rio);
                if (!s.ok()) {
                    LOG(ERROR) << s.ToString();
                    continue;
                }
                s = TickReadCode(&rio);
                if (!s.ok()) {
                    LOG(ERROR) << s.ToString();
                    continue;
                }
                s = TickReadPacket(&rio);
                if (s.ok()) {
                    std::queue<TickItem *> *q = &(tickThread_[last_thread_]->conn_queue_);
                    BuildObuf();
                    TickItem *ti = new TickItem(rbuf_, COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH + rbuf_len_);
                    {
                    MutexLock l(&tickThread_[last_thread_]->mutex_);
                    q->push(ti);
                    }
                    write(tickThread_[last_thread_]->notify_send_fd(), "", 1);
                    last_thread_++;
                    last_thread_ %= TICK_THREAD_NUM;
                } else {
                    LOG(ERROR) << s.ToString();
                }
            }
        }
    }
}
