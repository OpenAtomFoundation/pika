#include "tick_hb.h"
#include "tick_conf.h"
#include "status.h"

extern TickConf *g_tickConf;

TickHb::TickHb()
{
    thread_id_ = pthread_self();
    // init sock
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr_, 0, sizeof(servaddr_));

    hb_port_ = g_tickConf->hb_port();
    servaddr_.sin_family = AF_INET;
    servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr_.sin_port = htons(hb_port_);

	char* tmp = g_tickConf->seed();
	int sp = g_tickConf->seed_port();
	if (tmp != NULL) {
		srv_.push_back(Node(std::string(tmp), sp));
	}


    bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
    listen(sockfd_, 10);

    SetBlockType(kNonBlock);
    /*
     * inital the tickepoll object, add the notify_receive_fd to epoll
     */
    tickEpoll_ = new TickEpoll();

}

void TickHb::RunHb()
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


Status TickHb::DoConnect(int adj_port, char* adj_hostname, TickConn* tickConn)
{
    int sockfd, rv;
   
    char port[6];
    struct addrinfo hints, *servinfo, *p;
    snprintf(port, 6, "%d", adj_port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(adj_hostname, port, &hints, &servinfo)) != 0) {
        hints.ai_family = AF_INET6;
        if ((rv = getaddrinfo(hostname_, port, &hints, &servinfo)) != 0) {
            s = Status::IOError("tcp_connect error for ", hostname_);
            return s;
        }
    }
    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            continue;
        }
      
        int flags = fcntl(sockfd, F_GETFL, 0);
        fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            if (errno == EHOSTUNREACH) {
                close(sockfd);
                return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
            } else if (errno == EINPROGRESS && (tickConn->flags() & O_NONBLOCK)) {
                /**
                 * This branch mean the fd is NONBLOCK, so EINPROGRESS is true
                 */
            } else {
                if (errno == EINPROGRESS) {
                    struct pollfd   wfd[1];
                    long msec = (timeout_.tv_sec * 1000) + ((timeout_.tv_usec + 999) / 1000);
                    if (msec < 0 || msec > INT_MAX) {
                        msec = INT_MAX;
                    }
					wfd[0].fd = sockfd;
                    wfd[0].events = POLLOUT;
                    
                    int res;
                    if ((res = poll(wfd, 1, msec)) == -1) {
                        close(sockfd);
                        freeaddrinfo(servinfo);
                        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
                    } else if (res == 0) {
                        errno = ETIMEDOUT;
                        close(sockfd);
                        freeaddrinfo(servinfo);
                        return Status::IOError("ETIMEDOUT", "The target host connect timeout");
                    }
                    int err = 0;
                    socklen_t errlen = sizeof(err);
            
                    if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
                        freeaddrinfo(servinfo);
                        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
                    }
            
                    if (err) {
                        errno = err;
                        freeaddrinfo(servinfo);
                        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
                    }
                }
            }
        }

        flags = fcntl(sockfd, F_GETFL, 0);
        fcntl(sockfd, F_SETFL, flags & ~O_NONBLOCK);

        s = Status::OK();
        tickConn->set_fd(sockfd);
        tickConn->SetBlockType(kBlock);
        tickConn->SetTcpNoDelay();
        freeaddrinfo(servinfo);
        return s;
    }
    if (p == NULL) {
        s = Status::IOError(strerror(errno), "Can't create socket ");
        return s;
    }
    freeaddrinfo(servinfo);
    freeaddrinfo(p);
}

void TickHb::CreatePulse()
{
	vector<Node>::iterator iter;
	for (iter = srv_.begin(); iter != srv_.end(); iter++) {
		TickConn *tmp = new TickConn();
		DoConnect((*iter).host_, (*iter).port_, tmp); 
		hbConns_.push_back(tmp);
	}

}

void TickHb::RunPulse()
{

}
