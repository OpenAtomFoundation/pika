#include "pika_hb.h"
#include "pika_conf.h"
#include "status.h"
#include "pika_util.h"
#include "pika_thread.h"
#include "pika_item.h"
#include "pika_worker.h"
#include "mutexlock.h"
#include <glog/logging.h>
#include "pika_conn.h"
#include "hb_context.h"
#include "bada_sdk.pb.h"
#include "pika_packet.h"

#include <poll.h>
#include <vector>


extern PikaConf *gPikaConf;

PikaHb::PikaHb(std::vector<PikaNode>* cur) :
    cur_(cur)
{
	thread_id_ = pthread_self();
	// init sock
	sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
	memset(&servaddr_, 0, sizeof(servaddr_));

	hb_port_ = gPikaConf->hb_port();
	servaddr_.sin_family = AF_INET;
	servaddr_.sin_addr.s_addr = htonl(INADDR_ANY);
	servaddr_.sin_port = htons(hb_port_);

	char* tmp = gPikaConf->seed();
	int sp = gPikaConf->seed_port();
	log_info("seed%sseed %d", tmp, sp);
	if (tmp[0] != '\0') {
		log_info("seed%sseed %d", tmp, sp);
		cur_->push_back(PikaNode(std::string(tmp), sp));
        DebugSrv();
	}
	bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
	listen(sockfd_, 10);

	timeout_.tv_sec = 1;
	timeout_.tv_usec = 500000;

	SetBlockType(sockfd_, flags_, kNonBlock);

	/*
	 * inital the pikaepoll object, add the notify_receive_fd to epoll
	 */
	pikaEpoll_ = new PikaEpoll();
	pikaEpoll_->PikaAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);

	last_thread_ = 0;
	for (int i = 0; i < PIKA_HEARTBEAT_THREAD; i++) {
		hbThread_[i] = new PikaThread();
	}
	// start the hbThread thread
	for (int i = 0; i < PIKA_HEARTBEAT_THREAD; i++) {
		pthread_create(&(hbThread_[i]->thread_id_), NULL, &(PikaThread::StartThread), hbThread_[i]);
	}

	CreatePulse();
}

PikaHb::~PikaHb()
{

}

void PikaHb::CreateHb(pthread_t* pid, PikaHb* pikaHb)
{
    pthread_create(pid, NULL, &(PikaHb::StartHb), pikaHb);
}

void* PikaHb::StartHb(void* arg)
{
    reinterpret_cast<PikaHb*>(arg)->RunProcess();
    return NULL;
}

void PikaHb::RunProcess()
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
				std::queue<PikaItem> *q = &(hbThread_[last_thread_]->conn_queue_);
				log_info("Tfe must happen");
				PikaItem ti(connfd);
				{
					MutexLock l(&hbThread_[last_thread_]->mutex_);
					q->push(ti);
				}
				write(hbThread_[last_thread_]->notify_send_fd(), "", 1);
				last_thread_++;
				last_thread_ %= PIKA_THREAD_NUM;
			} else if ((tfe + i)->mask_ & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
				LOG(WARNING) << "Epoll timeout event";
			}
		}
	}
}


Status PikaHb::DoConnect(const char* adj_hostname, int adj_port, HbContext* hbContext)
{
	Status s;
	int sockfd, rv;

	char port[6];
	struct addrinfo hints, *servinfo, *p;
	snprintf(port, 6, "%d", adj_port);
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	if ((rv = getaddrinfo(adj_hostname, port, &hints, &servinfo)) != 0) {
		hints.ai_family = AF_INET6;
		if ((rv = getaddrinfo(adj_hostname, port, &hints, &servinfo)) != 0) {
			s = Status::IOError("tcp_connect error for ", adj_hostname);
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
				continue;
			} else if (errno == EINPROGRESS && (hbContext->flags() & O_NONBLOCK)) {
				/**
				 * This branch mean the fd is NONBLOCK, so EINPROGRESS is true
				 */
			} else {
				if (errno == EINPROGRESS) {
					struct pollfd wfd[1];
					long msec = (timeout_.tv_sec * 1000) + ((timeout_.tv_usec + 999) / 1000);
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
		hbContext->set_fd(sockfd);
		hbContext->SetBlockType(kBlock);
		hbContext->SetTcpNoDelay();
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

void PikaHb::CreatePulse()
{
	Status s;
	std::vector<PikaNode>::iterator iter;
	for (iter = cur_->begin(); iter != cur_->end(); iter++) {
		HbContext *tmp = new HbContext();
		log_info("%s %d", (*iter).host()->c_str(), (*iter).port());
		s = DoConnect((*iter).host()->c_str(), (*iter).port(), tmp); 
		log_info("Status %s", s.ToString().c_str());
        if (s.ok()) {
            hbContexts_.push_back(tmp);
        }
	}
}

/*
 * void PikaHb::DestroyPulse()
 * {
 * }
 */

Status PikaHb::Pulse(HbContext* hbContext, const std::string &host, const int port)
{
	Status s;
	HbSend hbSend;
	HbSendBuild(host, port, &hbSend);
	int buf_len = 0;
	char* buf = hbContext->GetContextBuffer(&buf_len);
	log_info("hbContext fd %d", hbContext->fd());
	if (!buf) {
		s = Status::InvalidArgument("context no buffer");
		return s;
	}
	bool ret = hbSend.SerializeToArray(buf, buf_len);
	if (!ret) {
		s = Status::InvalidArgument("pb serialize error");
	}
	hbContext->BuildObuf(kHbSend, hbSend.ByteSize());
	s = hbContext->HbBufferWrite();
	return s;
}

void PikaHb::StartPulse()
{
	Status s;
	std::vector<HbContext *>::iterator iter;
	while (1) {
		log_info("%d", hbContexts_.size());
		for (iter = hbContexts_.begin(); iter != hbContexts_.end(); iter++) {
			s = Pulse(reinterpret_cast<HbContext*>(*iter), "127.0.0.1", 9876);
            /*
             * if (s.ok() != 0) {
             *     hbContexts_.erase(iter);
             * }
             */
		}
        DebugSrv();
		sleep(3);
	}
}

void PikaHb::DebugSrv()
{
    log_info("-----------DebugSrv----------\n");
    std::vector<PikaNode>::iterator iter;
    for (iter = cur_->begin(); iter != cur_->end(); iter++) {
        log_info("host %s port %d", iter->host()->c_str(), iter->port());
    }
}
