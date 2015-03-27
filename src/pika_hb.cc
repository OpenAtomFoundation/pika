#include "tick_hb.h"
#include "tick_conf.h"
#include "status.h"
#include "tick_util.h"
#include "tick_thread.h"
#include "tick_item.h"
#include "tick_server.h"
#include "mutexlock.h"
#include <glog/logging.h>
#include "tick_conn.h"
#include "hb_context.h"
#include "bada_sdk.pb.h"
#include "tick_packet.h"

#include <poll.h>
#include <vector>


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
	log_info("seed%sseed %d", tmp, sp);
	if (tmp[0] != '\0') {
		log_info("seed%sseed %d", tmp, sp);
		srv_.push_back(Node(std::string(tmp), sp));
	}
	bind(sockfd_, (struct sockaddr *) &servaddr_, sizeof(servaddr_));
	listen(sockfd_, 10);

	timeout_.tv_sec = 1;
	timeout_.tv_usec = 500000;

	SetBlockType(sockfd_, flags_, kNonBlock);

	/*
	 * inital the tickepoll object, add the notify_receive_fd to epoll
	 */
	tickEpoll_ = new TickEpoll();
	tickEpoll_->TickAddEvent(sockfd_, EPOLLIN | EPOLLERR | EPOLLHUP);

	last_thread_ = 0;
	for (int i = 0; i < TICK_HEARTBEAT_THREAD; i++) {
		hbThread_[i] = new TickThread();
	}
	// start the hbThread thread
	for (int i = 0; i < TICK_HEARTBEAT_THREAD; i++) {
		pthread_create(&(hbThread_[i]->thread_id_), NULL, &(TickServer::StartThread), hbThread_[i]);
	}

	CreatePulse();
	/*
	 * RunPulse();
	 */
	/*
	 * Pulse("127.0.0.1", 9876);
	 */
}

TickHb::~TickHb()
{

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
				std::queue<TickItem> *q = &(hbThread_[last_thread_]->conn_queue_);
				log_info("Tfe must happen");
				TickItem ti(connfd);
				{
					MutexLock l(&hbThread_[last_thread_]->mutex_);
					q->push(ti);
				}
				write(hbThread_[last_thread_]->notify_send_fd(), "", 1);
				last_thread_++;
				last_thread_ %= TICK_THREAD_NUM;
			} else if ((tfe + i)->mask_ & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
				LOG(WARNING) << "Epoll timeout event";
			}
		}
	}
}


Status TickHb::DoConnect(const char* adj_hostname, int adj_port, HbContext* hbContext)
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

void TickHb::CreatePulse()
{
	Status s;
	std::vector<Node>::iterator iter;
	for (iter = srv_.begin(); iter != srv_.end(); iter++) {
		HbContext *tmp = new HbContext();
		log_info("%s %d", (*iter).host_.c_str(), (*iter).port_);
		s = DoConnect((*iter).host_.c_str(), (*iter).port_, tmp); 
		log_info("Status %s", s.ToString().c_str());
		hbContexts_.push_back(tmp);
	}

}

Status TickHb::Pulse(HbContext* hbContext, const std::string &host, const int port)
{
	log_info("send here");
	Status s;
	HbSend hbSend;
	HbSendBuild(host, port, &hbSend);
	int buf_len = 0;
	char* buf = hbContext->GetContextBuffer(&buf_len);
	log_info("hbContext fd %d", hbContext->fd());
	if (!buf) { // error
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

	/*
	 * if (!s.ok()) {
	 *     return s;
	 * }
	 * s = hbContext->HbBufferRead();
	 * if (!s.ok()) {
	 *     return s;
	 * }
	 */

	/*
	 * s = SetBufferParse(hbContext->r_opcode(), hbContext->rbuf(), hbContext->rbuf_len());
	 */

}

void TickHb::RunPulse()
{
	Status s;
	std::vector<HbContext *>::iterator iter;
	while (1) {
		log_info("%d", hbContexts_.size());
		for (iter = hbContexts_.begin(); iter != hbContexts_.end(); iter++) {
			Pulse(reinterpret_cast<HbContext*>(*iter), "127.0.0.1", 9876);
		}
		sleep(3);
	}
}

void TickHb::DebugSrv()
{
	/*
	 * printf("%s", 
	 */
}
