#include <glog/logging.h>
#include <poll.h>
#include "pika_slaveping_thread.h"
#include "pika_trysync_thread.h"
#include "pika_server.h"

extern PikaServer* g_pika_server;

bool PikaTrysyncThread::Init() {

  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd_ == -1) {
    LOG(WARNING) << "Connect master, Init, socket error: " << strerror(errno);
		return false;
	}

	int flags = fcntl(sockfd_, F_GETFL, 0);
	fcntl(sockfd_, F_SETFL, flags | O_NONBLOCK);

	int yes = 1;
	if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
		LOG(WARNING) << "Connect master, Init, setsockopt SO_REUSEADDR error: " << strerror(errno);
		return false;
	}
	if (setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
		LOG(WARNING) << "Connect master, Init, setsockopt SO_KEEPALIVE: error: " << strerror(errno);
		return false;
	}

	return true;

}

bool PikaTrysyncThread::Connect(const std::string& ip, int port) {

	struct sockaddr_in s_addr;
	memset(&s_addr, 0, sizeof(s_addr));
	s_addr.sin_family = AF_INET;
	s_addr.sin_addr.s_addr = inet_addr(ip.c_str());
	s_addr.sin_port = htons(port);

	if (-1 == connect(sockfd_, (struct sockaddr*)(&s_addr), sizeof(s_addr))) {
		if (errno == EINPROGRESS) {
			struct pollfd   wfd[1];
			wfd[0].fd     = sockfd_;
			wfd[0].events = POLLOUT;

			int res;
			if ((res = poll(wfd, 1, 500)) == -1) {
				LOG(WARNING) << "Connect master, Connect, poll error: " << strerror(errno);
				return false;
			} else if (res == 0) {
				LOG(WARNING) << "Connect master, Connect, timeout";
				return false;
			}

			int err = 0;
			socklen_t errlen = sizeof(err);
			if (getsockopt(sockfd_, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
				LOG(WARNING) << "Connect master, Connect, getsockopt error";
				return false;
			}
			if (err) {
				errno = err;
				LOG(WARNING) << "Connect master, Connect, error: " << strerror(errno);
				return false;
			}

      // err == 0 Success
		}
	}
	return true;
}

bool PikaTrysyncThread::Send() {
	char wbuf[256];
	int wbuf_len = 17;
	int wbuf_pos = 0;
	int nwritten = 0;
	strncpy(wbuf, "*1\r\n$7\r\ntrysync\r\n", wbuf_len); 

	while (1) {
		while (wbuf_len > 0) {
			nwritten = write(sockfd_, wbuf + wbuf_pos, wbuf_len - wbuf_pos);
			if (nwritten < 0) {
				break;
			}
			wbuf_pos += nwritten;
			if (wbuf_pos == wbuf_len) {
				wbuf_len = 0;
			}
		}
		if (nwritten == -1) {
			if (errno == EAGAIN) {
				continue;
			} else {
        LOG(WARNING) << "Connect master, Send, error: " <<strerror(errno);
				return false;
			}
		}
		if (wbuf_len == 0) {
			return true;
		}	
	}
}

bool PikaTrysyncThread::RecvProc() {
	char rbuf[256];
	int rbuf_pos = 0;
	int nread = 0;
	while (1) {
		nread = read(sockfd_, rbuf + rbuf_pos, 1);
	  if (nread == -1) {
    	if (errno == EAGAIN) {
				continue;
    	} else {
        LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
				return false;
    	}
		} else if (nread == 0) {
      LOG(WARNING) << "Connect master, master close the connection";
			return false;
		}

		if (rbuf[rbuf_pos] == '\n') {
			rbuf[rbuf_pos] = '\0';
			rbuf_pos--;
			if (rbuf_pos >= 0 && rbuf[rbuf_pos] == '\r') {
				rbuf[rbuf_pos] = '\0';
				rbuf_pos--;
			}
			break;
		}
		rbuf_pos++;
	}
  DLOG(INFO) << "Reply from master after trysync: " << std::string(rbuf, rbuf_pos+1);
	if (rbuf[0] == '+') {
		return true;
	} else {
		return false;
	}
}

void* PikaTrysyncThread::ThreadMain() {
  while (true) {
    if (g_pika_server->ShouldConnectMaster()) { //g_pika_server->repl_state_ == PIKA_REPL_CONNECT
      DLOG(INFO) << "Should connect master";
      if (Init()) {
        if (Connect(g_pika_server->master_ip(), g_pika_server->master_port()) && Send() && RecvProc()) {
          g_pika_server->ConnectMasterDone();
          PikaSlavepingThread t;
          t.StartThread();
          close(sockfd_);
          DLOG(INFO) << "Trysync success";
        } else {
          close(sockfd_);
        }
      }
    }
    sleep(1);
  }
  return NULL;
}
