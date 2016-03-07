#include <glog/logging.h>
#include <poll.h>
#include "pika_slaveping_thread.h"
#include "pika_server.h"

extern PikaServer* g_pika_server;

bool PikaSlavepingThread::Init() {

  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd_ == -1) {
    LOG(WARNING) << "Ping master, Init, socket error: " << strerror(errno);
		return false;
	}

	int flags = fcntl(sockfd_, F_GETFL, 0);
	fcntl(sockfd_, F_SETFL, flags | O_NONBLOCK);

	int yes = 1;
	if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
		LOG(WARNING) << "Ping master, Init, setsockopt SO_REUSEADDR error: " << strerror(errno);
		return false;
	}
	if (setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
		LOG(WARNING) << "Ping master, Init, setsockopt SO_KEEPALIVE: error: " << strerror(errno);
		return false;
	}

	return true;

}

bool PikaSlavepingThread::Connect(const std::string& ip, int port) {

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
			if ((res = poll(wfd, 1, 2000)) == -1) {
				LOG(WARNING) << "Ping master, Connect, poll error: " << strerror(errno);
				return false;
			} else if (res == 0) {
				LOG(WARNING) << "Ping master, Connect, timeout";
				return false;
			}

			int err = 0;
			socklen_t errlen = sizeof(err);
			if (getsockopt(sockfd_, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
				LOG(WARNING) << "Ping master, Connect, getsockopt error";
				return false;
			}
			if (err) {
				errno = err;
				LOG(WARNING) << "Ping master, Connect, error: " << strerror(errno);
				return false;
			}

      // err == 0 Success
		}
	}

	int flags = fcntl(sockfd_, F_GETFL, 0);
	fcntl(sockfd_, F_SETFL, flags & (~O_NONBLOCK));

  struct timeval timeout = {1, 500000};
  if (setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == -1) {
    LOG(WARNING) << "Ping master, Init, setsockopt SO_RCVTIMEO error: " << strerror(errno);
    return false;
  }
  if (setsockopt(sockfd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == -1) {
    LOG(WARNING) << "Ping master, Init, setsockopt SO_SNDTIMEO: error: " << strerror(errno);
    return false;
  }
	return true;
}

bool PikaSlavepingThread::Send() {
	char wbuf[256];
	int wbuf_pos = 0;
	int nwritten = 0;
  if (!is_first_send_) {
    strcpy(wbuf, "*1\r\n$4\r\nping\r\n");
  } else {
    strcpy(wbuf, "*2\r\n$4\r\nspci\r\n");
    char tmp[128];
    char len[20];
    sprintf(tmp, "%ld", sid_);
    sprintf(len, "$%d\r\n", strlen(tmp));
    strcat(wbuf, len);
    strcat(wbuf, tmp);
    strcat(wbuf, "\r\n");
    is_first_send_ = false;
  }
  DLOG(INFO) << wbuf;
  int wbuf_len = strlen(wbuf);

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
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
			} else {
        LOG(WARNING) << "Ping master, Send, error: " <<strerror(errno);
			}
      return false;
		}
		if (wbuf_len == 0) {
			return true;
		}	
	}
}

bool PikaSlavepingThread::RecvProc() {
	char rbuf[256];
	int rbuf_pos = 0;
	int nread = 0;
	while (1) {
		nread = read(sockfd_, rbuf + rbuf_pos, 1);
	  if (nread == -1) {
    	if (errno == EAGAIN || errno == EWOULDBLOCK) {
    	} else {
        LOG(WARNING) << "Ping master, Recv, error: " <<strerror(errno);
    	}
      return false;
		} else if (nread == 0) {
      LOG(WARNING) << "Ping master, master close the connection";
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
  DLOG(INFO) << "Reply from master after ping: " << std::string(rbuf, rbuf_pos+1);
	if (rbuf[0] == '+') {
		return true;
	} else {
		return false;
	}
}

void* PikaSlavepingThread::ThreadMain() {
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;
  while (!IsExit() && g_pika_server->ShouldStartPingMaster()) {
    if (!IsExit() && Init()) {
      if (!IsExit() && Connect(g_pika_server->master_ip(), g_pika_server->master_port() + 200)) {
        g_pika_server->PlusMasterConnection();
        while (true) {
          if (IsExit()) {
            DLOG(INFO) << "Close Slaveping Thread now";
            close(sockfd_);
            g_pika_server->pika_binlog_receiver_thread()->KillAll();
            break;
          }
          if (Send() && RecvProc()) {
            DLOG(INFO) << "Ping master success";
            gettimeofday(&last_interaction, NULL);
          } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            gettimeofday(&now, NULL);
            if (now.tv_sec - last_interaction.tv_sec > 30) {
              //timeout;
              DLOG(INFO) << "Ping master timeout";
              close(sockfd_);
              g_pika_server->pika_binlog_receiver_thread()->KillAll();
              break;
            }
          } else {
            // error happend
            DLOG(INFO) << "Ping master error";
            close(sockfd_);
            g_pika_server->pika_binlog_receiver_thread()->KillAll();
            break;
          }
          sleep(1);
        }
        g_pika_server->MinusMasterConnection();
      } else {
        close(sockfd_);
      }
    }
  }

  return NULL;
}
