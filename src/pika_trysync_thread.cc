#include <glog/logging.h>
#include <poll.h>
#include "pika_slaveping_thread.h"
#include "pika_trysync_thread.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

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

static void ConstructSyncCmd(char* wbuf) {
  char tmp[100];
  char len[10];

  std::string requirepass = g_pika_conf->requirepass();
  if (requirepass != "") {
    strcpy(wbuf, "*2\r\n$4\r\nauth\r\n");
    sprintf(tmp, "$%ld\r\n", requirepass.length());
    strcat(wbuf, tmp);
    strcat(wbuf, requirepass.c_str());
    strcat(wbuf, "\r\n");
  }

  strcat(wbuf, "*5\r\n$7\r\ntrysync\r\n");
  sprintf(tmp, "$%ld\r\n", g_pika_server->host().length());
  strcat(wbuf, tmp);
  strcat(wbuf, g_pika_server->host().c_str());

  sprintf(len, "%d", g_pika_server->port());
  sprintf(tmp, "\r\n$%ld\r\n", strlen(len));
  strcat(wbuf, tmp);
  strcat(wbuf, len);

  uint32_t filenum;
  uint64_t pro_offset;
  g_pika_server->logger_->GetProducerStatus(&filenum, &pro_offset);

  sprintf(len, "%u", filenum);
  sprintf(tmp, "\r\n$%ld\r\n", strlen(len));
  strcat(wbuf, tmp);
  strcat(wbuf, len);

  sprintf(len, "%lu", pro_offset);
  sprintf(tmp, "\r\n$%ld\r\n", strlen(len));
  strcat(wbuf, tmp);
  strcat(wbuf, len);
  strcat(wbuf, "\r\n");
}

bool PikaTrysyncThread::Send() {
	char wbuf[256];
  memset(wbuf, 256, '0');
  ConstructSyncCmd(wbuf);
  DLOG(INFO) << wbuf;
	int wbuf_len = strlen(wbuf);
	int wbuf_pos = 0;
	int nwritten = 0;

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
  bool should_auth = g_pika_conf->requirepass() == "" ? false : true;
  bool is_authed = true;
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
      if(should_auth) {
        if (rbuf[0] == '-') {
          is_authed = false; // auth success;
          break;
        }
        memset(rbuf, 256, '0');
        rbuf_pos = -1;
      } else {
        break;
      }
		}
		rbuf_pos++;
	}
  DLOG(INFO) << "Reply from master after trysync: " << std::string(rbuf, rbuf_pos+1);
	
  if (is_authed && rbuf[0] == ':') {
    std::string t(rbuf+1, rbuf_pos);
    slash::string2l(rbuf+1, rbuf_pos, &sid_);
    DLOG(INFO) << "Recv sid from master: " << sid_;
		return true;
	} else {
    g_pika_server->RemoveMaster();
		return false;
	}
}

void* PikaTrysyncThread::ThreadMain() {
  while (true) {
    if (g_pika_server->ShouldConnectMaster()) { //g_pika_server->repl_state_ == PIKA_REPL_CONNECT
      sleep(2);
      DLOG(INFO) << "Should connect master";
      if (Init()) {
        if (Connect(g_pika_server->master_ip(), g_pika_server->master_port()) && Send() && RecvProc()) {
          g_pika_server->ConnectMasterDone();
          delete g_pika_server->ping_thread_;
          g_pika_server->ping_thread_ = new PikaSlavepingThread(sid_);
          g_pika_server->ping_thread_->StartThread();
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
