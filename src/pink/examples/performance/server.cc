#include <stdio.h>
#include <unistd.h>
#include <atomic>
#include <sys/time.h>
#include <stdint.h>
#include <signal.h>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"
#include "message.pb.h"

using namespace pink;
using namespace std;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static atomic<int> num(0);

class PingConn : public PbConn {
 public:
  PingConn(int fd, std::string ip_port, pink::ServerThread* pself_thread = NULL) : 
    PbConn(fd, ip_port, pself_thread) {
  }
  virtual ~PingConn() {}

  int DealMessage() {
    num++;
    request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

    response_.Clear();
    response_.set_pong("hello " + request_.ping());
    res_ = &response_;

    set_is_reply(true);

    return 0;
  }

 private:

  Ping request_;
  Pong response_;

  PingConn(PingConn&);
  PingConn& operator=(PingConn&);
};


class PingConnFactory : public ConnFactory {
 public:
  virtual PinkConn *NewPinkConn(
      int connfd,
      const std::string &ip_port,
      ServerThread *thread,
      void* worker_specific_data) const {
    return new PingConn(connfd, ip_port, thread);
  }
};

std::atomic<bool> should_stop(false);

static void IntSigHandle(const int sig) {
  should_stop.store(true);
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf ("Usage: ./server ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int port = atoi(argv[2]);

  PingConnFactory conn_factory;

  SignalSetup();

  ServerThread *st_thread = NewDispatchThread(ip, port, 24, &conn_factory, 1000);
  st_thread->StartThread();
  uint64_t st, ed;

  while (!should_stop) {
    st = NowMicros();
    int prv = num.load();
    sleep(1);
    printf("num %d\n", num.load());
    ed = NowMicros();
    printf("mmap cost time microsecond(us) %lld\n", ed - st);
    printf("average qps %lf\n", (double)(num.load() - prv) / ((double)(ed - st) / 1000000));
  }
  st_thread->StopThread();

  delete st_thread;

  return 0;
}
