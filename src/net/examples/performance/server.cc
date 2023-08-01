#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <type_traits>

#include "message.pb.h"
#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/pb_conn.h"
#include "net/include/server_thread.h"

using namespace net;
using namespace std;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static atomic<int32_t> num(0);

class PingConn : public PbConn {
 public:
  PingConn(int32_t fd, std::string ip_port, net::ServerThread* pself_thread = nullptr) : PbConn(fd, ip_port, pself_thread) {}
  virtual ~PingConn() {}

  int32_t DealMessage() {
    num++;
    request_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);

    response_.Clear();
    response_.set_pong("hello " + request_.ping());
    // res_ = &response_;

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
  virtual std::shared_ptr<NetConn> NewNetConn(int32_t connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_specific_data,
                                              NetMultiplexer* net_mpx = nullptr) const override {
    return std::make_shared<PingConn>(connfd, ip_port, dynamic_cast<ServerThread*>(thread));
  }
};

std::atomic<bool> should_stop(false);

static void IntSigHandle(const int32_t sig) { should_stop.store(true); }

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int32_t main(int32_t argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: ./server ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int32_t port = atoi(argv[2]);

  PingConnFactory conn_factory;

  SignalSetup();

  std::unique_ptr<ServerThread> st_thread(NewDispatchThread(ip, port, 24, &conn_factory, 1000));
  st_thread->StartThread();
  uint64_t st, ed;

  while (!should_stop) {
    st = NowMicros();
    int32_t prv = num.load();
    sleep(1);
    printf("num %d\n", num.load());
    ed = NowMicros();
    printf("mmap cost time microsecond(us) %lld\n", ed - st);
    printf("average qps %lf\n", (double)(num.load() - prv) / ((double)(ed - st) / 1000000));
  }
  st_thread->StopThread();

  return 0;
}
