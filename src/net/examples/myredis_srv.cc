#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/redis_conn.h"
#include "net/include/server_thread.h"
#include "net/src/holy_thread.h"
#include "net/src/net_multiplexer.h"

using namespace net;

std::map<std::string, std::string> db;

class MyConn : public RedisConn {
 public:
  MyConn(int fd, const std::string& ip_port, Thread* thread, void* worker_specific_data);
  virtual ~MyConn() = default;

 protected:
  int DealMessage(const RedisCmdArgsType& argv, std::string* response) override;

 private:
};

MyConn::MyConn(int fd, const std::string& ip_port, Thread* thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

int MyConn::DealMessage(const RedisCmdArgsType& argv, std::string* response) {
  printf("Get redis message ");
  for (int i = 0; i < argv.size(); i++) {
    printf("%s ", argv[i].c_str());
  }
  printf("\n");

  std::string val = "result";
  std::string res;
  // set command
  if (argv.size() == 3) {
    response->append("+OK\r\n");
    db[argv[1]] = argv[2];
  } else if (argv.size() == 2) {
    std::map<std::string, std::string>::iterator iter = db.find(argv[1]);
    if (iter != db.end()) {
      const std::string& val = iter->second;
      response->append("*1\r\n$");
      response->append(std::to_string(val.length()));
      response->append("\r\n");
      response->append(val);
      response->append("\r\n");
    } else {
      response->append("$-1\r\n");
    }
  } else {
    response->append("+OK\r\n");
  }
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_specific_data,
                                              net::NetMultiplexer* net_epoll = nullptr) const {
    return std::make_shared<MyConn>(connfd, ip_port, thread, worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  running.store(false);
  printf("server Exit");
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
    printf("server will listen to 6379\n");
  } else {
    printf("server will listen to %d\n", atoi(argv[1]));
  }
  int my_port = (argc > 1) ? atoi(argv[1]) : 6379;

  SignalSetup();

  ConnFactory* conn_factory = new MyConnFactory();

  ServerThread* my_thread = new HolyThread(my_port, conn_factory, 1000, NULL, false);
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  my_thread->StopThread();

  delete my_thread;
  delete conn_factory;

  return 0;
}
