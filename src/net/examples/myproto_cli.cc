#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>

#include "myproto.pb.h"
#include "net/include/net_cli.h"
#include "net/include/net_define.h"

using namespace net;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Usage: ./client ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int port = atoi(argv[2]);

  std::unique_ptr<NetCli> cli(NewPbCli());

  Status s = cli->Connect(ip, port);
  if (!s.ok()) {
    printf("Connect (%s:%d) failed, %s\n", ip.c_str(), port,
           s.ToString().c_str());
  }
  printf("Connect (%s:%d) ok, fd is %d\n", ip.c_str(), port, cli->fd());

  for (int i = 0; i < 100000; i++) {
    myproto::Ping msg;
    msg.set_address("127.00000");
    msg.set_port(2222);

    s = cli->Send((void*)&msg);
    if (!s.ok()) {
      printf("Send failed %s\n", s.ToString().c_str());
      break;
    }

    printf("Send sussces\n");
    myproto::PingRes req;
    s = cli->Recv((void*)&req);
    if (!s.ok()) {
      printf("Recv failed %s\n", s.ToString().c_str());
      break;
    }
    printf("Recv res %d mess (%s)\n", req.res(), req.mess().c_str());
  }
  cli->Close();

  return 0;
}
