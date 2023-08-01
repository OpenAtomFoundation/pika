#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "message.pb.h"
#include "net/include/net_cli.h"
#include "net/include/net_define.h"

using namespace net;

int32_t main(int32_t argc, char* argv[]) {
  if (argc < 3) {
    printf("Usage: ./client ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int32_t port = atoi(argv[2]);

  std::unique_ptr<NetCli> cli(NewPbCli());

  Status s = cli->Connect(ip, port);
  if (!s.ok()) {
    printf("Connect (%s:%d) failed, %s\n", ip.c_str(), port, s.ToString().c_str());
  }
  for (int32_t i = 0; i < 100000000; i++) {
    Ping msg;
    msg.set_ping("ping");

    s = cli->Send((void*)&msg);
    if (!s.ok()) {
      printf("Send failed %s\n", s.ToString().c_str());
      break;
    }

    Pong req;
    s = cli->Recv((void*)&req);
    if (!s.ok()) {
      printf("Recv failed %s\n", s.ToString().c_str());
      break;
    }
    // printf ("Recv (%s)\n", req.pong().c_str());
  }
  cli->Close();

  return 0;
}
