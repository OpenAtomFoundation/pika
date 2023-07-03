#include <errno.h>
#include <stdio.h>

#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"
#include "pstd/include/pstd_coding.h"
#include "pstd/include/xdebug.h"

using namespace net;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Usage: ./redis_cli ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int port = atoi(argv[2]);

  std::unique_ptr<NetCli> rcli(NewRedisCli());
  rcli->set_connect_timeout(3000);

  Status s = rcli->Connect(ip, port, "127.0.0.1");
  printf(" RedisCli Connect(%s:%d) return %s\n", ip.c_str(), port,
         s.ToString().c_str());
  if (!s.ok()) {
    printf("Connect failed, %s\n", s.ToString().c_str());
    exit(-1);
  }

  net::RedisCmdArgsType redis_argv;
  std::string one_command = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$2\r\nab\r\n";

  std::string binlog_body;
  pstd::PutFixed16(&binlog_body, 1);   // type
  pstd::PutFixed32(&binlog_body, 0);   // exec_time
  pstd::PutFixed32(&binlog_body, 10);  // server_id
  pstd::PutFixed64(&binlog_body, 0);   // logic_id
  pstd::PutFixed32(&binlog_body, 0);   // filenum
  pstd::PutFixed64(&binlog_body, 0);   // offset
  uint32_t content_length = one_command.size();
  pstd::PutFixed32(&binlog_body, content_length);
  binlog_body.append(one_command);

  std::string header;
  pstd::PutFixed16(&header, 2);
  pstd::PutFixed32(&header, binlog_body.size());

  std::string command = header + binlog_body;
  {
    for (size_t i = 0; i < command.size(); ++i) {
      sleep(1);
      std::string one_char_str(command, i, 1);
      s = rcli->Send(&one_char_str);
      printf("Send %d  %s\n", i, s.ToString().c_str());
    }

    s = rcli->Recv(&redis_argv);
    printf("Recv return %s\n", s.ToString().c_str());
    if (redis_argv.size() > 0) {
      printf("  redis_argv[0]  is (%s)\n", redis_argv[0].c_str());
    }
  }

  char ch;
  scanf("%c", &ch);

  return 0;
}
