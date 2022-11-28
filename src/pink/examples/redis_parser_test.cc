#include <stdio.h>
#include <errno.h>
#include "slash/include/xdebug.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"

using namespace pink;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf ("Usage: ./redis_parser_test ip port\n");
    exit(0);
  }

  std::string ip(argv[1]);
  int port = atoi(argv[2]);

  PinkCli *rcli = NewRedisCli();
  rcli->set_connect_timeout(3000);

  Status s = rcli->Connect(ip, port, "127.0.0.1");
  printf(" RedisCli Connect(%s:%d) return %s\n", ip.c_str(), port, s.ToString().c_str());
  if (!s.ok()) {
      printf ("Connect failed, %s\n", s.ToString().c_str());
      exit(-1);
  }

  pink::RedisCmdArgsType redis_argv;

  std::string one_command = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$2\r\nab\r\n";

  {
  printf ("\nTest Send One whole command\n");
  std::string one_command = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$2\r\nab\r\n";
  s = rcli->Send(&one_command);
  printf("Send  %s\n",  s.ToString().c_str());

  s = rcli->Recv(&redis_argv);
  printf("Recv return %s\n", s.ToString().c_str());
  if (redis_argv.size() > 0) {
    printf("  redis_argv[0]  is (%s)\n", redis_argv[0].c_str());
  }
  }

  {
  printf ("\nTest Send command into two times bulk itself break\n");
  std::string half_command = "*3\r\n$3\r\nSET\r\n$3\r\nabc\r\n$10\r\n12345";
  std::string another_half_command = "67890\r\n";
  std::string one_command_and_a_half = one_command + half_command;
  s = rcli->Send(&one_command_and_a_half);
  printf("Send  %s\n",  s.ToString().c_str());
  sleep(1);
  s = rcli->Send(&another_half_command);
  printf("Send  %s\n",  s.ToString().c_str());

  s = rcli->Recv(&redis_argv);
  printf("Recv return %s\n", s.ToString().c_str());
  if (redis_argv.size() > 0) {
    printf("  redis_argv[0]  is (%s)\n", redis_argv[0].c_str());
  }
  }

  {
  printf ("\nTest Send command into two times bulk num break\n");
  std::string half_command = "*3\r\n$3\r\nSET\r\n$1";
  std::string another_half_command = "0\r\n0123456789\r\n$10\r\n1234567890\r\n";
  std::string one_command_and_a_half = one_command + half_command;
  s = rcli->Send(&one_command_and_a_half);
  printf("Send  %s\n",  s.ToString().c_str());
  sleep(1);
  s = rcli->Send(&another_half_command);
  printf("Send  %s\n",  s.ToString().c_str());

  s = rcli->Recv(&redis_argv);
  printf("Recv return %s\n", s.ToString().c_str());
  if (redis_argv.size() > 0) {
    printf("  redis_argv[0]  is (%s)\n", redis_argv[0].c_str());
  }
  }

  {
  printf ("\nTest Send command byte by byte\n");
  std::string half_command = "*";
  std::string another_half_command = "11\r\n$4\r\nMSET\r\n$10\r\n0123456789\r\n$10\r\n1234567890\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"; 
  std::string one_command_and_a_half = one_command + half_command;
  s = rcli->Send(&one_command_and_a_half);
  printf("Send  %s\n",  s.ToString().c_str());
  for (size_t i = 0; i < another_half_command.size(); ++i) {
    sleep(1);
    std::string one_char_str(another_half_command, i, 1);
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
  scanf ("%c", &ch);

  return 0;
}
