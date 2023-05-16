#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include "hiredis-vip/hiredis.h"

void Usage() {
  std::cout << "Usage:" << std::endl;
  std::cout << "      redisConn read protocol_file and send command to pika DB " << std::endl;
  std::cout << "      --f       protocol_file name" << std::endl;
  std::cout << "      --i       pika ip address" << std::endl;
  std::cout << "      --p       pika port" << std::endl;
  std::cout << "      [--a]     password for pika db; default = nullptr" << std::endl;
  std::cout << "example "
            << "./redisConn protocol_file 127.0.0.1 9221  password" << std::endl;
}
int main(int argc, char** argv) {
  if (argc < 4) {
    Usage();
    return -1;
  }
  const char* filename = argv[1];
  const char* ip = argv[2];
  std::ifstream fin(filename, std::ios::in);
  std::string strport(argv[3]);
  std::stringstream ss;
  ss << strport;
  int port;
  ss >> port;
  redisContext* c = redisConnect(ip, port);
  if (c == nullptr || (c->err != 0)) {
    if (c != nullptr) {
      redisFree(c);
      std::cout << "connection error" << std::endl;
      return -1;
    } else {
      std::cout << "conneciont error : can't allocate redis context." << std::endl;
      return -1;
    }
  }
  std::cout << "connection successful" << std::endl;
  redisReply* r;
  if (argc == 5) {
    const char* password = argv[4];
    r = static_cast<redisReply*>(redisCommand(c, "AUTH %s", password));
    if (r->type == REDIS_REPLY_ERROR) {
      std::cout << "authentication failed " << std::endl;
      freeReplyObject(r);
      redisFree(c);
      return -1;
    } else {
      std::cout << "authentication success " << std::endl;
      freeReplyObject(r);
    }
  }
  char line[1000000];
  while (fin.getline(line, sizeof(line))) {
    int k = atoi(line + 1);
    std::string command;
    for (int i = 1; i <= k; i++) {
      fin.getline(line, sizeof(line));
      fin.getline(line, sizeof(line), '\r');
      // std::string tmp(line);
      command.append(line);
      command.append(" ");
      fin.getline(line, sizeof(line));
    }
    // std::cout << command << std::endl;
    r = static_cast<redisReply*>(redisCommand(c, command.c_str()));
    if (r != nullptr) {
      freeReplyObject(r);
    }
  }
  std::cout << "transport finished" << std::endl;
  redisFree(c);
  fin.close();
  return 0;
}
