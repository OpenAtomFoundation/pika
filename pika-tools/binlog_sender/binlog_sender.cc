//  Copyright (c) 2018-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "chrono"
#include "ctime"
#include "iostream"

#include "stdlib.h"
#include "unistd.h"
#include "stdint.h"
#include "string.h"

#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

#include "utils.h"
#include "progress_thread.h"
#include "binlog_consumer.h"
#include "binlog_transverter.h"


std::string binlog_path = "./log/";
std::string ip = "127.0.0.1";
uint32_t port = 6379;
std::string files_to_send = "0";
int64_t file_offset = 0;
std::string start_time_str = "2001-01-01 00:01:01";
std::string end_time_str = "2100-01-01 00:00:01";
bool need_auth = false;
std::string pass_wd;

net::PinkCli* cli = nullptr;
BinlogConsumer* binlog_consumer = nullptr;
ProgressThread* progress_thread = nullptr;

void PrintInfo(const std::time_t& now) {
  std::cout << "===================== Binlog Sender ====================" << std::endl;
  std::cout << "Ip : " << ip << std::endl;
  std::cout << "Port : " << port << std::endl;
  std::cout << "Password : " << pass_wd << std::endl;
  std::cout << "Binlog_path: " << binlog_path << std::endl;
  std::cout << "Files_to_send: " << files_to_send << std::endl;
  std::cout << "File_offset : " << file_offset << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tBinlog_sender reads from pika's binlog and send to pika/redis server" << std::endl;
  std::cout << "\tYou can build a new pika back to any timepoint with this tool" << std::endl;
  std::cout << "\t-h    -- displays this help information and exits" << std::endl;
  std::cout << "\t-n    -- input binlog path" << std::endl;
  std::cout << "\t-i    -- ip of the pika server" << std::endl;
  std::cout << "\t-p    -- port of the pika server" << std::endl;
  std::cout << "\t-f    -- files to send, default = 0" << std::endl;
  std::cout << "\t-o    -- the offset that the first file starts sending" << std::endl;
  std::cout << "\t-s    -- start time , default: '2001-00-00 00:59:01'" << std::endl;
  std::cout << "\t-e    -- end time , default: '2100-01-30 24:00:01'" << std::endl;
  std::cout << "\t-a    -- password of the pika server" << std::endl;
  std::cout << "\texample1: ./binlog_sender -n ./log -i 127.0.0.1 -p 9221 -f 526 -o 8749409" << std::endl;
  std::cout << "\texample2: ./binlog_sender -n ./log -i 127.0.0.1 -p 9221 -f 526-530 -s '2001-10-11 11:11:11' -e '2020-12-11 11:11:11'" << std::endl;
}

void TryToCommunicate() {

  cli = net::NewRedisCli();
  cli->set_connect_timeout(3000);
  Status net_s = cli->Connect(ip, port, "");
  if (!net_s.ok()) {
    std::cout << "Connect failed " << net_s.ToString().c_str() << ", exit..." << std::endl;
    exit(-1);
  }
  std::cout << "Connect success..." << std::endl;

  net::RedisCmdArgsType argv;
  if (need_auth) {
    std::string ok = "ok";
    std::string auth_cmd;
    argv.clear();
    argv.push_back("auth");
    argv.push_back(pass_wd);
    net::SerializeRedisCommand(argv, &auth_cmd);

    net_s = cli->Send(&auth_cmd);
    net_s = cli->Recv(&argv);
    if (argv.size() == 1 && !strcasecmp(argv[0].c_str(), ok.c_str())) {
      std::cout << "Try communicate success..." << std::endl;
    } else {
      std::cout << "Auth failed..." << std::endl;
      for (const auto& arg : argv) {
        std::cout << arg << std::endl;
      }
      std::cout << "exit..." << std::endl;
      exit(-1);
    }
  } else {
    std::string pong = "pong";
    std::string ping_cmd;
    argv.clear();
    argv.push_back("ping");
    net::SerializeRedisCommand(argv, &ping_cmd);

    net_s = cli->Send(&ping_cmd);
    net_s = cli->Recv(&argv);
    if (argv.size() == 1 && !strcasecmp(argv[0].c_str(), pong.c_str())) {
      std::cout << "Try communicate success..." << std::endl;
    } else {
      std::cout << "Ping failed..." << std::endl;
      for (const auto& arg : argv) {
        std::cout << arg << std::endl;
      }
      std::cout << "exit..." << std::endl;
      exit(-1);
    }
  }
}

int main(int argc, char *argv[]) {

  int32_t opt;
  while ((opt = getopt(argc, argv, "hi:p:n:f:s:e:o:a:")) != -1) {
    switch (opt) {
      case 'h' :
        Usage();
        exit(0);
      case 'i' :
        ip = optarg;
        break;
      case 'p' :
        port = std::atoi(optarg);
        break;
      case 'n' :
        binlog_path = optarg;
        if (!binlog_path.empty() && binlog_path.back() != '/') {
          binlog_path += "/";
        }
        break;
      case 'f' :
        files_to_send = optarg;
        break;
      case 's' :
        start_time_str = optarg;
        break;
      case 'e' :
        end_time_str = optarg;
        break;
      case 'o' :
        file_offset = std::atoi(optarg);
        break;
      case 'a' :
        need_auth = true;
        pass_wd = optarg;
        break;
      default:
        break;
    }
  }
  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  std::vector<uint32_t> files;
  if (!CheckFilesStr(files_to_send)
    || !GetFileList(files_to_send, &files)) {
    std::cout << "input illlegal binlog scope of the sequence, exit..." << std::endl;
    exit(-1);
  }

  uint32_t tv_start,tv_end;
  struct tm tm;
  time_t timet;
  strptime(start_time_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  timet = mktime(&tm);
  tv_start = timet;
  strptime(end_time_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  timet = mktime(&tm);
  tv_end = timet;

  int32_t first_index = 0;
  int32_t last_index = files.size() - 1;

  binlog_consumer = new BinlogConsumer(binlog_path, files[first_index], files[last_index], file_offset);

  if (!binlog_consumer->Init()) {
    fprintf(stderr, "Binlog comsumer initialization failure, exit...\n");
    exit(-1);
  } else if (!binlog_consumer->trim()) {
    fprintf(stderr, "Binlog comsumer trim failure, maybe the offset is illegal, exit...\n");
    exit(-1);
  }

  TryToCommunicate();

  progress_thread = new ProgressThread(binlog_consumer);
  progress_thread->StartThread();

  uint64_t success_num = 0;
  uint64_t fail_num = 0;
  std::cout << "Start migrating data from Binlog to Pika/Redis..." << std::endl;
  BinlogItem binlog_item;
  while (true) {
    std::string scratch;
    slash::Status s = binlog_consumer->Parse(&scratch);
    if (s.ok()) {
      if (PikaBinlogTransverter::BinlogDecode(TypeFirst, scratch, &binlog_item)) {
        std::string redis_cmd = binlog_item.content();
        if (tv_start <= binlog_item.exec_time() && binlog_item.exec_time() <= tv_end) {
          Status net_s = cli->Send(&redis_cmd);
          if (net_s.ok()) {
            net_s = cli->Recv(NULL);
            if (net_s.ok()) {
              success_num++;
            } else {
              std::cout << "No." << fail_num << "send data failed, " << net_s.ToString().c_str() << std::endl;
              std::cout << "data: " << scratch << std::endl;
              fail_num++;
            }
          }
        }
      } else {
        std::cout << "Binlog Decode error, exit..." << std::endl;
        exit(-1);
      }
    } else if (s.IsComplete()) {
      break;
    } else {
      fprintf (stderr, "Binlog Parse err: %s, exit...\n", s.ToString().c_str());
      exit(-1);
    }
  }
  progress_thread->StopThread();
  progress_thread->JoinThread();
  std::cout << "Send binlog finished, success num: " << success_num << ", fail num: " << fail_num << std::endl;

  delete progress_thread;
  delete binlog_consumer;
  delete cli;

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : "
            << hours << " hours "
            << minutes % 60 << " minutes "
            << seconds % 60 << " seconds "
            << std::endl;

  return 0;
}

