// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <iostream>
#include "redis_cli.h"
#include <stdio.h>
#include <errno.h>
#include <string>
#include <signal.h>
#include <tuple>

#include "binlog.h"
#include "binlog_consumer.h"
#include "binlog_producer.h"

#define INPUT_FILESIZE 104857600


static void Usage()
{
    fprintf(stderr,
            "Usage: binlogsender [-h] [-t old/new -a password -i ip -p port -n input -f filenumber -s starttime -e endtime ]\n"
            "\tBinlog_sender reads from pika's binlog and send to pika/redis server\n"
            "\tYou can specify a unixtime because pika's new binlog(later than 2.1.0) is timestamped.\n"
            "\tYou can build a new pika back to any timepoint with this tool, let's rock and roll!\n"
            "\t-h     -- show this help\n"
            "\t-a     -- password for pika server\n"
            "\t-t     -- log type:old or new\n"
            "\t-i     -- ip of pika server\n"
            "\t-p     -- port of pika server\n"
            "\t-n     -- path of input binlog files , default: ./old_log/\n"
            "\t-f     -- files to send, default: 0\n"
            "\t-s     -- start time , default: '2001-00-00 00:59:01' \n"
            "\t-e     -- end time , default: '2100-01-30 24:00:01' \n"
            "  example: ./binlog_sender -n /data2/wangwenduo/newlog/ -t new -i 127.0.0.1 -p 10221 -s '2001-10-11 11:11:11' -e '2020-12-11 11:11:11' -f 526,527  \n"
            "  example2: ./binlog_sender -n /data2/wangwenduo/newlog/ -t new -i 127.0.0.1 -p 10221 -s '2001-10-11 11:11:11' -e '2020-12-11 11:11:11' -f 514-530  \n"
           );

}



/*
 * io functions
 */
bool Exists(std::string& base, std::string pattern) {
  std::size_t found = base.find(pattern);
  if (found == std::string::npos) {
    return false;
  } else {
    return true;
  }
}

std::tuple<int,std::vector<int>> GetFileList(std::string files_str) {
  std::vector<int> file_vec;
  int str_size = files_str.size();
  std::string::size_type pos;
  if (Exists(files_str, std::string(1,','))) {
    files_str += ',';
    for (int i = 0; i < str_size; i++) {
      pos = files_str.find(',',i);
      if (pos != (unsigned int)str_size) {
        std::string file = files_str.substr(i, pos - i);
        file_vec.push_back(atoi(file.c_str()));
        i = pos;
      }
    }
  } else if (Exists(files_str, std::string(1,'-'))) {
      pos = files_str.find('-',0);
      int start_file = atoi(files_str.substr(0, pos).c_str());
      int end_file = atoi(files_str.substr(pos+1).c_str());
      for (int i = start_file; i <= end_file; i++) {
        file_vec.push_back(i);
      }
  } else {
    fprintf (stderr, "wrong input file string:%s \n", files_str.c_str());
    exit(-1);
  }
  int file_num = file_vec.size();
  return std::make_tuple(file_num, file_vec);
}

bool CheckSequential(std::vector<int>& seq) {
  bool isSeq = true;
  if (seq.size() <= 1)
    return isSeq;
  for (unsigned int i = 0; i <= seq.size() - 2; i++) {
    if (seq[i+1] != seq[i] + 1) {
      isSeq = false;
      break;
    }
  }
  return isSeq;
}



/*
 * Signal Handle functions
 */
void LastWord(int sig) {
  fprintf(stderr,"receive signal %s", strsignal(sig));
  exit(-1);
}

void SignalAssign() {
  signal(SIGPIPE,LastWord);
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  SignalAssign();

  /*
   * parse args
   */
  std::string passwd;
  std::string input_path = "./old_log/";
  std::string ip = "127.0.0.1";
  int port = 6279;
  std::string log_type = "old";
  std::string files_str = "0";
  std::string start_time_str = "2001-01-01 00:01:01";
  std::string end_time_str = "2100-01-01 00:00:01";

  // for correct inputs , we use these flags to generate warning to user
  bool use_passwd = false;
  bool default_input_path = true;
  bool default_ip = true;
  bool default_port = true;
  bool default_log_type = true;
  bool default_files = true;
  bool default_start_time = true;
  bool default_end_time = true;
  char c;
  while (-1 != (c = getopt(argc, argv, "hn:i:p:t:f:s:e:a:"))) {
    switch (c) {
      case 'h':
        Usage();
        exit(-1);
      case 'a':
        passwd = optarg;
        use_passwd = true;
        break;
      case 'n':
        input_path = optarg;
        default_input_path = false;
        break;
      case 'i':
        ip = optarg;
        default_ip = false;
        break;
      case 'p':
        port = atoi(optarg);
        default_port = false;
        break;
      case 't':
        log_type = optarg;
        default_log_type = false;
        break;
      case 'f':
        files_str = optarg;
        default_files = false;
        break;
      case 's':
        start_time_str = optarg;
        default_start_time = false;
        break;
      case 'e':
        end_time_str = optarg;
        default_end_time = false;
        break;
      default:
        Usage();
        exit(-1);
        return 0;
    }
  }

  if (default_input_path) {
    fprintf (stderr, "Warning: use default input file path\n" );
  }
  if (default_ip) {
    fprintf (stderr, "Warning: use default ip:%s\n", ip.c_str());
  }
  if (default_port) {
    fprintf (stderr, "Warning: use default port:%d\n", port);
  }
  if (default_files) {
    fprintf (stderr, "Warning: use default input file number:%s\n", files_str.c_str() );
  }
  if (default_log_type) {
    fprintf (stderr, "Warning: use default log type:%s \n", log_type.c_str());
  }
  if (default_start_time) {
    fprintf (stderr, "Warning: use default start time:%s \n", start_time_str.c_str());
  }
  if (default_end_time) {
    fprintf (stderr, "Warning: use default end time:%s \n", end_time_str.c_str());
  }
  if (log_type != "old" && log_type != "new") {
    fprintf (stderr, "undefined log case: old or new only\n" );
    exit(-1);
  }

  std::vector<int> files;
  int file_num;
  std::tie(file_num,files) = GetFileList(files_str);
  int start_file = files[0];
  bool isSequential = CheckSequential(files);
  if (!isSequential) {
      fprintf (stderr, "please input sequential binlog num \n"  );
      exit(-1);
  }


  /*
   * open binlog
   */
  Binlog* old_logger = new Binlog(input_path, INPUT_FILESIZE);
  BinlogConsumer* binlog_consumer;
  if (log_type == "old") {
    binlog_consumer = new OldBinlogConsumer(old_logger);
  } else if (log_type == "new") {
    binlog_consumer = new NewBinlogConsumer(old_logger);
  } else {
    binlog_consumer = nullptr;
  }
  
  Status s;
  s = binlog_consumer->LoadFile(start_file);
  if(!s.ok()) {
      fprintf (stderr, "error while loading binlog file write2file%d : %s \n", start_file, s.ToString().c_str());
      exit(-1);
  }

  /*
   * Connect
   */
  pink::RedisCli *rcli = new pink::RedisCli();
  rcli->set_connect_timeout(3000);
  fprintf (stderr, "Connecting...\n");
  pink::Status pink_s = rcli->Connect(ip, port);
  if (!pink_s.ok()) {
      fprintf (stderr, "Connect failed, %s\n", pink_s.ToString().c_str());
      exit(-1);
  }
  fprintf (stderr, "Connected...\n");
  if (use_passwd) {
    fprintf (stderr, "Sending Auth...\n");
    std::string auth_str;
    auth_str = "*2\r\n$4\r\nauth\r\n$";
    auth_str.append(std::to_string(passwd.size()));
    auth_str.append("\r\n");
    auth_str.append(passwd);
    auth_str.append("\r\n");
    pink_s = rcli->Send(&auth_str);
    pink_s = rcli->Recv(NULL);
  if (!pink_s.ok()) {
      fprintf (stderr, "Auth failed, %s\n", pink_s.ToString().c_str());
      exit(-1);
  }
  }


  /*
   * parse binlog and send to pika server
   */
  fprintf (stderr, "Sending query data...\n");
  std::string scratch;
  scratch.reserve(1024 * 1024);
  int finished_num = 0;
  uint64_t tv_start,tv_end;
  struct tm tm;
  time_t timet;
  strptime(start_time_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  timet = mktime(&tm);
  tv_start = timet;
  strptime(end_time_str.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  timet = mktime(&tm);
  tv_end = timet;
  uint64_t produce_time = 0;
  int fail_num = 0;
  int success_num = 0;

  FILE * error_fp;
  error_fp = fopen("binlog_sender.error.log", "w");
  setvbuf(error_fp, NULL, _IONBF, 0);
  if (!error_fp) {
      fprintf (stderr, "error opening binlog_sender.error.log\n");
      exit(-1);
  }
  fprintf(error_fp, "This log contains data that failed to be sent");

  while (true){
    s = binlog_consumer->Parse(scratch, &produce_time);
    if (s.IsEndFile()) {
      fprintf (stderr, "parse binlog file: %s finished\n", NewFileName(old_logger->filename, start_file++).c_str());
      finished_num ++;
      if (finished_num < file_num) {
        s = binlog_consumer->LoadNextFile();
        if (!s.ok()) {
          fprintf (stderr, "error loading binlog file\n");
          exit(-1);
        }
      } else {
        break;
      }
    } else if (s.IsComplete()) {
      fprintf (stderr,"all binlog parsed \n");
      break;
    } else if (s.ok()) {
      if (log_type == "new" && (produce_time < tv_start || produce_time > tv_end) && !(default_start_time && default_end_time)) {
        continue;
      }
      pink_s = rcli->Send(&scratch);
      if (pink_s.ok()) {
        pink_s = rcli->Recv(NULL);
      }
      if (!pink_s.ok()) {
        fail_num ++;
        fprintf(error_fp, "No.%d send data failed, status:%s \n", fail_num, pink_s.ToString().c_str());
        fprintf(error_fp, "Data:\n%s\n", scratch.c_str());
      } else if (++success_num % 10000 == 0) {
        fprintf(stderr, "%d query data has been sent successfully\n", success_num);
      }
    } else if (!s.ok()) {
      fprintf(stderr, "something wrong when parsing binlog \n");
      break;
    }
  }

  fclose (error_fp);
  delete rcli;
  delete binlog_consumer;
  delete old_logger;
  return 0;

  
  }
