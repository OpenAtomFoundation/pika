#include <glog/logging.h>
#include <iostream>
#include "redis_cli.h"
#include <stdio.h>
#include <errno.h>
#include <string>

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
           );

}


int GetFiles(std::string& files_str, std::string& pattern, std::vector<int>& files) {
  std::string::size_type pos;  
  std::string file;  
  files_str += pattern;
  int str_size = files_str.size();  
  for(int i = 0; i < str_size; i++) {
    pos = files_str.find(pattern,i);  
    if (pos != (unsigned int)str_size) {
      file = files_str.substr(i, pos - i);  
      files.push_back(atoi(file.c_str()));  
      i = pos + pattern.size() - 1;  
    }  
  }  
  return files.size();  
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


int main(int argc, char *argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  std::string passwd;
  std::string input_path = "./old_log/";
  std::string ip = "127.0.0.1";
  int port = 6279;
  std::string log_type = "old";
  std::string files_str = "0";
  std::string start_time_str = "2001-00-00 00:59:01";
  std::string end_time_str = "2100-01-30 24:00:01";

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
  std::string comma = std::string(",");
  int file_num = GetFiles(files_str, comma, files);
  int start_file = files[0];
  bool isSequential = CheckSequential(files);
  if (!isSequential) {
      std::cout << "please input sequential binlog num :" << std::endl;
      exit(-1);
  }


  Binlog* old_logger = new Binlog(input_path, INPUT_FILESIZE);
  BinlogConsumer* binlog_consumer;
  if (log_type == "old") {
    binlog_consumer = new OldBinlogConsumer(old_logger);
  } else if (log_type == "new") {
    binlog_consumer = new NewBinlogConsumer(old_logger);
  }
  
  Status s;
  s = binlog_consumer->LoadFile(start_file);
  if(!s.ok()) {
      std::cout << "something wrong while loading binlog:" << s.ToString() << std::endl;
      exit(-1);
  }
  pink::RedisCli *rcli = new pink::RedisCli();
  rcli->set_connect_timeout(3000);
  pink::Status pink_s = rcli->Connect(ip, port);
  if (!pink_s.ok()) {
      printf ("Connect failed, %s\n", pink_s.ToString().c_str());
      exit(-1);
  }

  if (use_passwd) {
    std::string auth_str;
    auth_str = "*2\r\n$4\r\nauth\r\n$";
    auth_str.append(std::to_string(passwd.size()));
    auth_str.append("\r\n");
    auth_str.append(passwd);
    auth_str.append("\r\n");
    pink_s = rcli->Send(&auth_str);
    pink_s = rcli->Recv(NULL);
  }


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
  while (true){
    s = binlog_consumer->Parse(scratch, &produce_time);
    if (s.IsEndFile()) {
      std::cout << "parse binlog file:"<< NewFileName(old_logger->filename, start_file++) << " finished" << std::endl;
      finished_num ++;
      if (finished_num < file_num) {
        s = binlog_consumer->LoadNextFile();
      } else {
        break;
      }
    } else if (s.IsComplete()) {
      std::cout << "all binlog parsed" << std::endl;
      break;
    } else if (s.ok()) {
      if (log_type == "new" && (produce_time < tv_start || produce_time > tv_end)) {
        continue;
      }
      pink_s = rcli->Send(&scratch);
      pink_s = rcli->Recv(NULL);
    } else if (!s.ok()) {
      std::cout << "something wrong when parsing binlog " << std::endl;
      break;
    }
  }


  
  delete binlog_consumer;
  delete old_logger;
  return 0;

  
  }
