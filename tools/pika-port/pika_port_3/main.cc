// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// #include <glog/logging.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <csignal>
#include <random>

#include "conf.h"
#include "pika_port.h"

Conf g_conf;
PikaPort* g_pika_port;
int pidFileFd = 0;

static int lockFile(int fd) {
  struct flock lock;
  lock.l_type = F_WRLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  return fcntl(fd, F_SETLK, &lock);
}

static void createPidFile(const char* file) {
  int fd = open(file, O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
  if (-1 == fd) {
    LOG(FATAL) << "open(" << file << ") = " << fd;
  }

  int ret = lockFile(fd);
  if (ret < 0) {
    close(fd);
    LOG(FATAL) << "lock(" << fd << ") = " << ret;
  }

  // int pid = (int)(getpid());
  pidFileFd = fd;
}

static void daemonize() {
  if (fork() != 0) {
    exit(0); /* parent exits */
  }
  setsid(); /* create a new session */
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  if (2 < pidFileFd) {
    close(pidFileFd);
  }
  g_pika_port->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void GlogInit(const std::string& log_path, bool is_daemon) {
  if (!pstd::FileExists(log_path)) {
    pstd::CreatePath(log_path);
  }

  if (!is_daemon) {
    FLAGS_alsologtostderr = true;
  }

  FLAGS_log_dir = log_path;
  FLAGS_max_log_size = 2048;  // log file 2GB
  ::google::InitGoogleLogging("pika_port_3");
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout
      << "\tPika_port_3 reads data from pika 3.0 and send to redis or pika"
      << std::endl;
  std::cout << "\t-h     -- show this help" << std::endl;
  std::cout << "\t-t     -- local host ip(OPTIONAL default: 127.0.0.1)"
            << std::endl;
  std::cout << "\t-p     -- local port(OPTIONAL)" << std::endl;
  std::cout << "\t-i     -- master ip(OPTIONAL default: 127.0.0.1)"
            << std::endl;
  std::cout << "\t-o     -- master port(REQUIRED)" << std::endl;
  std::cout << "\t-m     -- forward ip(OPTIONAL default: 127.0.0.1)"
            << std::endl;
  std::cout << "\t-n     -- forward port(REQUIRED)" << std::endl;
  std::cout << "\t-x     -- forward thread num(OPTIONAL default: 1)"
            << std::endl;
  std::cout << "\t-y     -- forward password(OPTIONAL)" << std::endl;
  std::cout << "\t-z     -- max timeout duration for waiting pika master "
               "bgsave data (OPTIONAL default 1800s)"
            << std::endl;
  std::cout << "\t-f     -- binlog filenum(OPTIONAL default: local offset)"
            << std::endl;
  std::cout << "\t-s     -- binlog offset(OPTIONAL default: local offset)"
            << std::endl;
  std::cout << "\t-w     -- password for master(OPTIONAL)" << std::endl;
  std::cout
      << "\t-r     -- rsync dump data path(OPTIONAL default: ./rsync_dump)"
      << std::endl;
  std::cout << "\t-l     -- local log path(OPTIONAL default: ./log)"
            << std::endl;
  std::cout << "\t-b     -- max batch number when port rsync dump data "
               "(OPTIONAL default: 512)"
            << std::endl;
  std::cout << "\t-d     -- daemonize(OPTIONAL)" << std::endl;
  std::cout << "\t-e     -- exit(return -1) if dbsync start(OPTIONAL)"
            << std::endl;
  std::cout << "\texample: ./pika_port -t 127.0.0.1 -p 12345 -i 127.0.0.1 -o "
               "9221 -m 127.0.0.1 -n 6379 -x 7 -f 0 -s 0 "
               "-w abc -l ./log -r ./rsync_dump -b 512 -d -e"
            << std::endl;
}

void PrintInfo(const std::time_t& now) {
  std::cout << "================== Pika Port 3 ==================" << std::endl;
  std::cout << "local_ip:" << g_conf.local_ip << std::endl;
  std::cout << "Local_port:" << g_conf.local_port << std::endl;
  std::cout << "Master_ip:" << g_conf.master_ip << std::endl;
  std::cout << "Master_port:" << g_conf.master_port << std::endl;
  std::cout << "Forward_ip:" << g_conf.forward_ip << std::endl;
  std::cout << "Forward_port:" << g_conf.forward_port << std::endl;
  std::cout << "Forward_passwd:" << g_conf.forward_passwd << std::endl;
  std::cout << "Forward_thread_num:" << g_conf.forward_thread_num << std::endl;
  std::cout << "Wait_bgsave_timeout:" << g_conf.wait_bgsave_timeout
            << std::endl;
  std::cout << "Log_path:" << g_conf.log_path << std::endl;
  std::cout << "Dump_path:" << g_conf.dump_path << std::endl;
  std::cout << "Filenum:" << g_conf.filenum << std::endl;
  std::cout << "Offset:" << g_conf.offset << std::endl;
  std::cout << "Passwd:" << g_conf.passwd << std::endl;
  std::cout << "Sync_batch_num:" << g_conf.sync_batch_num << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================"
            << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  char c;
  char buf[1024];
  bool is_daemon = false;
  long num = 0;
  while (-1 != (c = getopt(argc, argv, "t:p:i:o:f:s:w:r:l:m:n:x:y:z:b:edh"))) {
    switch (c) {
      case 't':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.local_ip = std::string(buf);
        break;
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.local_port = static_cast<int>(num);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.master_ip = std::string(buf);
        break;
      case 'o':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.master_port = static_cast<int>(num);
        break;
      case 'm':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_ip = std::string(buf);
        break;
      case 'n':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.forward_port = static_cast<int>(num);
        break;
      case 'x':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.forward_thread_num = static_cast<int>(num);
        break;
      case 'y':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_passwd = std::string(buf);
        break;
      case 'z':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.wait_bgsave_timeout = static_cast<time_t>(num);
        break;

      case 'f':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.filenum = static_cast<size_t>(num);
        break;
      case 's':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.offset = static_cast<size_t>(num);
        break;
      case 'w':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.passwd = std::string(buf);
        break;

      case 'r':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.dump_path = std::string(buf);
        if (g_conf.dump_path[g_conf.dump_path.length() - 1] != '/') {
          g_conf.dump_path.append("/");
        }
        break;
      case 'l':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.log_path = std::string(buf);
        if (g_conf.log_path[g_conf.log_path.length() - 1] != '/') {
          g_conf.log_path.append("/");
        }
        break;
      case 'b':
        snprintf(buf, 1024, "%s", optarg);
        pstd::string2int(buf, strlen(buf), &(num));
        g_conf.sync_batch_num = static_cast<size_t>(num);
        break;
      case 'e':
        g_conf.exit_if_dbsync = true;
        break;
      case 'd':
        is_daemon = true;
        break;
      case 'h':
        Usage();
        return 0;
      default:
        Usage();
        return 0;
    }
  }

  GlogInit(g_conf.log_path, is_daemon);

  if (g_conf.local_port == 0) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> di(10000, 40000);
    // g_conf.local_port = di(mt);
    g_conf.local_port = 21333;
    LOG(INFO) << "Use random port: " << g_conf.local_port;
  }

  std::chrono::system_clock::time_point start_time =
      std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  if (g_conf.master_port == 0 || g_conf.forward_port == 0 ||
      g_conf.sync_batch_num == 0 || g_conf.wait_bgsave_timeout <= 0) {
    fprintf(stderr, "Invalid Arguments\n");
    Usage();
    exit(-1);
  }

  std::string pid_file_name = "/tmp/pika_port_" + std::to_string(getpid());
  createPidFile(pid_file_name.c_str());

  // daemonize if needed
  if (is_daemon) {
    daemonize();
  }

  SignalSetup();

  g_pika_port =
      new PikaPort(g_conf.master_ip, g_conf.master_port, g_conf.passwd);
  if (is_daemon) {
    close_std();
  }

  g_pika_port->Start();

  return 0;
}
