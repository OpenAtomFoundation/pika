// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <random>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "pika_port.h"
#include "conf.h"

Conf g_conf;

PikaPort* g_pika_port;

static void GlogInit(std::string& log_path, bool is_daemon) {
  if (!slash::FileExists(log_path)) {
    slash::CreatePath(log_path); 
  }

  if (!is_daemon) {
    FLAGS_alsologtostderr = true;
  }
  FLAGS_log_dir = log_path;
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("PikaPort");
}

static void daemonize() {
  if (fork() != 0) exit(0); /* parent exits */
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

#include <unistd.h>

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_port->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void Usage()
{
    fprintf(stderr,
            "Usage: pika_port [-h] [-t local_ip -p local_port -i master_ip -o master_port "
			"-m forward_ip -n forward_port -x forward_thread_num -y forward_passwd]\n"
			"-f filenum -s offset -w password -r rsync_dump_path  -l log_path "
            "\t-h               -- show this help\n"
            "\t-t     -- local host ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-p     -- local port(OPTIONAL) \n"
            "\t-i     -- master ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-o     -- master port(REQUIRED) \n"
            "\t-m     -- forward ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-n     -- forward port(REQUIRED) \n"
            "\t-x     -- forward thread num(OPTIONAL default: 1) \n"
            "\t-y     -- forward password(OPTIONAL) \n"
            "\t-f     -- binlog filenum(OPTIONAL default: local offset) \n"
            "\t-s     -- binlog offset(OPTIONAL default: local offset) \n"
            "\t-w     -- password for master(OPTIONAL) \n"
            "\t-r     -- rsync dump data path(OPTIONAL default: ./rsync_dump) \n"
            "\t-l     -- local log path(OPTIONAL default: ./log) \n"
            "\t-d     -- daemonize(OPTIONAL) \n"
            "  example: ./pika_port -t 127.0.0.1 -p 12345 -i 127.0.0.1 -o 9221 "
			"-m 127.0.0.1 -n 6379 -x 7 -f 0 -s 0 -w abc -l ./log -r ./rsync_dump -d\n"
           );
}

int main(int argc, char *argv[])
{
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  char c;
  char buf[1024];
  bool is_daemon = false;
  long num = 0;
  while (-1 != (c = getopt(argc, argv, "t:p:i:o:f:s:w:r:l:m:n:x:y:dh"))) {
    switch (c) {
      case 't':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.local_ip = std::string(buf);
        break;
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.local_port = int(num);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.master_ip = std::string(buf);
        break;
      case 'o':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.master_port = int(num);
        break;
      case 'm':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_ip = std::string(buf);
        break;
      case 'n':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.forward_port = int(num);
        break;
      case 'x':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.forward_thread_num = int(num);
        break;
      case 'y':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.forward_passwd = std::string(buf);
        break;

      case 'f':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.filenum = (size_t)(num);
        break;
      case 's':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &(num));
        g_conf.offset = (size_t)(num);
        break;
      case 'w':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.passwd = std::string(buf);
        break;

      case 'r':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.dump_path = std::string(buf);
        if (g_conf.dump_path[g_conf.dump_path.length() - 1] != '/' ) {
          g_conf.dump_path.append("/");
        }
        break;
      case 'l':
        snprintf(buf, 1024, "%s", optarg);
        g_conf.log_path = std::string(buf);
        if (g_conf.log_path[g_conf.log_path.length() - 1] != '/' ) {
          g_conf.log_path.append("/");
        }
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

  if (g_conf.local_port == 0) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> di(10000, 40000);
    // g_conf.local_port = di(mt);
	g_conf.local_port = 21333;
    LOG(INFO) << "Use random port: " << g_conf.local_port;
  }

  std::cout << "local_ip:" << g_conf.local_ip << " "
            << "local_port:" << g_conf.local_port << " "
            << "master_ip:"  << g_conf.master_ip << " "
            << "master_port:"  << g_conf.master_port << " "
            << "forward_ip:"  << g_conf.forward_ip << " "
            << "forward_port:"  << g_conf.forward_port << " "
            << "forward_passwd:"  << g_conf.forward_passwd << " "
            << "forward_thread_num:" << g_conf.forward_thread_num << " "
            << "log_path:"   << g_conf.log_path << " "
            << "dump_path:"  << g_conf.dump_path << " "
            << "filenum:"    << g_conf.filenum << " "
            << "offset:"     << g_conf.offset << " "
            << "passwd:"     << g_conf.passwd << std::endl;
  if (g_conf.master_port == 0 || g_conf.forward_port == 0) {
    fprintf (stderr, "Invalid Arguments\n" );
    Usage();
    exit(-1);
  }

  // daemonize if needed
  if (is_daemon) {
    daemonize();
  }

  GlogInit(g_conf.log_path, is_daemon);
  SignalSetup();

  g_pika_port = new PikaPort(g_conf.master_ip, g_conf.master_port, g_conf.passwd);
  if (is_daemon) {
    close_std();
  }

  g_pika_port->Start();

  return 0;
}
