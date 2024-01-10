// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <sys/resource.h>
#include <csignal>
#include <memory.h>

#include "include/build_version.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "include/pika_version.h"
#include "net/include/net_stats.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_defer.h"

std::unique_ptr<PikaConf> g_pika_conf;
// todo : change to unique_ptr will coredump
PikaServer* g_pika_server = nullptr;
std::unique_ptr<PikaReplicaManager> g_pika_rm;

std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

extern std::unique_ptr<net::NetworkStatistic> g_network_statistic;

static void version() {
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR, PIKA_MINOR, PIKA_PATCH);
  std::cout << "-----------Pika server----------" << std::endl;
  std::cout << "pika_version: " << version << std::endl;
  std::cout << pika_build_git_sha << std::endl;
  std::cout << "pika_build_compile_date: " << pika_build_compile_date << std::endl;
  // fake version for client SDK
  std::cout << "redis_version: " << version << std::endl;
}

static void PrintPikaLogo() {
  printf("   .............          ....     .....       .....           .....         \n"
         "   #################      ####     #####      #####           #######        \n"
         "   ####         #####     ####     #####    #####            #########       \n"
         "   ####          #####    ####     #####  #####             ####  #####      \n"
         "   ####         #####     ####     ##### #####             ####    #####     \n"
         "   ################       ####     ##### #####            ####      #####    \n"
         "   ####                   ####     #####   #####         #################   \n"
         "   ####                   ####     #####    ######      #####         #####  \n"
         "   ####                   ####     #####      ######   #####           ##### \n");
}

static void PikaConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_conf = std::make_unique<PikaConf>(path);
  if (g_pika_conf->Load() != 0) {
    LOG(FATAL) << "pika load conf error";
  }
  version();
  printf("-----------Pika config list----------\n");
  g_pika_conf->DumpConf();
  PrintPikaLogo();
  printf("-----------Pika config end----------\n");
}

static void PikaGlogInit() {
  if (!pstd::FileExists(g_pika_conf->log_path())) {
    pstd::CreatePath(g_pika_conf->log_path());
  }

  if (!g_pika_conf->daemonize()) {
    FLAGS_alsologtostderr = true;
  }
  FLAGS_log_dir = g_pika_conf->log_path();
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("pika");
}

static void daemonize() {
  if (fork()) {
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

static void create_pid_file() {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    pstd::CreateDir(path.substr(0, pos));
  } else {
    path = kPikaPidFile;
  }

  FILE* fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp, "%d\n", static_cast<int>(getpid()));
    fclose(fp);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_server->Exit();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void usage() {
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR, PIKA_MINOR, PIKA_PATCH);
  fprintf(stderr,
          "Pika module %s\n"
          "usage: pika [-hv] [-c conf/file]\n"
          "\t-h               -- show this help\n"
          "\t-c conf/file     -- config file \n"
          "\t-v               -- show version\n"
          "  example: ./output/bin/pika -c ./conf/pika.conf\n",
          version);
}

int main(int argc, char* argv[]) {
  if (argc != 2 && argc != 3) {
    usage();
    exit(-1);
  }

  bool path_opt = false;
  signed char c;
  char path[1024];
  while (-1 != (c = static_cast<int8_t>(getopt(argc, argv, "c:hv")))) {
    switch (c) {
      case 'c':
        snprintf(path, 1024, "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        usage();
        return 0;
      case 'v':
        version();
        return 0;
      default:
        usage();
        return 0;
    }
  }

  if (!path_opt) {
    fprintf(stderr, "Please specify the conf file path\n");
    usage();
    exit(-1);
  }
  PikaConfInit(path);

  rlimit limit;
  rlim_t maxfiles = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      LOG(WARNING) << "your 'limit -n ' of " << old_limit
                   << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit
                 << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno)
                 << "), do it by yourself";
    }
  }

  // daemonize if needed
  if (g_pika_conf->daemonize()) {
    daemonize();
    create_pid_file();
  }

  PikaGlogInit();
  PikaSignalSetup();
  InitCRC32Table();

  LOG(INFO) << "Server at: " << path;
  g_pika_cmd_table_manager = std::make_unique<PikaCmdTableManager>();
  g_pika_cmd_table_manager->InitCmdTable();
  g_pika_server = new PikaServer();
  g_pika_rm = std::make_unique<PikaReplicaManager>();
  g_network_statistic = std::make_unique<net::NetworkStatistic>();
  g_pika_server->InitDBStruct();

  auto status = g_pika_server->InitAcl();
  if (!status.ok()) {
    LOG(FATAL) << status.ToString();
  }

  if (g_pika_conf->daemonize()) {
    close_std();
  }

  DEFER {
    delete g_pika_server;
    g_pika_server = nullptr;
    g_pika_rm.reset();
    g_pika_cmd_table_manager.reset();
    g_network_statistic.reset();
    ::google::ShutdownGoogleLogging();
    g_pika_conf.reset();
  };

  g_pika_rm->Start();
  g_pika_server->Start();

  if (g_pika_conf->daemonize()) {
    unlink(g_pika_conf->pidfile().c_str());
  }

  // stop PikaReplicaManager first，avoid internal threads
  // may references to dead PikaServer
  g_pika_rm->Stop();

  return 0;
}
