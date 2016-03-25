#include <glog/logging.h>
#include "pika_server.h"
#include "pika_command.h"
#include "pika_conf.h"
#include "pika_define.h"
#include "env.h"

PikaConf *g_pika_conf;

PikaServer* g_pika_server;

static void version() {
    printf("-----------Pika server %s ----------\n", kPikaVersion.c_str());
}

static void PikaConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_conf = new PikaConf(path);
  if (g_pika_conf->Load() != 0) {
    LOG(FATAL) << "pika load conf error";
  }
  version();
  printf("-----------Pika config list----------\n");
  g_pika_conf->DumpConf();
  printf("-----------Pika config end----------\n");
}

static void PikaGlogInit() {
  if (!g_pika_conf->daemonize()) {
    FLAGS_alsologtostderr = true;
  }

  FLAGS_log_dir = g_pika_conf->log_path();
  FLAGS_minloglevel = g_pika_conf->log_level();
  FLAGS_max_log_size = 1800;
  ::google::InitGoogleLogging("pika");
}

static void daemonize() {
  int fd;

  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */

  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void create_pid_file(void) {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    slash::CreateDir(path.substr(0, pos));
  } else {
    path = kPikaPidFile;
  }

  FILE *fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp,"%d\n",(int)getpid());
    fclose(fp);
  }
}

static void IntSigHandle(const int sig) {
  DLOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_server->Exit();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
}


int main(int argc, char *argv[]) {

  PikaConfInit(argv[1]);

  // daemonize if needed
  if (g_pika_conf->daemonize()) {
    daemonize();
    create_pid_file();
  }


  PikaGlogInit();
  PikaSignalSetup();
  InitCmdInfoTable();

  DLOG(INFO) << "Server at: " << argv[1];
  g_pika_server = new PikaServer();
  g_pika_server->Start();

  return 0;
}
