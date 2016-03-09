#include <glog/logging.h>
#include "pika_server.h"
#include "pika_command.h"
#include "pika_conf.h"
#include "pika_define.h"

PikaConf *g_pika_conf;

PikaServer* g_pika_server;

static void version()
{
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
  FLAGS_log_dir = "./log";
  FLAGS_minloglevel = 0;
  FLAGS_alsologtostderr = true;
  FLAGS_max_log_size = 1800;
  ::google::InitGoogleLogging("pika");
}

static void IntSigHandle(const int sig) {
  ::google::ShutdownGoogleLogging();
  g_pika_server->mutex_.Unlock();
  DestoryCmdInfoTable();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
}


int main(int argc, char *argv[])
{

  PikaConfInit(argv[1]);
  PikaGlogInit();
  PikaSignalSetup();
  InitCmdInfoTable();

  DLOG(INFO) << "Server at: " << argv[1];
  g_pika_server = new PikaServer();
  g_pika_server->Start();

  return 0;
}
