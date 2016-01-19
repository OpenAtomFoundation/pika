#include <glog/logging.h>
#include "pika_server.h"

PikaServer* g_pika_server;

static void PikaGlogInit() {
  FLAGS_log_dir = "/tmp";
  FLAGS_minloglevel = 0;
  FLAGS_alsologtostderr = true;
  FLAGS_max_log_size = 1800;
  ::google::InitGoogleLogging("pika");
}

static void IntSigHandle(const int sig) {
  ::google::ShutdownGoogleLogging();
  g_pika_server->mutex_.Unlock();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
}


int main()
{
  PikaGlogInit();
  PikaSignalSetup();
  g_pika_server = new PikaServer(9211);
  g_pika_server->Start();

  return 0;
}
