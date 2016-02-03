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


int main(int argc, char *argv[])
{

  if (argc < 2) {
    printf ("Usage: ./pika port\n");
    exit(-1);
  }

  PikaGlogInit();
  PikaSignalSetup();

  DLOG(INFO) << "Server at: " << argv[1];
  g_pika_server = new PikaServer(atoi(argv[1]));
  g_pika_server->Start();

  return 0;
}
