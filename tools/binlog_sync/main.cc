#include <glog/logging.h>
#include "binlog_sync.h"


BinlogSync* g_binlog_sync;

static void GlogInit(std::string& log_path) {
  if (!slash::FileExists(log_path)) {
    slash::CreatePath(log_path); 
  }

//  if (!g_pika_conf->daemonize()) {
    FLAGS_alsologtostderr = true;
//  }
  FLAGS_log_dir = log_path;
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  ::google::InitGoogleLogging("BinlogSync");
}

//static void daemonize() {
//  if (fork() != 0) exit(0); /* parent exits */
//  setsid(); /* create a new session */
//}
//
//static void close_std() {
//  int fd;
//  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
//    dup2(fd, STDIN_FILENO);
//    dup2(fd, STDOUT_FILENO);
//    dup2(fd, STDERR_FILENO);
//    close(fd);
//  }
//}
//
//static void create_pid_file(void) {
//  /* Try to write the pid file in a best-effort way. */
//  std::string path(g_pika_conf->pidfile());
//
//  size_t pos = path.find_last_of('/');
//  if (pos != std::string::npos) {
//    // mkpath(path.substr(0, pos).c_str(), 0755);
//    slash::CreateDir(path.substr(0, pos));
//  } else {
//    path = kPikaPidFile;
//  }
//
//  FILE *fp = fopen(path.c_str(), "w");
//  if (fp) {
//    fprintf(fp,"%d\n",(int)getpid());
//    fclose(fp);
//  }
//}

static void IntSigHandle(const int sig) {
  DLOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_binlog_sync->UnLock();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
}

static void Usage()
{
    fprintf(stderr,
            "Usage: binlog_sync [-h] [-p local_port -i master_ip -o master_port -f filenum -s offset -w password -l log_path]\n"
            "\t-h               -- show this help\n"
            "\t-p     -- local port(REQUIRED) \n"
            "\t-i     -- master ip(OPTIONAL default: 127.0.0.1) \n"
            "\t-o     -- master port(REQUIRED) \n"
            "\t-f     -- binlog filenum(OPTIONAL default: local offset) \n"
            "\t-s     -- binlog offset(OPTIONAL default: local offset) \n"
            "\t-w     -- password for master(OPTIONAL) \n"
            "\t-l     -- local log path(OPTIONAL default: ./log) \n"
            "  example: ./binlog_sync -p 9222 -i 127.0.0.1 -o 9221 -f 0 -s 0 -w abc -l ./log\n"
           );
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  bool path_opt = false;
  char c;
  char buf[1024];
  int64_t src_port = -1;
  std::string dest_host = "127.0.0.1";
  int64_t dest_port = -1;
  int64_t filenum = -1;
  int64_t offset = -1;
  std::string passwd;
  std::string log_path = "./log/";
//  std::cout << src_port << " " << dest_host << " " << dest_port << " " << filenum << " " << offset << " " << passwd << std::endl;
  while (-1 != (c = getopt(argc, argv, "p:i:o:f:s:w:l:h"))) {
    switch (c) {
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &src_port);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        dest_host = std::string(buf);
        break;
      case 'o':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &dest_port);
        break;
      case 'f':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &filenum);
        break;
      case 's':
        snprintf(buf, 1024, "%s", optarg);
        slash::string2l(buf, strlen(buf), &offset);
        break;
      case 'w':
        snprintf(buf, 1024, "%s", optarg);
        passwd = std::string(buf);
        break;
      case 'l':
        snprintf(buf, 1024, "%s", optarg);
        log_path = std::string(buf);
        if (log_path[log_path.length() - 1] != '/' ) {
          log_path.append("/");
        }
        break;
      case 'h':
        Usage();
        return 0;
      default:
        Usage();
        return 0;
    }
  }

  //std::cout << src_port << " " << dest_host << " " << dest_port << " " << filenum << " " << offset << " " << passwd << " " << log_path << std::endl;
  if (src_port == -1 || dest_port == -1) {
    fprintf (stderr, "Invalid Arguments\n" );
    Usage();
    exit(-1);
  }

  // daemonize if needed
//  if (g_pika_conf->daemonize()) {
//    daemonize();
//    create_pid_file();
//  }


  GlogInit(log_path);
  SignalSetup();

  g_binlog_sync = new BinlogSync(filenum, offset, src_port, dest_host, dest_port, passwd, log_path);

//  if (g_pika_conf->daemonize()) {
//    close_std();
//  }

  g_binlog_sync->Start();

  return 0;
}
