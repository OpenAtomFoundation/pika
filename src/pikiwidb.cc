/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

//
//  PikiwiDB.cc

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <spawn.h>
#include <iostream>
#include <thread>

#include "log.h"

#include "client.h"
#include "command.h"
#include "store.h"

#include "aof.h"
#include "config.h"
#include "db.h"
#include "pubsub.h"
#include "slow_log.h"

#include "pikiwidb.h"
#include "pikiwidb_logo.h"

std::unique_ptr<PikiwiDB> g_pikiwidb;

static void SignalHandler(int num) {
  if (g_pikiwidb) {
    g_pikiwidb->Stop();
  }
}

static void InitSignal() {
  struct sigaction sig;
  ::memset(&sig, 0, sizeof(sig));

  sig.sa_handler = SignalHandler;
  sigaction(SIGINT, &sig, NULL);

  // ignore sigpipe
  sig.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &sig, NULL);
}

const unsigned PikiwiDB::kRunidSize = 40;

PikiwiDB::PikiwiDB() : port_(0), masterPort_(0) {}

PikiwiDB::~PikiwiDB() {}

static void Usage() {
  std::cerr << "Usage:  ./pikiwidb-server [/path/to/redis.conf] [options]\n\
        ./pikiwidb-server -v or --version\n\
        ./pikiwidb-server -h or --help\n\
Examples:\n\
        ./pikiwidb-server (run the server with default conf)\n\
        ./pikiwidb-server /etc/redis/6379.conf\n\
        ./pikiwidb-server --port 7777\n\
        ./pikiwidb-server --port 7777 --slaveof 127.0.0.1 8888\n\
        ./pikiwidb-server /etc/myredis.conf --loglevel verbose\n";
}

bool PikiwiDB::ParseArgs(int ac, char* av[]) {
  for (int i = 0; i < ac; i++) {
    if (cfgFile_.empty() && ::access(av[i], R_OK) == 0) {
      cfgFile_ = av[i];
      continue;
    } else if (strncasecmp(av[i], "-v", 2) == 0 || strncasecmp(av[i], "--version", 9) == 0) {
      std::cerr << "PikiwiDB Server v=" << PIKIWIDB_VERSION << " bits=" << (sizeof(void*) == 8 ? 64 : 32) << std::endl;

      exit(0);
      return true;
    } else if (strncasecmp(av[i], "-h", 2) == 0 || strncasecmp(av[i], "--help", 6) == 0) {
      Usage();
      exit(0);
      return true;
    } else if (strncasecmp(av[i], "--port", 6) == 0) {
      if (++i == ac) {
        return false;
      }
      port_ = static_cast<unsigned short>(std::atoi(av[i]));
    } else if (strncasecmp(av[i], "--loglevel", 10) == 0) {
      if (++i == ac) {
        return false;
      }
      logLevel_ = std::string(av[i]);
    } else if (strncasecmp(av[i], "--slaveof", 9) == 0) {
      if (i + 2 >= ac) {
        return false;
      }

      master_ = std::string(av[++i]);
      masterPort_ = static_cast<unsigned short>(std::atoi(av[++i]));
    } else {
      std::cerr << "Unknow option " << av[i] << std::endl;
      return false;
    }
  }

  return true;
}

static void PdbCron() {
  using namespace pikiwidb;

  if (g_qdbPid != -1) {
    return;
  }

  if (Now() > (g_lastPDBSave + unsigned(g_config.saveseconds)) * 1000UL && PStore::dirty_ >= g_config.savechanges) {
    int ret = fork();
    if (ret == 0) {
      {
        PDBSaver qdb;
        qdb.Save(g_config.rdbfullname.c_str());
        std::cerr << "ServerCron child save rdb done, exiting child\n";
      }  //  make qdb to be destructed before exit
      _exit(0);
    } else if (ret == -1) {
      ERROR("fork qdb save process failed");
    } else {
      g_qdbPid = ret;
    }

    INFO("ServerCron save rdb file {}", g_config.rdbfullname);
  }
}

static void LoadDBFromDisk() {
  using namespace pikiwidb;

  //  USE AOF RECOVERY FIRST, IF FAIL, THEN RDB
  PAOFLoader aofLoader;
  if (aofLoader.Load(g_config.appendfilename.c_str())) {
    const auto& cmds = aofLoader.GetCmds();
    for (const auto& cmd : cmds) {
      const PCommandInfo* info = PCommandTable::GetCommandInfo(cmd[0]);
      PCommandTable::ExecuteCmd(cmd, info);
    }
  } else {
    PDBLoader loader;
    loader.Load(g_config.rdbfullname.c_str());
  }
}

static void CheckChild() {
  using namespace pikiwidb;

  if (g_qdbPid == -1 && g_rewritePid == -1) {
    return;
  }

  int statloc = 0;
  pid_t pid = wait3(&statloc, WNOHANG, nullptr);

  if (pid != 0 && pid != -1) {
    int exit = WEXITSTATUS(statloc);
    int signal = 0;

    if (WIFSIGNALED(statloc)) {
      signal = WTERMSIG(statloc);
    }

    if (pid == g_qdbPid) {
      PDBSaver::SaveDoneHandler(exit, signal);
      if (PREPL.IsBgsaving()) {
        PREPL.OnRdbSaveDone();
      } else {
        PREPL.TryBgsave();
      }
    } else if (pid == g_rewritePid) {
      INFO("pid {} rewrite process success done.", pid);
      PAOFThreadController::RewriteDoneHandler(exit, signal);
    } else {
      ERROR("{} is not rdb or aof process", pid);
      assert(!!!"Is there any back process except rdb and aof?");
    }
  }
}

void PikiwiDB::OnNewConnection(pikiwidb::TcpObject* obj) {
  INFO("New connection from {}", obj->GetPeerIp());

  auto client = std::make_shared<pikiwidb::PClient>(obj);
  obj->SetContext(client);

  client->OnConnect();

  auto msg_cb = std::bind(&pikiwidb::PClient::HandlePackets, client.get(), std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3);
  obj->SetMessageCallback(msg_cb);
  obj->SetOnDisconnect([](pikiwidb::TcpObject* obj) { INFO("disconnect from {}", obj->GetPeerIp()); });
  obj->SetNodelay(true);
}

bool PikiwiDB::Init() {
  using namespace pikiwidb;

  char runid[kRunidSize + 1] = "";
  getRandomHexChars(runid, kRunidSize);
  g_config.runid.assign(runid, kRunidSize);

  if (port_ != 0) {
    g_config.port = port_;
  }

  if (!logLevel_.empty()) {
    g_config.loglevel = logLevel_;
  }

  if (!master_.empty()) {
    g_config.masterIp = master_;
    g_config.masterPort = masterPort_;
  }

  if (!event_loop_.Listen(g_config.ip.c_str(), g_config.port,
                          std::bind(&PikiwiDB::OnNewConnection, this, std::placeholders::_1))) {
    ERROR("can not bind socket on port {}", g_config.port);
    return false;
  }

  PCommandTable::Init();
  PCommandTable::AliasCommand(g_config.aliases);
  PSTORE.Init(g_config.databases);
  PSTORE.InitExpireTimer();
  PSTORE.InitBlockedTimer();
  PSTORE.InitEvictionTimer();
  PSTORE.InitDumpBackends();
  PPubsub::Instance().InitPubsubTimer();

  // Only if there is no backend, load aof or rdb
  if (g_config.backend == pikiwidb::BackEndNone) {
    LoadDBFromDisk();
  }

  PAOFThreadController::Instance().Start();

  PSlowLog::Instance().SetThreshold(g_config.slowlogtime);
  PSlowLog::Instance().SetLogLimit(static_cast<std::size_t>(g_config.slowlogmaxlen));

  event_loop_.ScheduleRepeatedly(1000 / pikiwidb::g_config.hz, PdbCron);
  event_loop_.ScheduleRepeatedly(1000, &PReplication::Cron, &PREPL);
  event_loop_.ScheduleRepeatedly(1, CheckChild);

  // master ip
  if (!g_config.masterIp.empty()) {
    PREPL.SetMasterAddr(g_config.masterIp.c_str(), g_config.masterPort);
  }

  // output logo to console
  char logo[512] = "";
  snprintf(logo, sizeof logo - 1, pikiwidbLogo, PIKIWIDB_VERSION, static_cast<int>(sizeof(void*)) * 8,
           static_cast<int>(g_config.port));
  std::cout << logo;

  return true;
}

void PikiwiDB::Run() {
  event_loop_.SetName("pikiwi-main");
  event_loop_.Run();
  INFO("server exit running");

  Recycle();
}

void PikiwiDB::Recycle() {
  std::cerr << "PikiwiDB::recycle: server is exiting.. BYE BYE\n";
  pikiwidb::PAOFThreadController::Instance().Stop();
}

void PikiwiDB::Stop() { event_loop_.Stop(); }

static void InitLogs() {
  logger::Init("logs/pikiwidb_server.log");

#if BUILD_DEBUG
  spdlog::set_level(spdlog::level::debug);
#else
  spdlog::set_level(spdlog::level::info);
#endif
}

int main(int ac, char* av[]) {
  g_pikiwidb = std::make_unique<PikiwiDB>();

  InitSignal();
  InitLogs();
  INFO("pikiwidb server start...");

  if (!g_pikiwidb->ParseArgs(ac - 1, av + 1)) {
    Usage();
    return -1;
  }

  if (!g_pikiwidb->GetConfigName().empty()) {
    if (!LoadPikiwiDBConfig(g_pikiwidb->GetConfigName().c_str(), pikiwidb::g_config)) {
      std::cerr << "Load config file [" << g_pikiwidb->GetConfigName() << "] failed!\n";
      return -1;
    }
  }

  if (pikiwidb::g_config.daemonize) {
    pid_t pid;
    ::posix_spawn(&pid, av[0], nullptr, nullptr, av, nullptr);
  }

  if (g_pikiwidb->Init()) {
    g_pikiwidb->Run();
  }

  return 0;
}
