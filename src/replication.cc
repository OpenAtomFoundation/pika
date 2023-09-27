/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "replication.h"

#include <unistd.h>
#include <iostream>  // the child process use stdout for log
#include <sstream>

#include "client.h"
#include "common.h"
#include "config.h"
#include "db.h"
#include "event_loop.h"
#include "log.h"
#include "net/util.h"
#include "pikiwidb.h"

namespace pikiwidb {



PReplication& PReplication::Instance() {
  static PReplication rep;
  return rep;
}

PReplication::PReplication() {}

bool PReplication::IsBgsaving() const { return bgsaving_; }

void PReplication::AddSlave(pikiwidb::PClient* cli) {
  slaves_.push_back(std::static_pointer_cast<PClient>(cli->shared_from_this()));
}

bool PReplication::HasAnyWaitingBgsave() const {
  for (const auto& c : slaves_) {
    auto cli = c.lock();
    if (cli && cli->GetSlaveInfo()->state == PSlaveState_wait_bgsave_start) {
      return true;
    }
  }

  return false;
}

void PReplication::OnRdbSaveDone() {
  bgsaving_ = false;

  InputMemoryFile rdb;

  // send rdb to slaves that wait rdb end, set state
  for (auto& wptr : slaves_) {
    auto cli = wptr.lock();
    if (!cli) {
      continue;
    }

    if (cli->GetSlaveInfo()->state == PSlaveState_wait_bgsave_end) {
      cli->GetSlaveInfo()->state = PSlaveState_online;

      if (!rdb.IsOpen() && !rdb.Open(g_config.rdbfullname.c_str())) {
        ERROR("can not open rdb when replication\n");
        return;  // fatal error;
      }

      std::size_t size = std::size_t(-1);
      const char* data = rdb.Read(size);

      // $file_len + filedata
      char tmp[32];
      int n = snprintf(tmp, sizeof tmp - 1, "$%ld\r\n", (long)size);
      evbuffer_iovec iovecs[] = {
          {tmp, (size_t)(n)},
          {const_cast<char*>(data), size},
          {buffer_.ReadAddr(), buffer_.ReadableSize()}
      };
      cli->GetTcpConnection()->SendPacketSafely(iovecs, sizeof(iovecs) / sizeof(iovecs[0]));

      INFO("Send to slave rdb {}, buffer {}", size, buffer_.ReadableSize());
    }
  }

  buffer_.Clear();
}

void PReplication::TryBgsave() {
  if (IsBgsaving()) {
    return;
  }

  if (!HasAnyWaitingBgsave()) {
    return;
  }

  int ret = fork();
  if (ret == 0) {
    {
      PDBSaver qdb;
      qdb.Save(g_config.rdbfullname.c_str());
      DEBUG("PReplication save rdb done, exiting child");
    }
    _exit(0);
  } else if (ret == -1) {
    ERROR("PReplication save rdb FATAL ERROR");
    onStartBgsave(false);
  } else {
    INFO("PReplication save rdb START");
    g_qdbPid = ret;
    onStartBgsave(true);
  }
}

void PReplication::onStartBgsave(bool succ) {
  buffer_.Clear();
  bgsaving_ = succ;

  for (auto& c : slaves_) {
    auto cli = c.lock();

    if (!cli) {
      continue;
    }

    if (cli->GetSlaveInfo()->state == PSlaveState_wait_bgsave_start) {
      if (succ) {
        INFO("onStartBgsave set cli wait bgsave end {}", cli->GetName());
        cli->GetSlaveInfo()->state = PSlaveState_wait_bgsave_end;
      } else {
        cli->Close();  // release slave
      }
    }
  }
}

void PReplication::SendToSlaves(const std::vector<PString>& params) {
  if (IsBgsaving()) {  // During the execution of RDB, there are cache changes.
    SaveCommand(params, buffer_);
    return;
  }

  UnboundedBuffer ub;

  for (const auto& wptr : slaves_) {
    auto cli = wptr.lock();
    if (!cli || cli->GetSlaveInfo()->state != PSlaveState_online) {
      continue;
    }

    if (ub.IsEmpty()) {
      SaveCommand(params, ub);
    }

    cli->GetTcpConnection()->SendPacketSafely(ub);
  }
}

void PReplication::Cron() {
  static unsigned pingCron = 0;

  if (pingCron++ % 50 == 0) {
    for (auto it = slaves_.begin(); it != slaves_.end();) {
      auto cli = it->lock();
      if (!cli) {
        it = slaves_.erase(it);
      } else {
        ++it;

        if (cli->GetSlaveInfo()->state == PSlaveState_online) {
          cli->GetTcpConnection()->SendPacketSafely("PING\r\n", 6);
        }
      }
    }
  }

  if (masterInfo_.addr.IsValid()) {
    switch (masterInfo_.state) {
      case PReplState_none: {
        if (masterInfo_.addr.GetIP() == g_config.ip && masterInfo_.addr.GetPort() == g_config.port) {
          ERROR("Fix config, master addr is self addr!");
          assert(!!!"wrong config for master addr");
        }

        if (auto master = master_.lock()) {
          WARN("Disconnect from previous master {}", master->PeerIP());
          master->Close();
        }

        INFO("Try connect to master {}", AddrToString(&masterInfo_.addr.GetAddr()));

        auto on_new_conn = [](TcpConnection* obj) {
          if (g_pikiwidb) {
            g_pikiwidb->OnNewConnection(obj);
          }
        };
        auto fail_cb = [&](EventLoop*, const char* peer_ip, int port) {
          WARN("OnCallback: Connect master {}:{} failed", peer_ip, port);

          PREPL.SetMasterState(PReplState_none);
          if (!masterInfo_.downSince) {
            masterInfo_.downSince = ::time(nullptr);
          }
        };

        auto loop = EventLoop::Self();
        loop->Connect(masterInfo_.addr.GetIP().c_str(), masterInfo_.addr.GetPort(), on_new_conn, fail_cb);

        masterInfo_.state = PReplState_connecting;
      } break;

      case PReplState_connected:
        if (!g_config.masterauth.empty()) {
          if (auto master = master_.lock()) {
            UnboundedBuffer req;
            req.PushData("auth ", 5);
            req.PushData(g_config.masterauth.data(), g_config.masterauth.size());
            req.PushData("\r\n", 2);
            master->GetTcpConnection()->SendPacketSafely(req);
            INFO("send auth with password {}", g_config.masterauth);

            masterInfo_.state = PReplState_wait_auth;
            break;
          }
        }
        // fall through to next case.

      case PReplState_wait_auth: {
        auto master = master_.lock();
        if (!master) {
          masterInfo_.state = PReplState_none;
          masterInfo_.downSince = ::time(nullptr);
          WARN("Master is down from wait_auth to none");
        } else if (master->GetAuth()) {
          // send replconf
          char req[128];
          auto len = snprintf(req, sizeof req - 1, "replconf listening-port %hu\r\n", g_config.port);
          master->GetTcpConnection()->SendPacketSafely(req, len);
          masterInfo_.state = PReplState_wait_replconf;

          INFO("Send replconf listening-port {}", g_config.port);
        } else {
          WARN("Haven't auth to master yet, or check masterauth password");
        }
      } break;

      case PReplState_wait_replconf: {
        auto master = master_.lock();
        if (!master) {
          masterInfo_.state = PReplState_none;
          masterInfo_.downSince = ::time(nullptr);
          WARN("Master is down from wait_replconf to none");
        } else {
          // request sync rdb file
          master->GetTcpConnection()->SendPacketSafely("SYNC\r\n", 6);
          INFO("Request SYNC");

          rdb_.Open(slaveRdbFile, false);
          masterInfo_.rdbRecved = 0;
          masterInfo_.rdbSize = std::size_t(-1);
          masterInfo_.state = PReplState_wait_rdb;
        }
      } break;

      case PReplState_wait_rdb:
        break;

      case PReplState_online:
        if (auto master = master_.lock()) {
        } else {
          masterInfo_.state = PReplState_none;
          masterInfo_.downSince = ::time(nullptr);
          WARN("Master is down");
        }
        break;

      default:
        break;
    }
  } else {
    if (masterInfo_.state != PReplState_none) {
      if (auto master = master_.lock(); master) {
        INFO("{} disconnect with Master {}", master->GetName(), master->PeerIP());
        master->Close();
      }

      masterInfo_.state = PReplState_none;
      masterInfo_.downSince = ::time(nullptr);
      WARN("Master is down from connected to none");
    }
  }
}

void PReplication::SaveTmpRdb(const char* data, std::size_t& len) {
  if (masterInfo_.rdbRecved + len > masterInfo_.rdbSize) {
    len = masterInfo_.rdbSize - masterInfo_.rdbRecved;
  }

  rdb_.Write(data, len);
  masterInfo_.rdbRecved += len;

  if (masterInfo_.rdbRecved == masterInfo_.rdbSize) {
    INFO("Rdb recv complete, bytes {}", masterInfo_.rdbSize);

    PSTORE.ResetDB();

    PDBLoader loader;
    loader.Load(slaveRdbFile);
    masterInfo_.state = PReplState_online;
    masterInfo_.downSince = 0;
  }
}

void PReplication::SetMaster(const std::shared_ptr<PClient>& cli) { master_ = cli; }

void PReplication::SetMasterState(PReplState s) { masterInfo_.state = s; }

PReplState PReplication::GetMasterState() const { return masterInfo_.state; }

SocketAddr PReplication::GetMasterAddr() const { return masterInfo_.addr; }

void PReplication::SetMasterAddr(const char* ip, unsigned short port) {
  if (ip) {
    masterInfo_.addr.Init(ip, port);
  } else {
    masterInfo_.addr.Clear();
  }
}

void PReplication::SetRdbSize(std::size_t s) { masterInfo_.rdbSize = s; }

std::size_t PReplication::GetRdbSize() const { return masterInfo_.rdbSize; }

PError replconf(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params.size() % 2 == 0) {
    ReplyError(PError_syntax, reply);
    return PError_syntax;
  }

  for (size_t i = 1; i < params.size(); i += 2) {
    if (strncasecmp(params[i].c_str(), "listening-port", 14) == 0) {
      long port;
      if (!TryStr2Long(params[i + 1].c_str(), params[i + 1].size(), port)) {
        ReplyError(PError_param, reply);
        return PError_param;
      }

      auto info = PClient::Current()->GetSlaveInfo();
      if (!info) {
        PClient::Current()->SetSlaveInfo();
        info = PClient::Current()->GetSlaveInfo();
        PREPL.AddSlave(PClient::Current());
      }
      info->listenPort = static_cast<unsigned short>(port);
    } else {
        break;
    }
  }

  FormatOK(reply);
  return PError_ok;
}

void PReplication::OnInfoCommand(UnboundedBuffer& res) {
  const char* slaveState[] = {
      "none",
      "wait_bgsave",
      "wait_bgsave",
      //"send_bulk", // pikiwidb does not have send bulk state
      "online",

  };

  std::ostringstream oss;
  int index = 0;
  for (const auto& c : slaves_) {
    auto cli = c.lock();
    if (cli) {
      oss << "slave" << index << ":";
      index++;

      oss << cli->PeerIP();

      auto slaveInfo = cli->GetSlaveInfo();
      auto state = slaveInfo ? slaveInfo->state : 0;
      oss << "," << (slaveInfo ? slaveInfo->listenPort : 0) << "," << slaveState[state] << "\r\n";
    }
  }

  std::string slaveInfo(oss.str());

  char buf[1024] = {};
  bool isMaster = !GetMasterAddr().IsValid();
  int n = snprintf(buf, sizeof buf - 1,
                   "# Replication\r\n"
                   "role:%s\r\n"
                   "connected_slaves:%d\r\n%s",
                   isMaster ? "master" : "slave", index, slaveInfo.c_str());

  std::ostringstream masterInfo;
  if (!isMaster) {
    masterInfo << "master_host:" << masterInfo_.addr.GetIP() << "\r\nmaster_port:" << masterInfo_.addr.GetPort()
               << "\r\nmaster_link_status:";

    auto master = master_.lock();
    masterInfo << (master ? "up\r\n" : "down\r\n");
    if (!master) {
      if (!masterInfo_.downSince) {
        assert(0);
      }

      masterInfo << "master_link_down_since_seconds:" << (::time(nullptr) - masterInfo_.downSince) << "\r\n";
    }
  }

  if (!res.IsEmpty()) {
    res.PushData("\r\n", 2);
  }

  res.PushData(buf, n);

  {
    std::string info(masterInfo.str());
    res.PushData(info.c_str(), info.size());
  }
}

PError slaveof(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (strncasecmp(params[1].data(), "no", 2) == 0 && strncasecmp(params[2].data(), "one", 3) == 0) {
    PREPL.SetMasterAddr(nullptr, 0);
  } else {
    long tmpPort = 0;
    Strtol(params[2].c_str(), params[2].size(), &tmpPort);
    unsigned short port = static_cast<unsigned short>(tmpPort);

    SocketAddr reqMaster(params[1].c_str(), port);

    if (port > 0 && PREPL.GetMasterAddr() != reqMaster) {
      PREPL.SetMasterAddr(params[1].c_str(), port);
      PREPL.SetMasterState(PReplState_none);
    }
  }

  FormatOK(reply);
  return PError_ok;
}

PError sync(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* cli = PClient::Current();
  auto slave = cli->GetSlaveInfo();
  if (!slave) {
    cli->SetSlaveInfo();
    slave = cli->GetSlaveInfo();
    PREPL.AddSlave(cli);
  }

  if (slave->state == PSlaveState_wait_bgsave_end || slave->state == PSlaveState_online) {
    WARN("{} state is {}, ignore this sync request", cli->GetName(), slave->state);

    return PError_ok;
  }

  slave->state = PSlaveState_wait_bgsave_start;
  PREPL.TryBgsave();

  return PError_ok;
}

}  // namespace pikiwidb
