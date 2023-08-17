/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "multi.h"
#include "client.h"
#include "log.h"
#include "store.h"

namespace pikiwidb {

PMulti& PMulti::Instance() {
  static PMulti mt;
  return mt;
}

void PMulti::Watch(PClient* client, int dbno, const PString& key) {
  if (client->Watch(dbno, key)) {
    Clients& cls = clients_[dbno][key];
    cls.push_back(std::static_pointer_cast<PClient>(client->shared_from_this()));
  }
}

bool PMulti::Multi(PClient* client) {
  if (client->IsFlagOn(ClientFlag_multi)) {
    return false;
  }

  client->ClearMulti();
  client->SetFlag(ClientFlag_multi);
  return true;
}

bool PMulti::Exec(PClient* client) { return client->Exec(); }

void PMulti::Discard(PClient* client) {
  client->ClearMulti();
  client->ClearWatch();
}

void PMulti::NotifyDirty(int dbno, const PString& key) {
  auto tmpDBIter = clients_.find(dbno);
  if (tmpDBIter == clients_.end()) {
    return;
  }

  auto& dbWatchedKeys = tmpDBIter->second;
  auto it = dbWatchedKeys.find(key);
  if (it == dbWatchedKeys.end()) {
    return;
  }

  Clients& cls = it->second;
  for (auto itCli(cls.begin()); itCli != cls.end();) {
    auto client(itCli->lock());
    if (!client) {
      WARN("Erase not exist client when notify dirty key[{}]", key);
      itCli = cls.erase(itCli);
    } else {
      if (client.get() != PClient::Current() && client->NotifyDirty(dbno, key)) {
        WARN("Erase dirty client {} when notify dirty key[{}]", client->GetName(), key);
        itCli = cls.erase(itCli);
        itCli = cls.erase(itCli);
      } else {
        ++itCli;
      }
    }
  }

  if (cls.empty()) {
    dbWatchedKeys.erase(it);
  }
}

void PMulti::NotifyDirtyAll(int dbno) {
  if (dbno == -1) {
    for (auto& db_set : clients_) {
      for (auto& key_clients : db_set.second) {
        std::for_each(key_clients.second.begin(), key_clients.second.end(), [&](const std::weak_ptr<PClient>& wcli) {
          auto scli = wcli.lock();
          if (scli) {
            scli->SetFlag(ClientFlag_dirty);
          }
        });
      }
    }
  } else {
    auto it = clients_.find(dbno);
    if (it != clients_.end()) {
      for (auto& key_clients : it->second) {
        std::for_each(key_clients.second.begin(), key_clients.second.end(), [&](const std::weak_ptr<PClient>& wcli) {
          auto scli = wcli.lock();
          if (scli) {
            scli->SetFlag(ClientFlag_dirty);
          }
        });
      }
    }
  }
}

// multi commands
PError watch(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  if (client->IsFlagOn(ClientFlag_multi)) {
    ReplyError(PError_watch, reply);
    return PError_watch;
  }

  std::for_each(++params.begin(), params.end(),
                [client](const PString& s) { PMulti::Instance().Watch(client, PSTORE.GetDB(), s); });

  FormatOK(reply);
  return PError_ok;
}

PError unwatch(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  client->ClearWatch();
  FormatOK(reply);
  return PError_ok;
}

PError multi(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  if (PMulti::Instance().Multi(client)) {
    FormatOK(reply);
  } else {
    reply->PushData("-ERR MULTI calls can not be nested\r\n", sizeof "-ERR MULTI calls can not be nested\r\n" - 1);
  }

  return PError_ok;
}

PError exec(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  if (!client->IsFlagOn(ClientFlag_multi)) {
    ReplyError(PError_noMulti, reply);
    return PError_noMulti;
  }
  if (!PMulti::Instance().Exec(client)) {
    ReplyError(PError_dirtyExec, reply);
    return PError_dirtyExec;
  }
  return PError_ok;
}

PError discard(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  if (!client->IsFlagOn(ClientFlag_multi)) {
    reply->PushData("-ERR DISCARD without MULTI\r\n", sizeof "-ERR DISCARD without MULTI\r\n" - 1);
  } else {
    PMulti::Instance().Discard(client);
    FormatOK(reply);
  }

  return PError_ok;
}

}  // namespace pikiwidb
