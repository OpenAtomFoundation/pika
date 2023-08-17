/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <memory>
#include <vector>
#include "pstring.h"

namespace pikiwidb {

class PClient;
class PMulti {
 public:
  static PMulti& Instance();

  PMulti(const PMulti&) = delete;
  void operator=(const PMulti&) = delete;

  void Watch(PClient* client, int dbno, const PString& key);
  bool Multi(PClient* client);
  bool Exec(PClient* client);
  void Discard(PClient* client);

  void NotifyDirty(int dbno, const PString& key);
  void NotifyDirtyAll(int dbno);

 private:
  PMulti() {}

  using Clients = std::vector<std::weak_ptr<PClient> >;
  using WatchedClients = std::map<int, std::map<PString, Clients> >;

  WatchedClients clients_;
};

}  // namespace pikiwidb

