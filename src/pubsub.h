/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <memory>
#include <set>
#include <vector>
#include "pstring.h"

namespace pikiwidb {

class PClient;
class PPubsub {
 public:
  static PPubsub& Instance();

  PPubsub(const PPubsub&) = delete;
  void operator=(const PPubsub&) = delete;

  std::size_t Subscribe(PClient* client, const PString& channel);
  std::size_t UnSubscribe(PClient* client, const PString& channel);
  std::size_t UnSubscribeAll(PClient* client);
  std::size_t PublishMsg(const PString& channel, const PString& msg);

  std::size_t PSubscribe(PClient* client, const PString& pchannel);
  std::size_t PUnSubscribeAll(PClient* client);
  std::size_t PUnSubscribe(PClient* client, const PString& pchannel);

  // introspect
  void PubsubChannels(std::vector<PString>& res, const char* pattern = 0) const;
  std::size_t PubsubNumsub(const PString& channel) const;
  std::size_t PubsubNumpat() const;

  void InitPubsubTimer();
  void RecycleClients(PString& startChannel, PString& startPattern);

 private:
  PPubsub() {}

  using Clients = std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > >;
  using ChannelClients = std::map<PString, Clients>;

  ChannelClients channels_;
  ChannelClients patternChannels_;

  PString startChannel_;
  PString startPattern_;
  static void recycleClients(ChannelClients& channels, PString& start);
};

}  // namespace pikiwidb

