/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "pubsub.h"
#include <fnmatch.h>
#include "client.h"
#include "event_loop.h"
#include "log.h"

namespace pikiwidb {
PPubsub& PPubsub::Instance() {
  static PPubsub ps;
  return ps;
}

size_t PPubsub::Subscribe(PClient* client, const PString& channel) {
  if (client && client->Subscribe(channel)) {
    auto it(channels_.find(channel));
    if (it == channels_.end()) {
      it = channels_.insert(ChannelClients::value_type(channel, Clients())).first;
    }

    assert(it != channels_.end());

    bool succ = it->second.insert(std::static_pointer_cast<PClient>(client->shared_from_this())).second;
    assert(succ);
    return 1;
  }

  return 0;
}

std::size_t PPubsub::UnSubscribe(PClient* client, const PString& channel) {
  if (client && client->UnSubscribe(channel)) {
    auto it(channels_.find(channel));
    assert(it != channels_.end());

    Clients& clientSet = it->second;

    std::size_t n = clientSet.erase(std::static_pointer_cast<PClient>(client->shared_from_this()));
    assert(n == 1);

    if (clientSet.empty()) {
      channels_.erase(it);
    }

    return client->ChannelCount();
  }

  return 0;
}

std::size_t PPubsub::UnSubscribeAll(PClient* client) {
  if (!client) {
    return 0;
  }

  std::size_t n = 0;
  const auto& channels = client->GetChannels();
  for (const auto& channel : channels) {
    n += UnSubscribe(client, channel);
  }

  return n;
}

size_t PPubsub::PSubscribe(PClient* client, const PString& channel) {
  if (client && client->PSubscribe(channel)) {
    auto it(patternChannels_.find(channel));
    if (it == patternChannels_.end()) {
      it = patternChannels_.insert(ChannelClients::value_type(channel, Clients())).first;
    }

    assert(it != patternChannels_.end());

    bool succ = it->second.insert(std::static_pointer_cast<PClient>(client->shared_from_this())).second;
    assert(succ);
    return 1;
  }

  return 0;
}

std::size_t PPubsub::PUnSubscribe(PClient* client, const PString& channel) {
  if (client && client->PUnSubscribe(channel)) {
    auto it(patternChannels_.find(channel));
    assert(it != patternChannels_.end());

    Clients& clientSet = it->second;

    std::size_t n = clientSet.erase(std::static_pointer_cast<PClient>(client->shared_from_this()));
    assert(n == 1);

    if (clientSet.empty()) {
      patternChannels_.erase(it);
    }

    return client->PatternChannelCount();
  }

  return 0;
}

std::size_t PPubsub::PUnSubscribeAll(PClient* client) {
  if (!client) {
    return 0;
  }

  std::size_t n = 0;
  const auto& channels = client->GetPatternChannels();
  for (const auto& channel : channels) {
    n += PUnSubscribe(client, channel);
  }

  return n;
}

std::size_t PPubsub::PublishMsg(const PString& channel, const PString& msg) {
  std::size_t n = 0;

  auto it(channels_.find(channel));
  if (it != channels_.end()) {
    Clients& clientSet = it->second;
    for (auto itCli(clientSet.begin()); itCli != clientSet.end();) {
      auto cli = itCli->lock();
      if (!cli) {
        clientSet.erase(itCli++);
      } else {
        INFO("Publish msg:{} to {}:{}", msg, cli->PeerIP(), cli->PeerPort());

        UnboundedBuffer reply;
        PreFormatMultiBulk(3, &reply);
        FormatBulk("message", 7, &reply);
        FormatBulk(channel, &reply);
        FormatBulk(msg, &reply);
        cli->SendPacket(reply);

        ++itCli;
        ++n;
      }
    }
  }

  // TODO refactor
  for (auto& pattern : patternChannels_) {
    if (fnmatch(pattern.first.c_str(), channel.c_str(), FNM_NOESCAPE) == 0) {
      INFO("{} match {}", channel, pattern.first);
      Clients& clientSet = pattern.second;
      for (auto itCli(clientSet.begin()); itCli != clientSet.end();) {
        auto cli = itCli->lock();
        if (!cli) {
          clientSet.erase(itCli++);
        } else {
          INFO("Publish msg:{} to {}:{}", msg, cli->PeerIP(), cli->PeerPort());

          UnboundedBuffer reply;
          PreFormatMultiBulk(4, &reply);
          FormatBulk("pmessage", 8, &reply);
          FormatBulk(pattern.first, &reply);
          FormatBulk(channel, &reply);
          FormatBulk(msg, &reply);
          cli->SendPacket(reply);

          ++itCli;
          ++n;
        }
      }
    }
  }

  return n;
}

void PPubsub::RecycleClients(PString& startChannel, PString& startPattern) {
  recycleClients(channels_, startChannel);
  recycleClients(patternChannels_, startPattern);
}

void PPubsub::recycleClients(ChannelClients& channels, PString& start) {
  auto it(start.empty() ? channels.begin() : channels.find(start));
  if (it == channels.end()) {
    it = channels.begin();
  }

  size_t n = 0;
  while (it != channels.end() && n < 20) {
    Clients& cls = it->second;
    for (auto itCli(cls.begin()); itCli != cls.end();) {
      if (itCli->expired()) {
        cls.erase(itCli++);
        ++n;
      } else {
        ++itCli;
      }
    }

    if (cls.empty()) {
      INFO("erase channel {}", it->first);
      channels.erase(it++);
    } else {
      ++it;
    }
  }

  if (it != channels.end()) {
    start = it->first;
  } else {
    start.clear();
  }
}

void PPubsub::InitPubsubTimer() {
  auto loop = EventLoop::Self();
  loop->ScheduleRepeatedly(
      100, [&](std::string& channel, std::string& pattern) { PPubsub::Instance().RecycleClients(channel, pattern); },
      std::ref(startChannel_), std::ref(startPattern_));
}

void PPubsub::PubsubChannels(std::vector<PString>& res, const char* pattern) const {
  res.clear();

  for (const auto& elem : channels_) {
    if (!pattern || fnmatch(pattern, elem.first.c_str(), FNM_NOESCAPE) == 0) {
      res.push_back(elem.first);
    }
  }
}

size_t PPubsub::PubsubNumsub(const PString& channel) const {
  auto it = channels_.find(channel);

  if (it != channels_.end()) {
    return it->second.size();
  }

  return 0;
}

size_t PPubsub::PubsubNumpat() const {
  std::size_t n = 0;

  for (const auto& elem : patternChannels_) {
    n += elem.second.size();
  }

  return n;
}

// pubsub commands
PError subscribe(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  for (size_t i = 1; i < params.size(); ++i) {
    const auto& pa = params[i];
    size_t n = PPubsub::Instance().Subscribe(client, pa);
    if (n == 1) {
      PreFormatMultiBulk(3, reply);
      FormatBulk("subscribe", 9, reply);
      FormatBulk(pa, reply);
      FormatInt(client->ChannelCount(), reply);

      INFO("subscribe {} by {}:{}", pa, client->PeerIP(), client->PeerPort());
    }
  }

  return PError_ok;
}

PError psubscribe(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();
  for (size_t i = 1; i < params.size(); ++i) {
    const auto& pa = params[i];
    size_t n = PPubsub::Instance().PSubscribe(client, pa);
    if (n == 1) {
      PreFormatMultiBulk(3, reply);
      FormatBulk("psubscribe", 9, reply);
      FormatBulk(pa, reply);
      FormatInt(client->PatternChannelCount(), reply);

      INFO("psubscribe {} by {}:{}", pa, client->PeerIP(), client->PeerPort());
    }
  }

  return PError_ok;
}

PError unsubscribe(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();

  if (params.size() == 1) {
    const auto& channels = client->GetChannels();
    for (const auto& channel : channels) {
      FormatBulk(channel, reply);
    }

    PPubsub::Instance().UnSubscribeAll(client);
  } else {
    std::set<PString> channels;

    for (size_t i = 1; i < params.size(); ++i) {
      size_t n = PPubsub::Instance().UnSubscribe(client, params[i]);
      if (n == 1) {
        channels.insert(params[i]);
        INFO("unsubscribe {} by {}:{}", params[i], client->PeerIP(), client->PeerPort());
      }
    }

    PreFormatMultiBulk(channels.size(), reply);
    for (const auto& channel : channels) {
      FormatBulk(channel, reply);
    }
  }

  return PError_ok;
}

PError punsubscribe(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PClient* client = PClient::Current();

  if (params.size() == 1) {
    const auto& channels = client->GetPatternChannels();
    for (const auto& channel : channels) {
      FormatBulk(channel, reply);
    }

    PPubsub::Instance().PUnSubscribeAll(client);
  } else {
    std::set<PString> channels;

    for (size_t i = 1; i < params.size(); ++i) {
      size_t n = PPubsub::Instance().PUnSubscribe(client, params[i]);
      if (n == 1) {
        channels.insert(params[i]);
        INFO("punsubscribe {} by {}:{}", params[i], client->PeerIP(), client->PeerPort());
      }
    }

    PreFormatMultiBulk(channels.size(), reply);
    for (const auto& channel : channels) {
      FormatBulk(channel, reply);
    }
  }

  return PError_ok;
}

PError publish(const std::vector<PString>& params, UnboundedBuffer* reply) {
  size_t n = PPubsub::Instance().PublishMsg(params[1], params[2]);
  FormatInt(n, reply);

  return PError_ok;
}

// neixing command
PError pubsub(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params[1] == "channels") {
    if (params.size() > 3) {
      ReplyError(PError_param, reply);
      return PError_param;
    }

    std::vector<PString> res;
    PPubsub::Instance().PubsubChannels(res, params.size() == 3 ? params[2].c_str() : 0);
    PreFormatMultiBulk(res.size(), reply);
    for (const auto& channel : res) {
      FormatBulk(channel, reply);
    }
  } else if (params[1] == "numsub") {
    PreFormatMultiBulk(2 * (params.size() - 2), reply);
    for (size_t i = 2; i < params.size(); ++i) {
      size_t n = PPubsub::Instance().PubsubNumsub(params[i]);
      FormatBulk(params[i], reply);
      FormatInt(n, reply);
    }
  } else if (params[1] == "numpat") {
    if (params.size() != 2) {
      ReplyError(PError_param, reply);
      return PError_param;
    }

    FormatInt(PPubsub::Instance().PubsubNumpat(), reply);
  } else {
    ERROR("Unknown pubsub subcmd {}", params[1]);
  }

  return PError_ok;
}

}  // namespace pikiwidb
