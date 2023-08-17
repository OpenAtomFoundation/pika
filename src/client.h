/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "common.h"
#include "tcp_obj.h"

#include <set>
#include <unordered_map>
#include <unordered_set>
#include "proto_parser.h"
#include "replication.h"

namespace pikiwidb {

enum ClientFlag {
  ClientFlag_multi = 0x1,
  ClientFlag_dirty = 0x1 << 1,
  ClientFlag_wrongExec = 0x1 << 2,
  ClientFlag_master = 0x1 << 3,
};

class DB;
struct PSlaveInfo;

class PClient : public std::enable_shared_from_this<PClient> {
 public:
  PClient() = delete;
  explicit PClient(TcpObject* obj);

  int HandlePackets(pikiwidb::TcpObject*, const char*, int);

  void OnConnect();

  const std::string& PeerIP() const { return tcp_obj_->GetPeerIp(); }
  int PeerPort() const { return tcp_obj_->GetPeerPort(); }
  int SendPacket(UnboundedBuffer& data) { return tcp_obj_->SendPacket(data.ReadAddr(), data.ReadableSize()); }
  int SendPacket(const void* data, int size) { return tcp_obj_->SendPacket(data, size); }
  void Close();

  bool SelectDB(int db);
  static PClient* Current();

  // multi
  void SetFlag(unsigned flag) { flag_ |= flag; }
  void ClearFlag(unsigned flag) { flag_ &= ~flag; }
  bool IsFlagOn(unsigned flag) { return flag_ & flag; }
  void FlagExecWrong() {
    if (IsFlagOn(ClientFlag_multi)) {
      SetFlag(ClientFlag_wrongExec);
    }
  }

  bool Watch(int dbno, const PString& key);
  bool NotifyDirty(int dbno, const PString& key);
  bool Exec();
  void ClearMulti();
  void ClearWatch();

  // pubsub
  std::size_t Subscribe(const PString& channel) { return channels_.insert(channel).second ? 1 : 0; }

  std::size_t UnSubscribe(const PString& channel) { return channels_.erase(channel); }

  std::size_t PSubscribe(const PString& channel) { return patternChannels_.insert(channel).second ? 1 : 0; }

  std::size_t PUnSubscribe(const PString& channel) { return patternChannels_.erase(channel); }

  const std::unordered_set<PString>& GetChannels() const { return channels_; }
  const std::unordered_set<PString>& GetPatternChannels() const { return patternChannels_; }
  std::size_t ChannelCount() const { return channels_.size(); }
  std::size_t PatternChannelCount() const { return patternChannels_.size(); }

  bool WaitFor(const PString& key, const PString* target = nullptr);

  const std::unordered_set<PString> WaitingKeys() const { return waitingKeys_; }
  void ClearWaitingKeys() { waitingKeys_.clear(), target_.clear(); }
  const PString& GetTarget() const { return target_; }

  void SetName(const PString& name) { name_ = name; }
  const PString& GetName() const { return name_; }

  void SetSlaveInfo();
  PSlaveInfo* GetSlaveInfo() const { return slaveInfo_.get(); }

  static void AddCurrentToMonitor();
  static void FeedMonitors(const std::vector<PString>& params);

  void SetAuth() { auth_ = true; }
  bool GetAuth() const { return auth_; }
  void RewriteCmd(std::vector<PString>& params) { parser_.SetParams(params); }

 private:
  int handlePacket(pikiwidb::TcpObject*, const char*, int);
  int processInlineCmd(const char*, size_t, std::vector<PString>&);
  void reset();
  bool isPeerMaster() const;

  TcpObject* const tcp_obj_;

  PProtoParser parser_;
  UnboundedBuffer reply_;

  int db_ = -1;

  std::unordered_set<PString> channels_;
  std::unordered_set<PString> patternChannels_;

  unsigned flag_;
  std::unordered_map<int, std::unordered_set<PString> > watchKeys_;
  std::vector<std::vector<PString> > queueCmds_;

  // blocked list
  std::unordered_set<PString> waitingKeys_;
  PString target_;

  // slave info from master view
  std::unique_ptr<PSlaveInfo> slaveInfo_;

  // name
  std::string name_;

  // auth
  bool auth_ = false;
  time_t lastauth_ = 0;

  static PClient* s_current;
  static std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > > s_monitors;
};

}  // namespace pikiwidb
