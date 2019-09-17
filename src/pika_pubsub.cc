// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_pubsub.h"

#include "include/pika_server.h"

extern PikaServer *g_pika_server;


static std::string ConstructPubSubResp(
                                const std::string& cmd,
                                const std::vector<std::pair<std::string, int>>& result) {
  std::stringstream resp;
  if (result.size() == 0) {
    resp << "*3\r\n" << "$" << cmd.length() << "\r\n" << cmd << "\r\n" <<
                        "$" << -1           << "\r\n" << ":" << 0      << "\r\n";
  }
  for (auto it = result.begin(); it != result.end(); it++) {
    resp << "*3\r\n" << "$" << cmd.length()       << "\r\n" << cmd       << "\r\n" <<
                        "$" << it->first.length() << "\r\n" << it->first << "\r\n" <<
                        ":" << it->second         << "\r\n";
  }
  return resp.str();
}


void PublishCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePublish);
    return;
  }
  channel_ = argv_[1];
  msg_ = argv_[2];
}

void PublishCmd::Do(std::shared_ptr<Partition> partition) {
  int receivers = g_pika_server->Publish(channel_, msg_);
  res_.AppendInteger(receivers);
  return;
}

void SubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSubscribe);
    return;
  }
}

void SubscribeCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<pink::PinkConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameSubscribe);
    LOG(WARNING) << name_  << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  if (!cli_conn->IsPubSub()) {
    cli_conn->server_thread()->MoveConnOut(conn->fd());
  }
  std::vector<std::string > channels;
  for (size_t i = 1; i < argv_.size(); i++) {
    channels.push_back(argv_[i]);
  }
  std::vector<std::pair<std::string, int>> result;
  cli_conn->SetIsPubSub(true);
  cli_conn->SetHandleType(pink::HandleType::kSynchronous);
  g_pika_server->Subscribe(conn, channels, name_ == kCmdNamePSubscribe, &result);
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void UnSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameUnSubscribe);
    return;
  }
}

void UnSubscribeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string > channels;
  for (size_t i = 1; i < argv_.size(); i++) {
    channels.push_back(argv_[i]);
  }

  std::shared_ptr<pink::PinkConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameUnSubscribe);
    LOG(WARNING) << name_  << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  std::vector<std::pair<std::string, int>> result;
  int subscribed = g_pika_server->UnSubscribe(conn, channels, name_ == kCmdNamePUnSubscribe, &result);
  if (subscribed == 0 && cli_conn->IsPubSub()) {
    /*
     * if the number of client subscribed is zero,
     * the client will exit the Pub/Sub state
     */
    cli_conn->server_thread()->HandleNewConn(conn->fd(), conn->ip_port());
    cli_conn->SetIsPubSub(false);
  }
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePSubscribe);
    return;
  }
}

void PSubscribeCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<pink::PinkConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePSubscribe);
    LOG(WARNING) << name_  << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  if (!cli_conn->IsPubSub()) {
    cli_conn->server_thread()->MoveConnOut(conn->fd());
  }
  std::vector<std::string > channels;
  for (size_t i = 1; i < argv_.size(); i++) {
    channels.push_back(argv_[i]);
  }
  std::vector<std::pair<std::string, int>> result;
  cli_conn->SetIsPubSub(true);
  cli_conn->SetHandleType(pink::HandleType::kSynchronous);
  g_pika_server->Subscribe(conn, channels, name_ == kCmdNamePSubscribe, &result);
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PUnSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePUnSubscribe);
    return;
  }
}

void PUnSubscribeCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<std::string > channels;
  for (size_t i = 1; i < argv_.size(); i++) {
    channels.push_back(argv_[i]);
  }

  std::shared_ptr<pink::PinkConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePUnSubscribe);
    LOG(WARNING) << name_  << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  std::vector<std::pair<std::string, int>> result;
  int subscribed = g_pika_server->UnSubscribe(conn, channels, name_ == kCmdNamePUnSubscribe, &result);
  if (subscribed == 0 && cli_conn->IsPubSub()) {
    /*
     * if the number of client subscribed is zero,
     * the client will exit the Pub/Sub state
     */
    cli_conn->server_thread()->HandleNewConn(conn->fd(), conn->ip_port());
    cli_conn->SetIsPubSub(false);
  }
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PubSubCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePubSub);
    return;
  }
  subcommand_ = argv_[1];
  if (strcasecmp(subcommand_.data(), "channels")
    && strcasecmp(subcommand_.data(), "numsub")
    && strcasecmp(subcommand_.data(), "numpat")) {
    res_.SetRes(CmdRes::kErrOther, "Unknown PUBSUB subcommand or wrong number of arguments for '" + subcommand_ + "'");
  }
  for (size_t i = 2; i < argv_.size(); i++) {
    arguments_.push_back(argv_[i]); 
  }
}

void PubSubCmd::Do(std::shared_ptr<Partition> partition) {
  if (!strcasecmp(subcommand_.data(), "channels")) {
    std::string pattern = "";
    std::vector<std::string > result;
    if (arguments_.size() == 1) {
      pattern = arguments_[0];
    } else if (arguments_.size() > 1) {
      res_.SetRes(CmdRes::kErrOther, "Unknown PUBSUB subcommand or wrong number of arguments for '" + subcommand_ + "'");
      return;
    }
    g_pika_server->PubSubChannels(pattern, &result);

    res_.AppendArrayLen(result.size());
    for (auto it = result.begin(); it != result.end(); ++it) {
      res_.AppendStringLen((*it).length());
      res_.AppendContent(*it);
    }
  } else if (!strcasecmp(subcommand_.data(), "numsub")) {
    std::vector<std::pair<std::string, int>> result;
    g_pika_server->PubSubNumSub(arguments_, &result);
    res_.AppendArrayLen(result.size() * 2);
    for (auto it = result.begin(); it != result.end(); ++it) {
      res_.AppendStringLen(it->first.length());
      res_.AppendContent(it->first); 
      res_.AppendInteger(it->second);
    }
    return;
  } else if (!strcasecmp(subcommand_.data(), "numpat")) {
    int subscribed = g_pika_server->PubSubNumPat();
    res_.AppendInteger(subscribed);
  }
  return;
}

