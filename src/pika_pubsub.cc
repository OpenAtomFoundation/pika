// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_pubsub.h"

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

static std::string ConstructPubSubResp(const std::string& cmd, const std::vector<std::pair<std::string, int>>& result) {
  std::stringstream resp;
  if (result.empty()) {
    resp << "*3\r\n"
         << "$" << cmd.length() << "\r\n"
         << cmd << "\r\n"
         << "$" << -1 << "\r\n"
         << ":" << 0 << "\r\n";
  }
  for (const auto & it : result) {
    resp << "*3\r\n"
         << "$" << cmd.length() << "\r\n"
         << cmd << "\r\n"
         << "$" << it.first.length() << "\r\n"
         << it.first << "\r\n"
         << ":" << it.second << "\r\n";
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

void PublishCmd::Do(std::shared_ptr<Slot> slot) {
  int receivers = g_pika_server->Publish(channel_, msg_);
  res_.AppendInteger(receivers);
}

void SubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSubscribe);
    return;
  }
  for (size_t i = 1; i < argv_.size(); i++) {
    channels_.push_back(argv_[i]);
  }
}

void SubscribeCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameSubscribe);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (!cli_conn->IsPubSub()) {
    cli_conn->server_thread()->MoveConnOut(conn->fd());
    cli_conn->SetIsPubSub(true);
    cli_conn->SetHandleType(net::HandleType::kSynchronous);
    cli_conn->SetWriteCompleteCallback([cli_conn]() {
      if (!cli_conn->IsPubSub()) {
        return;
      }
      cli_conn->set_is_writable(true);
      g_pika_server->EnablePublish(cli_conn->fd());
    });
  }
  std::vector<std::pair<std::string, int>> result;
  g_pika_server->Subscribe(conn, channels_, name_ == kCmdNamePSubscribe, &result);
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void UnSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameUnSubscribe);
    return;
  }
  for (size_t i = 1; i < argv_.size(); i++) {
    channels_.push_back(argv_[i]);
  }
}

void UnSubscribeCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNameUnSubscribe);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  std::vector<std::pair<std::string, int>> result;
  int subscribed = g_pika_server->UnSubscribe(conn, channels_, name_ == kCmdNamePUnSubscribe, &result);
  if (subscribed == 0 && cli_conn->IsPubSub()) {
    /*
     * if the number of client subscribed is zero,
     * the client will exit the Pub/Sub state
     */
    cli_conn->SetIsPubSub(false);
    cli_conn->SetWriteCompleteCallback([cli_conn, conn]() {
      if (cli_conn->IsPubSub()) {
        return;
      }
      cli_conn->set_is_writable(false);
      cli_conn->SetHandleType(net::HandleType::kAsynchronous);
      cli_conn->server_thread()->MoveConnIn(conn, net::NotifyType::kNotiWait);
    });
  }
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePSubscribe);
    return;
  }
  for (size_t i = 1; i < argv_.size(); i++) {
    channels_.push_back(argv_[i]);
  }
}

void PSubscribeCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePSubscribe);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (!cli_conn->IsPubSub()) {
    cli_conn->server_thread()->MoveConnOut(conn->fd());
    cli_conn->SetIsPubSub(true);
    cli_conn->SetHandleType(net::HandleType::kSynchronous);
    cli_conn->SetWriteCompleteCallback([cli_conn]() {
      if (!cli_conn->IsPubSub()) {
        return;
      }
      cli_conn->set_is_writable(true);
      g_pika_server->EnablePublish(cli_conn->fd());
    });
  }
  std::vector<std::pair<std::string, int>> result;
  g_pika_server->Subscribe(conn, channels_, name_ == kCmdNamePSubscribe, &result);
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PUnSubscribeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePUnSubscribe);
    return;
  }
  for (size_t i = 1; i < argv_.size(); i++) {
    channels_.push_back(argv_[i]);
  }

}

void PUnSubscribeCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<net::NetConn> conn = GetConn();
  if (!conn) {
    res_.SetRes(CmdRes::kErrOther, kCmdNamePUnSubscribe);
    LOG(WARNING) << name_ << " weak ptr is empty";
    return;
  }
  std::shared_ptr<PikaClientConn> cli_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);

  std::vector<std::pair<std::string, int>> result;
  int subscribed = g_pika_server->UnSubscribe(conn, channels_, name_ == kCmdNamePUnSubscribe, &result);
  if (subscribed == 0 && cli_conn->IsPubSub()) {
    /*
     * if the number of client subscribed is zero,
     * the client will exit the Pub/Sub state
     */
    cli_conn->SetIsPubSub(false);
    cli_conn->SetWriteCompleteCallback([cli_conn, conn]() {
      if (cli_conn->IsPubSub()) {
        return;
      }
      cli_conn->set_is_writable(false);
      cli_conn->SetHandleType(net::HandleType::kAsynchronous);
      cli_conn->server_thread()->MoveConnIn(conn, net::NotifyType::kNotiWait);
    });
  }
  return res_.SetRes(CmdRes::kNone, ConstructPubSubResp(name_, result));
}

void PubSubCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePubSub);
    return;
  }
  subcommand_ = argv_[1];
  if (strcasecmp(subcommand_.data(), "channels") != 0 && strcasecmp(subcommand_.data(), "numsub") != 0 &&
      strcasecmp(subcommand_.data(), "numpat") != 0) {
    res_.SetRes(CmdRes::kErrOther, "Unknown PUBSUB subcommand or wrong number of arguments for '" + subcommand_ + "'");
  }
  for (size_t i = 2; i < argv_.size(); i++) {
    arguments_.push_back(argv_[i]);
  }
}

void PubSubCmd::Do(std::shared_ptr<Slot> slot) {
  if (strcasecmp(subcommand_.data(), "channels") == 0) {
    std::string pattern;
    std::vector<std::string> result;
    if (arguments_.size() == 1) {
      pattern = arguments_[0];
    } else if (arguments_.size() > 1) {
      res_.SetRes(CmdRes::kErrOther,
                  "Unknown PUBSUB subcommand or wrong number of arguments for '" + subcommand_ + "'");
      return;
    }
    g_pika_server->PubSubChannels(pattern, &result);

    res_.AppendArrayLenUint64(result.size());
    for (auto &it : result) {
      res_.AppendStringLenUint64(it.length());
      res_.AppendContent(it);
    }
  } else if (strcasecmp(subcommand_.data(), "numsub") == 0) {
    std::vector<std::pair<std::string, int>> result;
    g_pika_server->PubSubNumSub(arguments_, &result);
    res_.AppendArrayLenUint64(result.size() * 2);
    for (auto &it : result) {
      res_.AppendStringLenUint64(it.first.length());
      res_.AppendContent(it.first);
      res_.AppendInteger(it.second);
    }
    return;
  } else if (strcasecmp(subcommand_.data(), "numpat") == 0) {
    int subscribed = g_pika_server->PubSubNumPat();
    res_.AppendInteger(subscribed);
  }
}
