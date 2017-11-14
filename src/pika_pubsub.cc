// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_pubsub.h"
#include "pika_server.h"

extern PikaServer *g_pika_server;

void PublishCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePublish);
    return;
  }
  channel_ = slash::StringToLower(argv[1]);
  msg_ = argv[2];
}

void PublishCmd::Do() {
  int receivers = g_pika_server->Publish(channel_, msg_);
  res_.AppendInteger(receivers);
  return;
}

void SubscribeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSubscribe);
    return;
  }
}

void SubscribeCmd::Do() {
}

void UnSubscribeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameUnSubscribe);
    return;
  }
}

void UnSubscribeCmd::Do() {
}

void PSubscribeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePSubscribe);
    return;
  }
}

void PSubscribeCmd::Do() {
}

void PUnSubscribeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePUnSubscribe);
    return;
  }
}

void PUnSubscribeCmd::Do() {
}

void PubSubCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePubSub);
    return;
  }
  subcommand_ = slash::StringToLower(argv[1]);
  if (subcommand_ != "channels" && subcommand_ != "numsub" && subcommand_ != "numpat") {
    res_.SetRes(CmdRes::kErrOther, "Unknown PUBSUB subcommand or wrong number of arguments for '" + subcommand_ + "'");
  }
  for (size_t i = 2; i < argv.size(); i++) {
    arguments_.push_back(argv[i]); 
  }
}

void PubSubCmd::Do() {
  if (subcommand_ == "channels") {
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
  } else if (subcommand_ == "numsub") {
    std::vector<std::pair<std::string, int>> result;
    g_pika_server->PubSubNumSub(arguments_, &result);
    res_.AppendArrayLen(result.size() * 2);
    for (auto it = result.begin(); it != result.end(); ++it) {
      res_.AppendStringLen(it->first.length());
      res_.AppendContent(it->first); 
      res_.AppendInteger(it->second);
    }
    return;
  } else if (subcommand_ == "numpat") {
    int subscribed = g_pika_server->PubSubNumPat();
    res_.AppendInteger(subscribed);
  }
  return;
}

