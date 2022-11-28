// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_INCLUDE_PINK_CLI_H_
#define PINK_INCLUDE_PINK_CLI_H_

#include <string>

#include "slash/include/slash_status.h"

using slash::Status;

namespace pink {

class PinkCli {
 public:
  explicit PinkCli(const std::string& ip = "", const int port = 0);
  virtual ~PinkCli();

  Status Connect(const std::string& bind_ip = "");
  Status Connect(const std::string &peer_ip, const int peer_port,
      const std::string& bind_ip = "");
  // Check whether the connection got fin from peer or not
  virtual int CheckAliveness(void);
  // Compress and write the message
  virtual Status Send(void *msg) = 0;

  // Read, parse and store the reply
  virtual Status Recv(void *result = NULL) = 0;

  void Close();

  // TODO(baotiao): delete after redis_cli use RecvRaw
  int fd() const;

  bool Available() const;

  // default connect timeout is 1000ms
  int set_send_timeout(int send_timeout);
  int set_recv_timeout(int recv_timeout);
  void set_connect_timeout(int connect_timeout);

 protected:
  Status SendRaw(void* buf, size_t len);
  Status RecvRaw(void* buf, size_t* len);

 private:
  struct Rep;
  Rep* rep_;
  int set_tcp_nodelay();

  PinkCli(const PinkCli&);
  void operator=(const PinkCli&);
};

extern PinkCli *NewPbCli(
    const std::string& peer_ip = "",
    const int peer_port = 0);

extern PinkCli *NewRedisCli();

}  // namespace pink
#endif  // PINK_INCLUDE_PINK_CLI_H_
