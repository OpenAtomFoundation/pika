// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fcntl.h>
#include <google/protobuf/message.h>
#include <netinet/in.h>
#include <unistd.h>

#include "net/include/net_cli.h"
#include "net/include/net_define.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/xdebug.h"

using pstd::Status;
namespace net {

// Default PBCli is block IO;
class PbCli : public NetCli {
 public:
  PbCli(const std::string& ip, int port);
  ~PbCli() override;

  // msg should have been parsed
  Status Send(void* msg_req) override;

  // Read, parse and store the reply
  Status Recv(void* msg_res) override;

  PbCli(const PbCli&) = delete;
  void operator=(const PbCli&) = delete;
 private:
  // BuildWbuf need to access rbuf_, wbuf_;
  char* rbuf_;
  char* wbuf_;

};

PbCli::PbCli(const std::string& ip, const int port) : NetCli(ip, port) {
  rbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * kProtoMaxMessage));
  wbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * kProtoMaxMessage));
}

PbCli::~PbCli() {
  free(wbuf_);
  free(rbuf_);
}

Status PbCli::Send(void* msg) {
  auto req = reinterpret_cast<google::protobuf::Message*>(msg);

  int wbuf_len = req->ByteSizeLong();
  req->SerializeToArray(wbuf_ + kCommandHeaderLength, wbuf_len);
  uint32_t len = htonl(wbuf_len);
  memcpy(wbuf_, &len, sizeof(uint32_t));
  wbuf_len += kCommandHeaderLength;

  return NetCli::SendRaw(wbuf_, wbuf_len);
}

Status PbCli::Recv(void* msg_res) {
  auto res = reinterpret_cast<google::protobuf::Message*>(msg_res);

  // Read Header
  size_t read_len = kCommandHeaderLength;
  Status s = RecvRaw(reinterpret_cast<void*>(rbuf_), &read_len);
  if (!s.ok()) {
    return s;
  }

  uint32_t integer;
  memcpy(reinterpret_cast<char*>(&integer), rbuf_, sizeof(uint32_t));
  size_t packet_len = ntohl(integer);

  // Read Packet
  s = RecvRaw(reinterpret_cast<void*>(rbuf_), &packet_len);
  if (!s.ok()) {
    return s;
  }

  if (!res->ParseFromArray(rbuf_, packet_len)) {
    return Status::Corruption("PbCli::Recv Protobuf ParseFromArray error");
  }
  return Status::OK();
}

NetCli* NewPbCli(const std::string& peer_ip, const int peer_port) { return new PbCli(peer_ip, peer_port); }

}  // namespace net
