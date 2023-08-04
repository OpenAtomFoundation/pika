// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_COMMONFUNC_H_
#define PIKA_COMMONFUNC_H_

#include <cstdint>
#include <string>

#include "net/include/net_cli.h"


class PikaCommonFunc {
 public:
  static void InitCRC32Table(void);
  static uint32_t CRC32Update(uint32_t crc, const char *buf, int len);
  static uint32_t CRC32CheckSum(const char *buf, int len);
  static bool DoAuth(net::NetCli *client, const std::string requirepass);
  static void BinlogPut(const std::string &key, const std::string &raw_args);
  static std::string TimestampToDate(int64_t timestamp);
  static std::string AppendSubDirectory(const std::string& db_path, const std::string& sub_path);
    
 private:
  PikaCommonFunc();
  ~PikaCommonFunc();
};

#endif