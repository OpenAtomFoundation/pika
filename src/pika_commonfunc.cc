//// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
//// This source code is licensed under the BSD-style license found in the
//// LICENSE file in the root directory of this source tree. An additional grant
//// of patent rights can be found in the PATENTS file in the same directory.
//
//#include <ctime>
//#include <cstring>
//#include <cstdlib>
//
//#include <glog/logging.h>
//
//#include "include/pika_commonfunc.h"
//#include "include/pika_define.h"
//#include "include/pika_server.h"
//#include "include/pika_conf.h"
//
//#include "net/include/redis_conn.h"
//#include "net/include/redis_cli.h"
//#include "pstd/include/pstd_mutex.h"
//#include "pstd/include/pstd_status.h"
//#include "pstd/include/pstd_string.h"
//
//extern PikaServer *g_pika_server;
//extern PikaConf *g_pika_conf;
//
//// crc
//static const uint32_t IEEE_POLY = 0xedb88320;
//static const uint32_t CAST_POLY = 0x82f63b78;
//static const uint32_t KOOP_POLY = 0xeb31d82e;
//
//static uint32_t crc32tab[256];
//
//static void CRC32TableInit(uint32_t poly) {
//    int i, j;
//    for (i = 0; i < 256; i ++) {
//        uint32_t crc = i;
//        for (j = 0; j < 8; j ++) {
//            if (crc & 1) {
//                crc = (crc >> 1) ^ poly;
//            } else {
//                crc = (crc >> 1);
//            }
//        }
//        crc32tab[i] = crc;
//    }
//}
//
//void PikaCommonFunc::InitCRC32Table(void) {
//  CRC32TableInit(IEEE_POLY);
//}
//
//uint32_t PikaCommonFunc::CRC32Update(uint32_t crc, const char *buf, int len) {
//  crc = ~crc;
//  for (int i = 0; i < len; i++) {
//    crc = crc32tab[(uint8_t)((char)crc ^ buf[i])] ^ (crc >> 8);
//  }
//  return ~crc;
//}
//
//uint32_t PikaCommonFunc::CRC32CheckSum(const char *buf, int len) {
//  return CRC32Update(0, buf, len);
//}
//
//bool PikaCommonFunc::DoAuth(net::PinkCli *client, const std::string requirepass) {
//    if (client == nullptr) {
//      return false;
//    }
//
//    net::RedisCmdArgsType argv;
//    std::string wbuf_str;
//    if (requirepass != "") {
//      argv.push_back("auth");
//      argv.push_back(requirepass);
//      net::SerializeRedisCommand(argv, &wbuf_str);
//
//      pstd::Status s;
//      s = client->Send(&wbuf_str);
//      if (!s.ok()) {
//        LOG(WARNING) << "PikaCommonFunc::DoAuth Slot Migrate auth error: " << strerror(errno);
//        return false;
//      }
//
//      // Recv
//      s = client->Recv(&argv);
//      if (!s.ok()) {
//        LOG(WARNING) << "PikaCommonFunc::DoAuth Slot Migrate auth Recv error: " << strerror(errno);
//        return false;
//      }
//
//      if (kInnerReplOk != pstd::StringToLower(argv[0])) {
//        LOG(ERROR) << "PikaCommonFunc::DoAuth auth error";
//        return false;
//      }
//    }
//    return true;
//}
//
//void PikaCommonFunc::BinlogPut(const std::string &key, const std::string &raw_args) {
//  uint32_t crc = CRC32Update(0, key.data(), (int)key.size());
//  int binlog_writer_num = g_pika_conf->binlog_writer_num();
//  int thread_index = (int)(crc % binlog_writer_num);
//  pstd::Status s = g_pika_server->binlog_write_thread_[thread_index]->WriteBinlog(raw_args, "sync" == g_pika_conf->binlog_writer_method());
//  if (!s.ok()) {
//    LOG(ERROR) << "Writing binlog failed, maybe no space left on device";
//    for (int i = 0; i < binlog_writer_num; i++) {
//      if (i != thread_index) {
//        g_pika_server->binlog_write_thread_[i]->SetBinlogIoError(true);
//      }
//    }
//    g_pika_conf->SetReadonly(true);
//  }
//}
//
//std::string PikaCommonFunc::TimestampToDate(int64_t timestamp) {
//  time_t t = static_cast<time_t>(timestamp);
//  char buf[32] = {0};
//  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&t));
//  return std::string(buf);
//}
//
//std::string PikaCommonFunc::AppendSubDirectory(const std::string& db_path, const std::string& sub_path) {
//  if (db_path.back() == '/') {
//    return db_path + sub_path;
//  } else {
//    return db_path + "/" + sub_path;
//  }
//}
