// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_PARSER_H_
#define PIKA_BINLOG_PARSER_H_

#include "pink/include/pink_conn.h"

#include "include/pika_binlog_transverter.h"

enum BinlogParserStatus {
  kBinlogParserNone = 0,
  kBinlogParserHeaderDone = 1, // HEADER_LEN
  kBinlogParserDecodeDone = 2 // BINLOG_ENCODE_LEN 
};

class PikaBinlogParser {
 public:
  PikaBinlogParser();
  ~PikaBinlogParser();
  /* Scrube header and decode info from input
   * Provide bytes need to be scrubed from rbuf
   * the scrubed rbuf is std redis string
   * Return one command at most
   *
   * @param rbub len, input read buffer and length
   * @param processed_len, binlog parser parsed length
   * including cached length and parsed length
   * @param scrubed_len, len need to be scrubed from the begining fo rbuf
   */
  pink::ReadStatus ScrubReadBuffer(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item);
  BinlogHeader& binlog_header() {
    return binlog_header_;
  }
  BinlogItem& binlog_item() {
    return binlog_item_;
  }
 private:
  pink::ReadStatus BinlogParseHeader(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item);

  pink::ReadStatus BinlogParseDecode(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item);

  pink::ReadStatus BinlogParseContent(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item);

  void ResetStatus();
  BinlogHeader binlog_header_;
  BinlogItem binlog_item_;
  uint32_t processed_item_content_len_;
  std::string half_binlog_buf_; 

  BinlogParserStatus parse_status_;
};

#endif
