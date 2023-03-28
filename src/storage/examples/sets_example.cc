//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "storage/storage.h"

using namespace storage;

int main() {
  storage::BlackWidow db;
  BlackwidowOptions bw_options;
  bw_options.options.create_if_missing = true;
  storage::Status s = db.Open(bw_options, "./db");
  if (s.ok()) {
    printf("Open success\n");
  } else {
    printf("Open failed, error: %s\n", s.ToString().c_str());
    return -1;
  }
  // SAdd
  int32_t ret = 0;
  std::vector<std::string> members {"MM1", "MM2", "MM3", "MM2"};
  s = db.SAdd("SADD_KEY", members, &ret);
  printf("SAdd return: %s, ret = %d\n", s.ToString().c_str(), ret);

  // SCard
  ret = 0;
  s = db.SCard("SADD_KEY", &ret);
  printf("SCard, return: %s, scard ret = %d\n", s.ToString().c_str(), ret);

  return 0;
}
