// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_META
#define PIKA_META

#include <shared_mutex>

#include "include/pika_define.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_mutex.h"

class PikaMeta : public pstd::noncopyable {
 public:
  PikaMeta() = default;
  ~PikaMeta() = default;

  void SetPath(const std::string& path);

  pstd::Status StableSave(const std::vector<DBStruct>& db_structs);
  pstd::Status ParseMeta(std::vector<DBStruct>* db_structs);

 private:
  std::shared_mutex rwlock_;
  std::string local_meta_path_;
};

#endif
