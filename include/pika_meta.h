// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_META
#define PIKA_META

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"

#include "include/pika_define.h"

using slash::Status;

class PikaMeta {
 public:
  PikaMeta();
  ~PikaMeta();

  void SetPath(const std::string& path);

  Status StableSave(const std::vector<TableStruct>& table_structs);
  Status ParseMeta(std::vector<TableStruct>* const table_structs);

 private:
  pthread_rwlock_t rwlock_;
  std::string local_meta_path_;

  // No copying allowed;
  PikaMeta(const PikaMeta&);
  void operator=(const PikaMeta&);
};

#endif
