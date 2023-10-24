//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_DEBUG_H_
#define SRC_DEBUG_H_

#ifndef NDEBUG
#  define TRACE(M, ...) fprintf(stderr, "[TRACE] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#  define DEBUG(M, ...) fprintf(stderr, "[Debug] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#  define TRACE(M, ...) {}
#  define DEBUG(M, ...) {}
#endif  // NDEBUG

static std::string get_printable_key(const std::string& key) {
  std::string res;
  for (int i = 0; i < key.size(); i++) {
    if (std::isprint(key[i])) {
      res.append(1, key[i]);
    } else {
      char tmp[3];
      sprintf(tmp, "%02x", key[i] & 0xFF);
      res.append(tmp, 2);
    }
  }
  return res;
}


#endif  // SRC_DEBUG_H_
