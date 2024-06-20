// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_THREAD_NAME_H
#define NET_THREAD_NAME_H

#include <pthread.h>
#include <string>

namespace net {

#if defined(__GLIBC__) && !defined(__APPLE__) && !defined(__ANDROID__)
#  if __GLIBC_PREREQ(2, 12)
// has pthread_setname_np(pthread_t, const char*) (2 params)
#    define HAS_PTHREAD_SETNAME_NP 1
#  endif
#endif

#ifdef HAS_PTHREAD_SETNAME_NP
inline bool SetThreadName(pthread_t id, const std::string& name) {
  // printf ("use pthread_setname_np(%s)\n", name.substr(0, 15).c_str());
  return 0 == pthread_setname_np(id, name.substr(0, 15).c_str());
}
#else
inline bool SetThreadName(pthread_t id, const std::string& name) {
  // printf ("no pthread_setname\n");
  return pthread_setname_np(name.c_str()) == 0;
}
#endif
}  // namespace net

#endif
