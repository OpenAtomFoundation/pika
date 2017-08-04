// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BUILD_VERSION_H_
#define INCLUDE_BUILD_VERSION_H_

// this variable tells us about the git revision
extern const char* pika_build_git_sha;

// Date on which the code was compiled:
extern const char* pika_build_compile_date;

#endif  // INCLUDE_BUILD_VERSION_H_
