// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef LIKELY_H
#define LIKELY_H

#if defined(__GNUC__) && __GNUC__ >= 4
#  define LIKELY(x) (__builtin_expect((x), 1))
#  define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#  define LIKELY(x) (x)
#  define UNLIKELY(x) (x)
#endif

#endif  // LIKELY_H
