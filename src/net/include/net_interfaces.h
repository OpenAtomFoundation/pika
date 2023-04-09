// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_NET_INTERFACES_H_
#define NET_INCLUDE_NET_INTERFACES_H_

#include <string>

std::string GetDefaultInterface();
std::string GetIpByInterface(const std::string& network_interface);

#endif
