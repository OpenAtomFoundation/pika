// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_interfaces.h"

#include <unistd.h>

#include <glog/logging.h>

#include <arpa/inet.h>
#include <ifaddrs.h>

#if defined(__APPLE__)
#  include <net/if.h>
#  include <sys/ioctl.h>
#  include <sys/socket.h>
#  include <unistd.h>

#  include "pstd/include/pstd_defer.h"

#else
#  include <fstream>
#  include <iterator>
#  include <sstream>
#  include <vector>

#endif

#include "pstd/include/xdebug.h"

std::string GetDefaultInterface() {
#if defined(__APPLE__)
  std::string name("lo0");

  int32_t fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return name;
  }

  DEFER { close(fd); };

  struct ifreq* ifreq;
  struct ifconf ifconf;
  char buf[16384];

  ifconf.ifc_len = sizeof buf;
  ifconf.ifc_buf = buf;
  if (ioctl(fd, SIOCGIFCONF, &ifconf) != 0) {
    LOG(ERROR) << "ioctl(SIOCGIFCONF) failed";
    return name;
  }

  ifreq = ifconf.ifc_req;
  for (uint32_t i = 0; i < ifconf.ifc_len;) {
    /* some systems have ifr_addr.sa_len and adjust the length that
     * way, but not mine. weird */
    size_t len = IFNAMSIZ + ifreq->ifr_addr.sa_len;
    name = ifreq->ifr_name;
    if (!name.empty()) {
      LOG(INFO) << "got interface " << name;
      break;
    }

    ifreq = reinterpret_cast<struct ifreq*>(reinterpret_cast<char*>(ifreq) + len);
    i += len;
  }

  return name;
#else
  std::string name("eth0");
  std::ifstream routeFile("/proc/net/route", std::ios_base::in);
  if (!routeFile.good()) {
    return name;
  }

  std::string line;
  std::vector<std::string> tokens;
  while (std::getline(routeFile, line)) {
    std::istringstream stream(line);
    std::copy(std::istream_iterator<std::string>(stream), std::istream_iterator<std::string>(),
              std::back_inserter<std::vector<std::string> >(tokens));

    // the default interface is the one having the second
    // field, Destination, set to "00000000"
    if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000"))) {
      name = tokens[0];
      break;
    }

    tokens.clear();
  }

  return name;
#endif
}

std::string GetIpByInterface(const std::string& network_interface) {
  if (network_interface.empty()) {
    return "";
  }

  LOG(INFO) << "Using Networker Interface: " << network_interface;

  struct ifaddrs* ifAddrStruct = nullptr;
  struct ifaddrs* ifa = nullptr;
  void* tmpAddrPtr = nullptr;

  if (getifaddrs(&ifAddrStruct) == -1) {
    LOG(ERROR) << "getifaddrs failed";
    return "";
  }

  std::string host;
  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (!(ifa->ifa_addr)) {
      continue;
    }

    if (ifa->ifa_addr->sa_family == AF_INET) {  // Check it is a valid IPv4 address
      tmpAddrPtr = &(reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr))->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host = addressBuffer;
        break;
      }
    } else if (ifa->ifa_addr->sa_family == AF_INET6) {  // Check it is a valid IPv6 address
      tmpAddrPtr = &(reinterpret_cast<struct sockaddr_in6*>(ifa->ifa_addr))->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host = addressBuffer;
        break;
      }
    }
  }

  if (ifAddrStruct) {
    freeifaddrs(ifAddrStruct);
  }

  if (!ifa) {
    LOG(ERROR) << "error network interface: " << network_interface;
  }

  LOG(INFO) << "got ip " << host;
  return host;
}
