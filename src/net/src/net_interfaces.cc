// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_interfaces.h"

#include <unistd.h>

#include <ifaddrs.h>
#include <arpa/inet.h>

#if defined(__APPLE__)
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>

#include "pstd/include/pstd_defer.h"

#else
#include <vector>
#include <fstream>
#include <sstream>
#include <iterator>

#endif

#include "pstd/include/xdebug.h"

std::string GetDefaultInterface() {
#if defined(__APPLE__)
  std::string name("lo0");

  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    return name;
  }

  DEFER { close(fd); };

  struct ifreq *ifreq;
  struct ifconf ifconf;
  char buf[16384];

  ifconf.ifc_len = sizeof buf;
  ifconf.ifc_buf = buf;
  if (ioctl(fd, SIOCGIFCONF, &ifconf) != 0) {
    log_err("ioctl(SIOCGIFCONF) failed");
    return name;
  }

  ifreq = ifconf.ifc_req;
  for (unsigned int i = 0; i < ifconf.ifc_len;) {
    /* some systems have ifr_addr.sa_len and adjust the length that
     * way, but not mine. weird */
    size_t len = IFNAMSIZ + ifreq->ifr_addr.sa_len;
    name = ifreq->ifr_name;
    if (!name.empty()) {
      log_info("got interface %s", name.c_str());
      break;
    }

    ifreq = (struct ifreq*)((char*)ifreq+len);
    i += len;
  }

  return name;
#else
  std::string name("eth0");
  std::ifstream routeFile("/proc/net/route", std::ios_base::in);
  if (!routeFile.good())
    return name;

  std::string line;
  std::vector<std::string> tokens;
  while(std::getline(routeFile, line)) {
    std::istringstream stream(line);
    std::copy(std::istream_iterator<std::string>(stream),
        std::istream_iterator<std::string>(),
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

  log_info("Using Networker Interface: %s", network_interface.c_str());

  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;

  if (getifaddrs(&ifAddrStruct) == -1) {
    log_err("getifaddrs failed");
    return "";
  }

  std::string host;
  for (ifa = ifAddrStruct; ifa; ifa = ifa->ifa_next) {
    if (!ifa->ifa_addr) {
      continue;
    }

    if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is a valid IPv4 address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host = addressBuffer;
        break;
      }
    } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is a valid IPv6 address
      tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host = addressBuffer;
        break;
      }
    }
  }

  if (ifAddrStruct ) {
    freeifaddrs(ifAddrStruct);
  }

  if (!ifa) {
    log_err("error network interface: %s", network_interface.c_str());
  }

  log_info("got ip %s", host.c_str());
  return host;
}

