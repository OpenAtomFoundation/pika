/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <fstream>

#include "helper.h"

#if defined(__gnu_linux__)
#  include <fcntl.h>

#elif defined(__APPLE__)
#  include <mach/mach_init.h>
#  include <mach/task.h>

#else
#  error "unknow platform"

#endif

namespace pikiwidb {

static unsigned dict_hash_function_seed = 5381;

// hash func from redis
unsigned int dictGenHashFunction(const void* key, int len) {
  /* 'm' and 'r' are mixing constants generated offline.
   They're not really 'magic', they just happen to work well.  */
  unsigned seed = dict_hash_function_seed;
  const unsigned m = 0x5bd1e995;
  const int r = 24;

  /* Initialize the hash to a 'random' value */
  unsigned h = seed ^ len;

  /* Mix 4 bytes at a time into the hash */
  const unsigned char* data = (const unsigned char*)key;

  while (len >= 4) {
    unsigned int k = *(unsigned int*)data;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  /* Handle the last few bytes of the input array  */
  switch (len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };

  /* Do a few final mixes of the hash to ensure the last few
   * bytes are well-incorporated. */
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return (unsigned int)h;
}

// hash function
size_t my_hash::operator()(const PString& str) const {
  return dictGenHashFunction(str.data(), static_cast<int>(str.size()));
}

static const uint8_t bitsinbyte[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2,
    3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
    3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
    6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4,
    5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
    6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

std::size_t BitCount(const uint8_t* buf, std::size_t len) {
  std::size_t cnt = 0;
  std::size_t loop = len / 4;
  std::size_t remain = len % 4;

  for (std::size_t i = 0; i < loop; ++i) {
    cnt += bitsinbyte[buf[4 * i]];
    cnt += bitsinbyte[buf[4 * i + 1]];
    cnt += bitsinbyte[buf[4 * i + 2]];
    cnt += bitsinbyte[buf[4 * i + 3]];
  }

  for (std::size_t i = 0; i < remain; ++i) {
    cnt += bitsinbyte[buf[4 * loop + i]];
  }

  return cnt;
}

/* Copy from redis source.
 * Generate the Redis "Run ID", a SHA1-sized random number that identifies a
 * given execution of Redis, so that if you are talking with an instance
 * having run_id == A, and you reconnect and it has run_id == B, you can be
 * sure that it is either a different instance or it was restarted. */
void getRandomHexChars(char* p, unsigned int len) {
  FILE* fp = fopen("/dev/urandom", "r");
  const char* charset = "0123456789abcdef";
  unsigned int j;

  if (fp == nullptr || fread(p, len, 1, fp) == 0) {
    /* If we can't read from /dev/urandom, do some reasonable effort
     * in order to create some entropy, since this function is used to
     * generate run_id and cluster instance IDs */
    char* x = p;
    unsigned int l = len;
    struct timeval tv;
    pid_t pid = getpid();

    /* Use time and PID to fill the initial array. */
    gettimeofday(&tv, nullptr);
    if (l >= sizeof(tv.tv_usec)) {
      memcpy(x, &tv.tv_usec, sizeof(tv.tv_usec));
      l -= sizeof(tv.tv_usec);
      x += sizeof(tv.tv_usec);
    }
    if (l >= sizeof(tv.tv_sec)) {
      memcpy(x, &tv.tv_sec, sizeof(tv.tv_sec));
      l -= sizeof(tv.tv_sec);
      x += sizeof(tv.tv_sec);
    }
    if (l >= sizeof(pid)) {
      memcpy(x, &pid, sizeof(pid));
      l -= sizeof(pid);
      x += sizeof(pid);
    }
    /* Finally xor it with rand() output, that was already seeded with
     * time() at startup. */
    for (j = 0; j < len; j++) {
      p[j] ^= rand();
    } 
  }
  /* Turn it into hex digits taking just 4 bits out of 8 for every byte. */
  for (j = 0; j < len; j++) {
    p[j] = charset[p[j] & 0x0F];
  }
  fclose(fp);
}

#if defined(__gnu_linux__)
/*
/proc/<PID>/status Field Description Option Explanation

VmPeak
peak virtual memory size

VmSize
This is the process's virtual set size, which is the amount of virtual memory that the application is using.
It's the same as the vsz parameter provided by ps.

VmLck
This is the amount of memory that has been locked by this process. Locked memory cannot be swapped to disk.

VmHWM : peak resident set size ("high water mark")
VmRSS
This is the resident set size or amount of physical memory the application is currently using. This is the same as the
rss statistic provided by ps.

VmData
This is the data size or the virtual size of the program's data usage. Unlike ps dsiz statistic, this does not include
stack information.

VmStk
This is the size of the process's stack.

VmExe
This is the virtual size of the executable memory that the program has. It does not include libraries that the process
is using.

VmLib
Size of shared library code. This is the size of the libraries that the process is using.

VmSwap
amount of swap used by anonymous private data(shmem swap usage is not included)
*/

std::vector<size_t> getMemoryInfo() {
  /*
  VmPeak = 0,
  VmSize = 1,
  VmLck = 2,
  VmHWM = 3,
  VmRSS = 4,
  VmSwap = 5, */
  std::vector<size_t> res(VmMax);
  // int page = sysconf(_SC_PAGESIZE);

  char filename[64];
  snprintf(filename, sizeof filename, "/proc/%d/status", getpid());
  std::ifstream ifs(filename);
  std::string line;
  int count = 0;
  while (count < VmMax && std::getline(ifs, line)) {
    auto it(res.begin());
    if (line.find("VmPeak") == 0) {
      ++count;
      std::advance(it, VmPeak);
    } else if (line.find("VmSize") == 0) {
      ++count;
      std::advance(it, VmSize);
    } else if (line.find("VmLck") == 0) {
      ++count;
      std::advance(it, VmLck);
    } else if (line.find("VmHWM") == 0) {
      ++count;
      std::advance(it, VmHWM);
    } else if (line.find("VmRSS") == 0) {
      ++count;
      std::advance(it, VmRSS);
    } else if (line.find("VmSwap") == 0) {
      ++count;
      std::advance(it, VmSwap);
    } else {
      continue;
    }

    // skip until number;
    std::size_t offset = 0;
    while (offset < line.size() && !isdigit(line[offset])) {
      ++offset;
    }

    if (offset < line.size()) {
      char* end = nullptr;
      long val = strtol(&line[offset], &end, 0);
      val *= 1024;  // since suffix is KB
      *it = static_cast<size_t>(val);
    }
  }

  return res;
}

size_t getMemoryInfo(MemoryInfoType type) {
  char filename[64];
  snprintf(filename, sizeof filename, "/proc/%d/status", getpid());
  std::ifstream ifs(filename);
  std::string line;
  bool found = false;
  while (!found && std::getline(ifs, line)) {
    switch (type) {
      case VmPeak:
        if (line.find("VmPeak") == 0)  {
          found = true;
        }
        break;

      case VmSize:
        if (line.find("VmSize") == 0)  {
          found = true;
        }
        break;

      case VmLck:
        if (line.find("VmLck") == 0)  {
          found = true;
        }
        break;

      case VmHWM:
        if (line.find("VmHWM") == 0) {
          found = true;
        }
        break;

      case VmRSS:
        if (line.find("VmRSS") == 0)  {
          found = true;
        }
        break;

      case VmSwap:
        if (line.find("VmSwap") == 0)  {
          found = true;
        }
        break;

      default:
        return 0;
    }
  }

  // skip until number;
  std::size_t offset = 0;
  while (offset < line.size() && !isdigit(line[offset])) {
    ++offset;
  }

  size_t res = 0;
  if (offset < line.size()) {
    char* end = nullptr;
    long val = strtol(&line[offset], &end, 0);
    val *= 1024;  // since suffix is KB
    res = static_cast<size_t>(val);
  }

  return res;
}

#elif defined(__APPLE__)
std::vector<size_t> getMemoryInfo() {
  /* only support
   mach_vm_size_t  virtual_size;   virtual memory size (bytes)
   mach_vm_size_t  resident_size;      resident memory size (bytes)
   mach_vm_size_t  resident_size_max;  maximum resident memory size (bytes) */
  std::vector<size_t> res(VmMax);
  task_t task = MACH_PORT_NULL;
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS) {
    return res;
  }

  task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

  res[VmSize] = t_info.virtual_size;
  res[VmRSS] = t_info.resident_size;

  return res;
}

size_t getMemoryInfo(MemoryInfoType type) {
  if (type != VmSize && type != VmRSS) {
    return 0;
  }

  task_t task = MACH_PORT_NULL;
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS) {
    return 0;
  }

  task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

  if (type == VmSize) {
    return t_info.virtual_size;
  } else if (type == VmRSS) {
    return t_info.resident_size;
  }

  return 0;
}

#endif

}  // namespace pikiwidb
