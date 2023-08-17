#pragma once

#include <arpa/inet.h>
#include <sys/types.h>

#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <type_traits>

namespace pikiwidb {

template <typename T>
inline T ToValue(const void* ptr) {
  static_assert(sizeof(T) == sizeof(void*), "must equal to pointer size");

  using unionType = typename std::aligned_union<sizeof(T), T, void*>::type;
  unionType alignedStorage;
  T* pt = new (&alignedStorage) T();
  new (&alignedStorage) const void*(ptr);

  return *pt;
}

template <typename T>
inline void* ToPointer(T v) {
  static_assert(sizeof v == sizeof(void*), "must equal to pointer size");

  using unionType = typename std::aligned_union<sizeof(T), T, void*>::type;
  unionType alignedStorage;

  void** ptr = new (&alignedStorage) void*();
  new (&alignedStorage) T(v);

  return *ptr;
}

class ThreadGuard {
 public:
  ThreadGuard() = default;

  template <typename F, typename... Args>
  explicit ThreadGuard(F&& f, Args&&... args) : thread_(std::forward<F>(f), std::forward<Args>(args)...) {}

  // no copyable
  ThreadGuard(const ThreadGuard&) = delete;
  ThreadGuard& operator=(const ThreadGuard&) = delete;

  // movable
  ThreadGuard(ThreadGuard&& t) = default;
  ThreadGuard& operator=(ThreadGuard&& t) = default;

  explicit ThreadGuard(std::thread&& t) : thread_(std::move(t)) {}
  ThreadGuard& operator=(std::thread&& t) {
    if (&t != &thread_) thread_ = std::move(t);

    return *this;
  }

  void join() {
    if (thread_.joinable()) thread_.join();
  }

  ~ThreadGuard() {
    if (thread_.joinable()) thread_.join();
  }

 private:
  std::thread thread_;
};

inline std::string GetSockaddrIp(const struct sockaddr_in* addr) {
  char tmp[128];
  const char* ip = inet_ntop(AF_INET, &addr->sin_addr, tmp, (socklen_t)(sizeof tmp));
  if (!ip) {
    return std::string();
  }

  return std::string(ip);
}

inline std::string GetSockaddrIp(const struct sockaddr* sa) {
  auto addr = reinterpret_cast<const sockaddr_in*>(sa);
  return GetSockaddrIp(addr);
}

inline int GetSockaddrPort(const struct sockaddr_in* addr) {
  int port = static_cast<int>(ntohs(addr->sin_port));
  if (port > 0 && port <= 65535) {
    return port;
  }

  return -1;
}

inline int GetSockaddrPort(const struct sockaddr* sa) {
  auto addr = reinterpret_cast<const sockaddr_in*>(sa);
  return GetSockaddrPort(addr);
}

inline std::string AddrToString(const struct sockaddr_in* addr) {
  auto ip = GetSockaddrIp(addr);
  int port = GetSockaddrPort(addr);
  return ip + ":" + std::to_string(port);
}

inline std::string AddrToString(const struct sockaddr* addr) {
  auto ip = GetSockaddrIp(addr);
  int port = GetSockaddrPort(addr);
  return ip + ":" + std::to_string(port);
}

inline struct sockaddr_in MakeSockaddr(const char* ip, int port) {
  sockaddr_in addr;
  memset(&addr, 0, sizeof addr);
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ::inet_addr(ip);
  addr.sin_port = htons(port);

  return addr;
}

inline int64_t HashSockaddr(const struct sockaddr_in* addr) {
  int64_t h = addr->sin_addr.s_addr;

  h <<= 16;
  h |= addr->sin_port;

  return h;
}

inline std::string& Trim(std::string& s) {
  if (s.empty()) {
    return s;
  }

  s.erase(0, s.find_first_not_of(" "));
  s.erase(s.find_last_not_of(" ") + 1);
  return s;
}

inline uint32_t RandomUniformUInt32() {
  std::random_device eng;
  std::mt19937 dist(eng());

  return dist();
}

inline uint64_t RandomUniformUInt64() {
  std::random_device eng;
  std::mt19937_64 dist(eng());

  return dist();
}

template <typename T = int>
inline T RandomBetween(T begin, T end) {
  if (begin >= end) {
    return begin;
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<T> dist(begin, end);

  return dist(gen);
}

inline bool Probability(int percent) {
  if (percent <= 0) {
    return false;
  } else if (percent >= 100) {
    return true;
  }

  return RandomBetween(1, 100 * 100) <= percent * 100;
}

struct SocketAddr
{
    static const uint16_t kInvalidPort = -1;

    SocketAddr()
    {
        memset(&addr_, 0, sizeof addr_);
    }
    
    SocketAddr(const sockaddr_in& addr)
    {
        Init(addr);
    }

    SocketAddr(uint32_t netip, uint16_t netport)
    {
        Init(netip, netport);
    }

    SocketAddr(const char* ip, uint16_t hostport)
    {
        Init(ip, hostport);
    }

    void Init(const sockaddr_in& addr)
    {
        memcpy(&addr_, &addr, sizeof(addr));
    }

    void Init(uint32_t netip, uint16_t netport)
    {
        addr_.sin_family = AF_INET;       
        addr_.sin_addr.s_addr = netip;       
        addr_.sin_port   = netport;
    }

    void Init(const char* ip, uint16_t hostport)
    {
        addr_.sin_family = AF_INET;
        addr_.sin_addr.s_addr = ::inet_addr(ip);
        addr_.sin_port = htons(hostport);
    }

    const sockaddr_in& GetAddr() const
    {
        return addr_;
    }

    std::string GetIP() const
    {
        char tmp[32];
        const char* res = inet_ntop(AF_INET, &addr_.sin_addr,
                                    tmp, (socklen_t)(sizeof tmp));
        return std::string(res);
    }

    uint16_t GetPort() const
    {
        return ntohs(addr_.sin_port);
    }

    bool IsValid() const { return addr_.sin_family != 0; }

    void Clear()
    {
        memset(&addr_, 0, sizeof addr_);
    }

    inline friend bool operator== (const SocketAddr& a, const SocketAddr& b)
    {
        return a.addr_.sin_family      ==  b.addr_.sin_family &&
               a.addr_.sin_addr.s_addr ==  b.addr_.sin_addr.s_addr &&
               a.addr_.sin_port        ==  b.addr_.sin_port ;
    }

    inline friend bool operator!= (const SocketAddr& a, const SocketAddr& b)
    {
        return !(a == b);
    }

private:
    sockaddr_in  addr_;
};



}  // namespace pikiwidb
