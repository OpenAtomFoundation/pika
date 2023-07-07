#ifndef RSYNC_SERVER_H_
#define RSYNC_SERVER_H_
#include <stdio.h>
#include <unistd.h>
#include <atomic>
#include <memory>

#include "rsync_service.pb.h"
#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/thread_pool.h"
#include "net/include/pb_conn.h"
#include "net/include/server_thread.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/env.h"
#include "net/src/holy_thread.h"

using namespace net;
using namespace RsyncService;
using namespace pstd;

namespace Rsync {

struct RsyncServerTaskArg {
  std::shared_ptr<RsyncService::RsyncRequest> req;
  std::shared_ptr<net::PbConn> conn;
  RsyncServerTaskArg(std::shared_ptr<RsyncService::RsyncRequest> _req, std::shared_ptr<net::PbConn> _conn)
      : req(std::move(_req)), conn(std::move(_conn)) {}
};
class RsyncReader;
class RsyncServerThread;

class RsyncServer {
public:
    RsyncServer(const std::string& ip_port, void* worker_specific_data,
                const std::string& dir);
    ~RsyncServer();
private:
    std::string dir_;
    std::map<std::string, std::shared_ptr<RsyncReader> > file_map_;
    std::unique_ptr<ThreadPool> work_thread_ = nullptr;
    std::unique_ptr<RsyncServerThread> rsync_server_thread_ = nullptr;
};

class RsyncServerConn : public PbConn {
public:
    RsyncServerConn(int connfd, const std::string& ip_port,
                    Thread* thread, void* worker_specific_data,
                    NetMultiplexer* mpx);
    virtual ~RsyncServerConn() override;
    int DealMessage() override;
    //处理slave发来的meta请求，arg参数类型为RsyncServerTaskArg，
    //请求处理完成之后将序列化好的response通过conn->WriteResp进行发送
    void HandleMetaRsyncRequest(void* arg);
    //处理slave发来的file请求，arg参数类型为RsyncServerTaskArg
    //请求处理完成之后将序列化好的response通过conn->WriteResp进行发送
    void HandleFileRsyncRequest(void* arg);
};

class RsyncServerThread : public HolyThread {
public:
  RsyncServerThread(const std::set<std::string>& ips, int port, int cron_internal);
  ~RsyncServerThread();

private:
  class RsyncServerConnFactory : public ConnFactory {
  public:
      explicit RsyncServerConnFactory(RsyncServerThread* thread);

      std::shared_ptr<NetConn> NewNetConn(int connfd, const std::string& ip_port,
                                          Thread* thread, void* worker_specific_data,
                                          NetMultiplexer* net) const override;
  };
  class RsyncServerHandle : public ServerHandle {
  public:
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    void FdTimeoutHandle(int fd, const std::string& ip_port);
    bool AccessHandle(int fd, std::string& ip);
    bool AccessHandle(std::string& ip);
    void CronHandle();
  };
private:
  RsyncServerConnFactory conn_factory_;
  RsyncServerHandle handle_;
};

class RsyncServerConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_specific_data,
                                              NetMultiplexer* net_epoll) const override;
};

class RSyncReader {
public:
    RSyncReader(const std::string& filepath);
    ~RSyncReader();
    Status Read(uint64_t offset, size_t n, Slice* result);

private:
    std::string filepath_;
    std::unique_ptr<RandomRWFile> file_;
};
} //end namespace rsync

#endif