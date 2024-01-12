// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef RSYNC_SERVER_H_
#define RSYNC_SERVER_H_

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/pb_conn.h"
#include "net/include/server_thread.h"
#include "net/include/thread_pool.h"
#include "net/src/holy_thread.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/env.h"
#include "pstd_hash.h"
#include "rsync_service.pb.h"

namespace rsync {
class RsyncServerConn;
struct RsyncServerTaskArg {
  std::shared_ptr<RsyncService::RsyncRequest> req;
  std::shared_ptr<RsyncServerConn> conn;
  RsyncServerTaskArg(std::shared_ptr<RsyncService::RsyncRequest> _req, std::shared_ptr<RsyncServerConn> _conn)
      : req(std::move(_req)), conn(std::move(_conn)) {}
};
class RsyncReader;
class RsyncServerThread;

class RsyncServer {
public:
  RsyncServer(const std::set<std::string>& ips, const int port);
  ~RsyncServer();
  void Schedule(net::TaskFunc func, void* arg);
  int Start();
  int Stop();
private:
  std::unique_ptr<net::ThreadPool> work_thread_;
  std::unique_ptr<RsyncServerThread> rsync_server_thread_;
};

class RsyncServerConn : public net::PbConn {
public:
  RsyncServerConn(int connfd, const std::string& ip_port,
                  net::Thread* thread, void* worker_specific_data,
                  net::NetMultiplexer* mpx);
  virtual ~RsyncServerConn() override;
  int DealMessage() override;
  static void HandleMetaRsyncRequest(void* arg);
  static void HandleFileRsyncRequest(void* arg);
private:
  std::vector<std::shared_ptr<RsyncReader> > readers_;
  std::mutex mu_;
  void* data_ = nullptr;
};

class RsyncServerThread : public net::HolyThread {
public:
  RsyncServerThread(const std::set<std::string>& ips, int port, int cron_internal, RsyncServer* arg);
  ~RsyncServerThread();

private:
  class RsyncServerConnFactory : public net::ConnFactory {
  public:
      explicit RsyncServerConnFactory(RsyncServer* sched) : scheduler_(sched) {}

      std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port,
                                              net::Thread* thread, void* worker_specific_data,
                                              net::NetMultiplexer* net) const override {
        return std::static_pointer_cast<net::NetConn>(
        std::make_shared<RsyncServerConn>(connfd, ip_port, thread, scheduler_, net));
      }
  private:
    RsyncServer* scheduler_ = nullptr;
  };
  class RsyncServerHandle : public net::ServerHandle {
  public:
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
    bool AccessHandle(int fd, std::string& ip) const override;
    void CronHandle() const override;
  };
private:
  RsyncServerConnFactory conn_factory_;
  RsyncServerHandle handle_;
};

class RsyncReader {
public:
  RsyncReader() {
    block_data_ = new char[kBlockSize];
  }
  ~RsyncReader() {
    if (!filepath_.empty()) {
      Reset();
    }
    delete []block_data_;
  }
  pstd::Status Read(const std::string filepath, const size_t offset,
                    const size_t count, char* data, size_t* bytes_read,
                    std::string* checksum, bool* is_eof) {
    std::lock_guard<std::mutex> guard(mu_);
    pstd::Status s = readAhead(filepath, offset);
    if (!s.ok()) {
      return s;
    }
    size_t offset_in_block = offset % kBlockSize;
    size_t copy_count = count > (end_offset_ - offset) ? end_offset_ - offset : count;
    memcpy(data, block_data_ + offset_in_block, copy_count);
    *bytes_read = copy_count;
    *is_eof = (offset + copy_count == total_size_);
    return pstd::Status::OK();
  }
private:
  pstd::Status readAhead(const std::string filepath, const size_t offset) {
    if (filepath == filepath_ && offset >= start_offset_ && offset < end_offset_) {
      return pstd::Status::OK();
    }
    if (filepath != filepath_) {
      Reset();
      fd_ = open(filepath.c_str(), O_RDONLY);
      if (fd_ < 0) {
        LOG(ERROR) << "open file [" << filepath <<  "] failed! error: " << strerror(errno);
        return pstd::Status::IOError("open file [" + filepath +  "] failed! error: " + strerror(errno));
      }
      filepath_ = filepath;
      struct stat buf;
      stat(filepath.c_str(), &buf);
      total_size_ = buf.st_size;
    }
    start_offset_ = (offset / kBlockSize) * kBlockSize;

    size_t read_offset = start_offset_;
    size_t read_count = kBlockSize > (total_size_ - read_offset) ? (total_size_ - read_offset) : kBlockSize;
    ssize_t bytesin = 0;
    char* ptr = block_data_;
    while ((bytesin = pread(fd_, ptr, read_count, read_offset)) > 0) {
      read_count -= bytesin;
      read_offset += bytesin;
      ptr += bytesin;
      if (read_count <= 0) {
        break;
      }
    }
    if (bytesin < 0) {
      LOG(ERROR) << "unable to read from " << filepath << ". error: " << strerror(errno);
      Reset();
      return pstd::Status::IOError("unable to read from " + filepath + ". error: " + strerror(errno));
    }
    end_offset_ = start_offset_ + (ptr - block_data_);
    return pstd::Status::OK();
  }
  void Reset() {
    total_size_ = -1;
    start_offset_ = 0xFFFFFFFF;
    end_offset_ = 0xFFFFFFFF;
    memset(block_data_, 0, kBlockSize);
    md5_.reset(new pstd::MD5());
    filepath_ = "";
    close(fd_);
    fd_ = -1;
  }

private:
  std::mutex mu_;
  const size_t kBlockSize = 16 << 20;

  char* block_data_;
  size_t start_offset_ = -1;
  size_t end_offset_ = -1;
  size_t total_size_ = -1;

  int fd_ = -1;
  std::string filepath_;
  std::unique_ptr<pstd::MD5> md5_;
};

} //end namespace rsync
#endif

