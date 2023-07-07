#ifndef RSYNC_CLIENT_H_
#define RSYNC_CLIENT_H_
#include <glog/logging.h>

#include "throttle.h"
#include "pstd/include/env.h"
#include "net/include/net_cli.h"
#include "pstd/include/pstd_hash.h"
#include "net/include/bg_thread.h"
#include "rsync_service.pb.h"

using namespace pstd;
using namespace net;
using namespace RsyncService;

namespace rsync {

class RsyncWriter;
class Session;
class RsyncClient {
public:
    RsyncClient(const std::string& dir, int64_t reader_id, int ip_port);
    Status Start();
    void Recover();
    Status Copy();
private:
    Status LoadMetaTable();
    Status FlushMetaTable();

private:
    //一个RsyncClient包含多个后台线程，并行地从master拉取文件
    std::vector<std::unique_ptr<net::BGThread>> bg_workers_;

    //待拉取的文件集合
    std::set<std::string> file_set_;
    //已经下载完成的文件名与checksum值，用于宕机重启时恢复，减少重复文件下载，周期性flush到磁盘上
    std::map<std::string, std::string> meta_table_;

    int64_t remote_reader_id_;
    std::string ip_port_;
    std::string dir_;

    std::atomic<bool> is_running_;
};

class RsyncWriter {
public:
    RsyncWriter(const std::string& filepath);
    ~RsyncWriter();
    Status Write(uint64_t offset, size_t n, Slice* result);

private:
    std::string filepath_;
    std::unique_ptr<RandomRWFile> file_;
};

class Session {
public:
    Session(const std::string& filepath, std::shared_ptr<NetCli> conn,
            std::shared_ptr<Throttle> throttle);
    ~Session();
    Status Result(Slice* content) const;
    Status CopyFile();

private:
    Status HandleResponse();
    Status SendRequest();

private:
    bool stop_;
    Status s_;

    std::unique_ptr<RsyncWriter> writer_;
    std::string filepath_;
    int64_t reader_id_;
    size_t offset_;
    MD5 md5_;

    std::shared_ptr<Throttle> throttle_;
    std::shared_ptr<NetCli> conn_;
};

} // end namespace rsync

#endif