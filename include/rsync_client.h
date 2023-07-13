#ifndef RSYNC_CLIENT_H_
#define RSYNC_CLIENT_H_
#include <glog/logging.h>
#include <list>
#include <memory>
#include <thread>
#include <condition_variable>

#include "include/rsync_client_thread.h"
#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_hash.h"
#include "pstd/include/pstd_status.h"
#include "rsync_service.pb.h"
#include "throttle.h"

using namespace pstd;
using namespace net;
using namespace RsyncService;

const std::string kDumpMetaFileName = "DUMP_META_DATA";
const std::string kUuidPrefix = "snapshot-uuid:";

namespace rsync {

class RsyncWriter;
class Session;

class RsyncClient : public net::Thread {
public:
    enum State {
        IDLE,
        RUNNING,
        STOP,
    };
    enum Type {
        kMeta,
        kFile,
    };
    RsyncClient(const std::string& dir, const std::string& ip, const int port);

    void* ThreadMain() override;
    bool Init();
    Status Start();
    Status Stop();
    void OnReceive(RsyncResponse* resp);
private:
    void Recover();
    Status SendRequest(const std::string& filename, size_t offset, Type type);
    Status HandleResponse(const std::string& filename, size_t& offset, MD5& md5, RsyncWriter& writer);
    Status LoadFile(const std::string& filename);
    Status LoadMetaTable();
    Status FlushMetaTable();
    void HandleRsyncMetaResponse(RsyncResponse* response);

private:
    //已经下载完成的文件名与checksum值，用于宕机重启时恢复，
    //减少重复文件下载，周期性flush到磁盘上
    std::map<std::string, std::string> meta_table_;
    //待拉取的文件集合
    std::set<std::string> file_set_;
    std::string snapshot_uuid_;

    std::string dir_;
    std::string ip_;
    int port_;

    std::unique_ptr<RsyncClientThread> client_thread_;
    std::atomic<State> state_;

    std::list<RsyncService::RsyncResponse*> resp_list_;
    std::condition_variable cond_;
    std::mutex mu_;
};

//TODO: jinge
class RsyncWriter {
public:
    RsyncWriter(const std::string& filepath) {}
    ~RsyncWriter() {}
    Status Write(uint64_t offset, size_t n, Slice* result) {
         return Status::OK();
    }

private:
    std::string filepath_;
    std::unique_ptr<RandomRWFile> file_;
};

} // end namespace rsync

#endif