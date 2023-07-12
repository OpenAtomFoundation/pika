#include "include/rsync_server.h"
#include <glog/logging.h>

namespace rsync {

RsyncServer::RsyncServer(const std::string& ip, const int port, void* worker_specific_data,
                         const std::string& dir) : dir_(dir), ip_(ip), port_(port) {
    work_thread_ = std::make_unique<net::ThreadPool>(2, 100000);
    //TODO parse parameter
    std::set<std::string> ips = {ip_};
    rsync_server_thread_ = std::make_unique<RsyncServerThread>(ips, port, 60 * 1000, this);
}

RsyncServer::~RsyncServer() {
    LOG(INFO) << "Rsync server destroyed";
}

void RsyncServer::Schedule(net::TaskFunc func, void* arg) {
    work_thread_->Schedule(func, arg);
}

int RsyncServer::Start() {
    int res = rsync_server_thread_->StartThread();
    LOG(WARNING) << "after RsyncServer::Start";
    if (res != net::kSuccess) {
        LOG(FATAL) << "Start rsync Server Thread Error: " << res;
    }
    res = work_thread_->start_thread_pool();
    if (res != net::kSuccess) {
      LOG(FATAL) << "Start ThreadPool Error: " << res
                 << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
    }
    LOG(WARNING) << "after start_thread_pool";
  return res;
}

int RsyncServer::Stop() {
    work_thread_->stop_thread_pool();
    rsync_server_thread_->StopThread();
    return 0;
}

/*-------------------------RsyncServerConn--------------------*/
RsyncServerConn::RsyncServerConn(int connfd, const std::string& ip_port,
                    Thread* thread, void* worker_specific_data,
                    NetMultiplexer* mpx) : PbConn(connfd, ip_port, thread, mpx), data_(worker_specific_data) {}

RsyncServerConn::~RsyncServerConn() {
    LOG(INFO) << "RsyncServerConn destroyed";
}

int RsyncServerConn::DealMessage() {
    std::shared_ptr<RsyncService::RsyncRequest> req = std::make_shared<RsyncService::RsyncRequest>();
    bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
    LOG(WARNING) << "RsyncServer DealMessage...";
    if (!parse_res) {
        LOG(WARNING) << "Pika rsync server connection pb parse error.";
        return -1;
    }
    switch (req->type()) {
        case RsyncService::kRsyncMeta: {
            auto task_arg =
                new RsyncServerTaskArg(req, std::dynamic_pointer_cast<RsyncServerConn>(shared_from_this()));
            ((RsyncServer*)(data_))->Schedule(&RsyncServerConn::HandleMetaRsyncRequest, task_arg);
            break;
        }
        case RsyncService::kRsyncFile: {
            auto task_arg =
                new RsyncServerTaskArg(req, std::dynamic_pointer_cast<RsyncServerConn>(shared_from_this()));
            ((RsyncServer*)(data_))->Schedule(&RsyncServerConn::HandleFileRsyncRequest, task_arg);
            break;
        }
        default: {
            LOG(WARNING) << "Invalid RsyncRequest type";
        }
    }
    return 0;
}

void RsyncServerConn::HandleMetaRsyncRequest(void* arg) {
    std::unique_ptr<RsyncServerTaskArg> task_arg(static_cast<RsyncServerTaskArg*>(arg));
    const std::shared_ptr<RsyncService::RsyncRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  RsyncService::RsyncResponse response;
  response.set_type(RsyncService::kRsyncMeta);
  LOG(INFO) << "Receive RsyncMeta request";

  //TODO: temporarily mock response
  RsyncService::MetaResponse* meta_resp = response.mutable_meta_resp();
  meta_resp->set_uuid("uuid");

  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Process MetaRsync request serialization failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

void RsyncServerConn::HandleFileRsyncRequest(void* arg) {
    std::unique_ptr<RsyncServerTaskArg> task_arg(static_cast<RsyncServerTaskArg*>(arg));
    const std::shared_ptr<RsyncService::RsyncRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  RsyncService::RsyncResponse response;
  response.set_type(RsyncService::kRsyncFile);
  LOG(INFO) << "Receive RsyncFile request " << "filename: " << req->file_req().filename()
            << " offset: " << req->file_req().offset()
            << " count: " << req->file_req().count();

  //TODO: temporarily mock response
  RsyncService::FileResponse* file_resp = response.mutable_file_resp();
  file_resp->set_eof(1);
  file_resp->set_count(1024);
  file_resp->set_offset(1024);
  file_resp->set_data("mocking data");
  file_resp->set_checksum("checksum");
  file_resp->set_filename("filename");

  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Process FileRsync request serialization failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

RsyncServerThread::RsyncServerThread(const std::set<std::string>& ips, int port, int cron_interval, RsyncServer* arg)
    : HolyThread(ips, port, &conn_factory_, cron_interval, &handle_, true), conn_factory_(arg) {}

RsyncServerThread::~RsyncServerThread() {
    LOG(WARNING) << "RsyncServerThread destroyed";
}

void RsyncServerThread::RsyncServerHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
    LOG(WARNING) << "ip_port: " << ip_port << " connection closed";
}

void RsyncServerThread::RsyncServerHandle::FdTimeoutHandle(int fd, const std::string& ip_port) const {
    LOG(WARNING) << "ip_port: " << ip_port << " connection timeout";
}

bool RsyncServerThread::RsyncServerHandle::AccessHandle(int fd, std::string& ip_port) const {
    LOG(WARNING) << "fd: "<< fd << " ip_port: " << ip_port << " connection accepted";
    return true;
}

void RsyncServerThread::RsyncServerHandle::CronHandle() const {
    LOG(WARNING) << "CronHandle called";
}

} // end namespace rsync