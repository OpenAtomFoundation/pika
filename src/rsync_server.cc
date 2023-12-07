// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <filesystem>

#include <glog/logging.h>
#include <google/protobuf/map.h>

#include "pstd_hash.h"
#include "include/pika_server.h"
#include "include/rsync_server.h"
#include "pstd/include/pstd_defer.h"

extern PikaServer* g_pika_server;
namespace rsync {

using namespace net;
using namespace pstd;
using namespace RsyncService;

void RsyncWriteResp(RsyncService::RsyncResponse& response, std::shared_ptr<net::PbConn> conn) {
  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Process FileRsync request serialization failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

RsyncServer::RsyncServer(const std::set<std::string>& ips, const int port) {
  work_thread_ = std::make_unique<net::ThreadPool>(2, 100000);
  rsync_server_thread_ = std::make_unique<RsyncServerThread>(ips, port, 1 * 1000, this);
}

RsyncServer::~RsyncServer() {
  //TODO: handle destory
  LOG(INFO) << "Rsync server destroyed";
}

void RsyncServer::Schedule(net::TaskFunc func, void* arg) {
  work_thread_->Schedule(func, arg);
}

int RsyncServer::Start() {
  LOG(INFO) << "start RsyncServer ...";
  int res = rsync_server_thread_->StartThread();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start rsync Server Thread Error. ret_code: " << res << " message: "
               << (res == net::kBindError ? ": bind port conflict" : ": other error");
  }
  res = work_thread_->start_thread_pool();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start rsync Server ThreadPool Error, ret_code: " << res << " message: "
               << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  LOG(INFO) << "RsyncServer started ...";
  return res;
}

int RsyncServer::Stop() {
  LOG(INFO) << "stop RsyncServer ...";
  work_thread_->stop_thread_pool();
  rsync_server_thread_->StopThread();
  return 0;
}

RsyncServerConn::RsyncServerConn(int connfd, const std::string& ip_port, Thread* thread,
                                 void* worker_specific_data, NetMultiplexer* mpx)
    : PbConn(connfd, ip_port, thread, mpx), data_(worker_specific_data) {
  readers_.resize(kMaxRsyncParallelNum);
  for (int i = 0; i < kMaxRsyncParallelNum; i++) {
    readers_[i].reset(new RsyncReader());
  }
}

RsyncServerConn::~RsyncServerConn() {
  std::lock_guard<std::mutex> guard(mu_);
  for (int i = 0; i < readers_.size(); i++) {
    readers_[i].reset();
  }
}

int RsyncServerConn::DealMessage() {
  std::shared_ptr<RsyncService::RsyncRequest> req = std::make_shared<RsyncService::RsyncRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
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
  std::string db_name = req->db_name();
  uint32_t slot_id = req->slot_id();
  std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, slot_id);
  if (!slot || slot->IsBgSaving()) {
    LOG(WARNING) << "waiting bgsave done...";
    return;
  }

  RsyncService::RsyncResponse response;
  response.set_reader_index(req->reader_index());
  response.set_code(RsyncService::kOk);
  response.set_type(RsyncService::kRsyncMeta);
  response.set_db_name(db_name);
  response.set_slot_id(slot_id);

  std::vector<std::string> filenames;
  std::string snapshot_uuid;
  g_pika_server->GetDumpMeta(db_name, slot_id, &filenames, &snapshot_uuid);
  response.set_snapshot_uuid(snapshot_uuid);

  LOG(INFO) << "Rsync Meta request, snapshot_uuid: " << snapshot_uuid
            << " files count: " << filenames.size() << " file list: ";
  std::for_each(filenames.begin(), filenames.end(), [](auto& file) {
    LOG(INFO) << "rsync snapshot file: " << file;
  });

  RsyncService::MetaResponse* meta_resp = response.mutable_meta_resp();
  for (const auto& filename : filenames) {
        meta_resp->add_filenames(filename);
  }
  RsyncWriteResp(response, conn);
}

void RsyncServerConn::HandleFileRsyncRequest(void* arg) {
  std::unique_ptr<RsyncServerTaskArg> task_arg(static_cast<RsyncServerTaskArg*>(arg));
  const std::shared_ptr<RsyncService::RsyncRequest> req = task_arg->req;
  std::shared_ptr<RsyncServerConn> conn = task_arg->conn;

  uint32_t slot_id = req->slot_id();
  std::string db_name = req->db_name();
  std::string filename = req->file_req().filename();
  size_t offset = req->file_req().offset();
  size_t count = req->file_req().count();

  RsyncService::RsyncResponse response;
  response.set_reader_index(req->reader_index());
  response.set_code(RsyncService::kOk);
  response.set_type(RsyncService::kRsyncFile);
  response.set_db_name(db_name);
  response.set_slot_id(slot_id);

  std::string snapshot_uuid;
  Status s = g_pika_server->GetDumpUUID(db_name, slot_id, &snapshot_uuid);
  response.set_snapshot_uuid(snapshot_uuid);
  if (!s.ok()) {
    LOG(WARNING) << "rsyncserver get snapshotUUID failed";
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    return;
  }

  std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, slot_id);
  if (!slot) {
   LOG(WARNING) << "cannot find slot for db_name: " << db_name
                << " slot_id: " << slot_id;
   response.set_code(RsyncService::kErr);
   RsyncWriteResp(response, conn);
  }

  const std::string filepath = slot->bgsave_info().path + "/" + filename;
  char* buffer = new char[req->file_req().count() + 1];
  size_t bytes_read{0};
  std::string checksum = "";
  bool is_eof = false;
  std::shared_ptr<RsyncReader> reader = conn->readers_[req->reader_index()];
  s = reader->Read(filepath, offset, count, buffer,
                   &bytes_read, &checksum, &is_eof);
  if (!s.ok()) {
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    delete []buffer;
    return;
  }

  RsyncService::FileResponse* file_resp = response.mutable_file_resp();
  file_resp->set_data(buffer, bytes_read);
  file_resp->set_eof(is_eof);
  file_resp->set_checksum(checksum);
  file_resp->set_filename(filename);
  file_resp->set_count(bytes_read);
  file_resp->set_offset(offset);

  RsyncWriteResp(response, conn);
  delete []buffer;
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
}

} // end namespace rsync

