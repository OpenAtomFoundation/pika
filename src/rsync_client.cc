#include "include/rsync_client.h"
#include "pstd/include/pstd_defer.h"
#include "pstd/src/env.cc"
#include <stdio.h>
#include <stdlib.h>

using namespace net;
using namespace pstd;
using namespace RsyncService;

using namespace pstd;
namespace rsync {
RsyncClient::RsyncClient(const std::string& dir, const std::string& db_name, const uint32_t slot_id)
    : dir_ (dir), flush_period_(100), db_name_(db_name), slot_id_(slot_id), state_(IDLE),
      max_retries_(10) {
    client_thread_ = std::make_unique<RsyncClientThread>(10 * 1000, 60 * 1000, this);
}

bool RsyncClient::Init(const std::string& ip_port) {
    if (!ParseIpPortString(ip_port, ip_, port_)) {
        LOG(WARNING) << "Parse ip_port error " << ip_port;
        return false;
    }
    client_thread_->StartThread();
    bool ret = Recover();
    if (!ret) {
        client_thread_->StopThread();
        return false;
    }
    return true;
}

void* RsyncClient::ThreadMain() {
    int cnt = 0;
    int period = 0;
    Status s = Status::OK();
    while (state_.load(std::memory_order_relaxed) == RUNNING) {
        for (const auto& file : file_set_) {
            LOG(INFO) << "CopyRemoteFile: " << file;
            while (state_.load() == RUNNING) {
              s = CopyRemoteFile(file);
              if (!s.ok()) {
                  LOG(WARNING) << "rsync CopyRemoteFile failed, filename: " << file;
                  continue;
              }
              LOG(WARNING) << "CopyRemoteFile "<< file << "success...";
              break;
            }
            if (state_.load(std::memory_order_relaxed) != RUNNING) {
                break;
            }
            if (++period == flush_period_) {
                period = 0;
                FlushMetaTable();
            }
        }
        if (meta_table_.size() == file_set_.size()) {
            LOG(INFO) << "rsync success...";
            state_.store(STOP, std::memory_order_relaxed);
            break;
        }
    }
    return nullptr;
}

void RsyncClient::OnReceive(RsyncResponse* resp) {
    std::unique_lock<std::mutex> lock(mu_);
    resp_list_.push_back(resp);
    cond_.notify_all();
}

Status RsyncClient::Wait(WaitObject* wo) {
    Status s = Status::Timeout("rsync timeout", "timeout");
    std::list<RsyncResponse*> resp_list;
    {
        std::unique_lock<std::mutex> lock(mu_);
        cond_.wait_for(lock, std::chrono::seconds(3), [this]{
            return !resp_list_.empty();}
        );
        resp_list.swap(resp_list_);
    }

    auto iter = resp_list.begin();
    while (iter != resp_list.end()) {
        RsyncResponse* resp = *iter;
        if (resp->type() != wo->type_) {
            LOG(WARNING) << "mismatch request/response type, skip";
            iter++;
            continue;
        }
        if (resp->type() == kRsyncFile &&
            (resp->file_resp().filename() != wo->filename_ ||
            resp->file_resp().offset() != wo->offset_)) {
                LOG(WARNING) << "mismatch rsync response, skip";
                continue;
        }
        s = Status::OK();
        wo->resp_ = resp;
        resp_list.erase(iter);
        break;
    }

    iter = resp_list.begin();
    while (iter != resp_list.end()) {
        delete (*iter);
        iter++;
    }
    return s;
}

Status RsyncClient::CopyRemoteFile(const std::string& filename) {
    Status s;
    int retries = 0;
    size_t offset = 0;
    size_t copy_file_begin_time = pstd::NowMicros();
    size_t count = throttle_->ThrottledByThroughput(1024 * 1024);
    MD5 md5;
    std::unique_ptr<RsyncWriter> writer(new RsyncWriter(dir_ + "/" + filename));
    DEFER {
        if (writer) {
            writer->Close();
            writer.reset();
        }
        if (!s.ok()) {
            DeleteFile(filename);
        }
    };
    while (retries < max_retries_) {
        RsyncRequest request;
        request.set_type(kRsyncFile);
        request.set_db_name(db_name_);
        request.set_slot_id(slot_id_);
        FileRequest* file_req = request.mutable_file_req();
        file_req->set_filename(filename);
        file_req->set_offset(offset);
        file_req->set_count(count);
        std::string to_send;
        request.SerializeToString(&to_send);

        s = client_thread_->Write(ip_, port_, to_send);
        if (!s.ok()) {
            LOG(WARNING) << "send rsync request failed";
            continue;
        }

        WaitObject wo(filename, kRsyncFile, offset);
        LOG(INFO) << "wait CopyRemoteFile response.....";
        s = Wait(&wo);
        if (s.IsTimeout() || wo.resp_ == nullptr) {
            LOG(WARNING) << "rsync request timeout";
            retries++;
            continue;
        }
        RsyncResponse* resp = wo.resp_;

        LOG(INFO) << "receive fileresponse, snapshot_uuid: " << resp->snapshot_uuid()
                  << "filename: " << resp->file_resp().filename()
                  << "offset: " << resp->file_resp().offset()
                  << "count: " << resp->file_resp().count();

        if (resp->snapshot_uuid() != snapshot_uuid_) {
            LOG(WARNING) << "receive newer dump, reset state to STOP";
            state_.store(STOP);
            delete resp;
            return s;
        }

        size_t ret_count = resp->file_resp().count();
        resp->file_resp().data();
        s = writer->Write((uint64_t)offset, ret_count, resp->file_resp().data().c_str());
        if (!s.ok()) {
            LOG(WARNING) << "rsync client write file error";
            break;
        }

        md5.update(resp->file_resp().data().c_str(), ret_count);
        if (resp->file_resp().eof()) {
            if (md5.finalize().hexdigest() != resp->file_resp().checksum()) {
                LOG(WARNING) << "mismatch file checksum for file: " << filename;
                //TODO: 处理返回status
                s = Status::IOError("mismatch checksum", "mismatch checksum");
            }
            s = writer->Fsync();
            if (!s.ok()) {
                return s;
            }
            s = writer->Close();
            if (!s.ok()) {
                return s;
            }
            writer.reset();
            meta_table_.insert(std::make_pair(filename, resp->file_resp().checksum()));
            break;
        } else {
            offset += resp->file_resp().count();
        }
        size_t copy_file_end_time = pstd::NowMicros();
        size_t elaspe_time_us = copy_file_end_time - copy_file_begin_time;
        throttle_->ReturnUnusedThroughput(count, ret_count, elaspe_time_us);
        retries = 0;
    }

    return s;
}

Status RsyncClient::Start() {
    StartThread();
    return Status::OK();
}

Status RsyncClient::Stop() {
    state_ = STOP;
    StopThread();
    client_thread_->StopThread();
    JoinThread();
    client_thread_->JoinThread();
    state_ = IDLE;
    return Status::OK();
}

//TODO: shaoyi
Status RsyncClient::CopyRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set) {
    Status s;
    int retries = 0;
    RsyncRequest request;
    request.set_db_name(db_name_);
    request.set_slot_id(slot_id_);
    request.set_type(kRsyncMeta);
    std::string to_send;
    request.SerializeToString(&to_send);
    while (retries < max_retries_) {
        s = client_thread_->Write(ip_, port_, to_send);
        if (!s.ok()) {
            retries++;
        }
        WaitObject wo(kRsyncMeta);
        s = Wait(&wo);
        if (s.IsTimeout() || wo.resp_ == nullptr) {
            LOG(WARNING) << "rsync CopyRemoteMeta request timeout, retry times: " << retries;
            retries++;
            continue;
        }
        RsyncResponse* resp = wo.resp_;
        LOG(INFO) << "receive rsync meta infos, snapshot_uuid: " << resp->snapshot_uuid()
                  << "files count: " << resp->meta_resp().filenames_size();
        for (int i = 0; i < resp->meta_resp().filenames_size(); i++) {
            LOG(INFO) << "file: " << resp->meta_resp().filenames(i);
        }
        *snapshot_uuid = resp->snapshot_uuid();
        for (int i = 0; i < resp->meta_resp().filenames_size(); i++) {
            file_set->insert(resp->meta_resp().filenames(i));
        }
        break;
    }
    return s;
}
bool RsyncClient::Recover() {
    std::string snapshot_uuid;
    std::set<std::string> file_set;
    Status s = CopyRemoteMeta(&snapshot_uuid, &file_set);
    if (!s.ok()) {
        LOG(WARNING) << "copy remote meta failed";
        return false;
    }
    //TODO: yuecai 加载本地元信息文件，与master回包内容diff
    snapshot_uuid_ = snapshot_uuid;
    file_set_.insert(file_set.begin(), file_set.end());
    state_ = RUNNING;
    LOG(WARNING) << "copy remote meta done";
    return true;
}

Status RsyncClient::LoadMetaTable() {
    if (!FileExists(dir_)) {
        return Status::OK();
    }

    FILE* fp;
    char* line = nullptr;
    size_t len = 0;
    size_t read = 0;
    int32_t line_num = 0;

    std::atomic_int8_t retry_times = 5;

    while (retry_times -- > 0) {
        fp = fopen(dir_.c_str(), "r");
        if (fp == nullptr) {
            LOG(WARNING) << "open meta file failed, meta_path: " << dir_;
        } else {
            break;
        }
    }
    // if the file cannot be read from disk, use the remote file directly
    if (fp == nullptr) {
        LOG(WARNING) << "open meta file failed, meta_path: " << dir_ << ", retry times: " << retry_times;
        return Status::IOError("open meta file failed, dir: ", dir_);
    }

    while ((read = getline(&line, &len, fp)) != -1) {
        std::string str(line);
        std::string::size_type pos;
        while ((pos = str.find("\r")) != std::string::npos) {
            str.erase(pos, 1);
        }
        while ((pos = str.find("\n")) != std::string::npos) {
            str.erase(pos, 1);
        }

        if (str.empty()) {
            continue;
        }

        if (line_num == 0) {
            snapshot_uuid_ = str.erase(0, kUuidPrefix.size());
        } else {
            if ((pos = str.find(":")) != std::string::npos) {
               str.erase(pos, str.size() - pos);
            }
            file_set_.insert(str);
        }

        line_num++;
    }
    return Status::OK();
}

//TODO: shaoyi
Status RsyncClient::FlushMetaTable() {
    LOG(WARNING) << "FlushMetaTable called";
    return Status::OK();
}

} // end namespace rsync
