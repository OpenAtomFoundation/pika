#include "include/rsync_client.h"
#include "pstd/src/env.cc"
#include <stdio.h>
#include <stdlib.h>

using namespace net;
using namespace pstd;
using namespace RsyncService;

namespace rsync {
RsyncClient::RsyncClient(const std::string& dir, const std::string& ip, const int port)
    : dir_ (dir), ip_ (ip), port_(port), state_ (IDLE) {
    client_thread_ = std::make_unique<RsyncClientThread>(10 * 1000, 60 * 1000, this);
}

bool RsyncClient::Init() {
    if (state_.load(std::memory_order_relaxed) != IDLE) {
        Stop();
        Start();
    }
    Recover();
    state_ = RUNNING;
    return true;
}

void* RsyncClient::ThreadMain() {
    int period = 1000;
    Status s = Status::OK();
    while (state_.load(std::memory_order_relaxed) == RUNNING) {
        for (const auto& file : file_set_) {
            s = LoadFile(file);
            if (!s.ok()) {
                //TODO: 同步状态下，是保持最大重试次数还是死循环地搞？
                LOG(WARNING) << "rsync LoadFile failed";
                return nullptr;
            }
            if (state_.load(std::memory_order_relaxed) != RUNNING) {
                break;
            }
            if (--period == 0) {
                period = 10;
                FlushMetaTable();
            }
        }
        if (meta_table_.size() == file_set_.size()) {
            LOG(INFO) << "rsync done...";
            state_.store(STOP);
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

Status RsyncClient::SendRequest(const std::string& filename, size_t offset, Type type) {
    Status s = Status::OK();
    RsyncRequest request;
    switch (type) {
        case kMeta: {
            request.set_type(kRsyncMeta);
            break;
        }
        case kFile: {
            request.set_type(kRsyncFile);
            FileRequest* file_req = request.mutable_file_req();
            file_req->set_filename(filename);
            file_req->set_offset(offset);
            file_req->set_count(4096);
            break;
        }
        default:
            break;
    }
    std::string to_send;
    request.SerializeToString(&to_send);
    s = client_thread_->Write(ip_, port_, to_send);
    if (!s.ok()) {
        LOG(WARNING) << "send rsync request failed";
    }

    return s;
}

Status RsyncClient::HandleResponse(const std::string& filename, size_t& offset, MD5& md5, RsyncWriter& writer) {
    std::unique_lock<std::mutex> lock(mu_);
    cond_.wait_for(lock, std::chrono::seconds(3), [this]{return !resp_list_.empty();});

    Status s = Status::Timeout("rsync timeout", "timeout");
    auto iter = resp_list_.begin();
    while (iter != resp_list_.end()) {
        RsyncResponse* response = *iter;
        switch (response->type()) {
            case kRsyncMeta: {
                LOG(INFO) << "receive rsync meta infos, uuid: " << response->meta_resp().uuid()
                             << "files count: " << response->meta_resp().filenames_size();
                for (int i = 0; i < response->meta_resp().filenames_size(); i++) {
                    LOG(WARNING) << "filename: " << response->meta_resp().filenames(i);
                }
                break;
            }
            case kRsyncFile: {
                s = Status::OK();
                //比对filename和offset
                LOG(WARNING) << "receive rsync file infos: "
                             << "filename: " << response->file_resp().filename()
                             << "offset: " << response->file_resp().offset()
                             << "count: " << response->file_resp().count();
                md5.update(response->file_resp().data().c_str(), response->file_resp().count());
                break;
            }
            default:
               break;
        }
        //TODO: 比对正常，退出resp_list
        delete response;
        break;
    }
    return s;
}

Status RsyncClient::LoadFile(const std::string& filename) {
    Status s;
    int retries = 0;
    int max_retries_ = 10;
    size_t offset = 0;
    Type type = kFile;
    MD5 md5;
    RsyncWriter writer(dir_ + "/" + filename);
    while (retries < max_retries_) {
        s = SendRequest(filename, offset, kFile);
        if (!s.ok()) {
            retries++;
            continue;
        }
        s = HandleResponse(filename, offset, md5, writer);
        if (!s.ok()) {
            retries++;
            break;
        }
        retries = 0;
    }
    return s;
}

Status RsyncClient::Start() {
    StartThread();
    client_thread_->StartThread();
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

//TODO: yuecai
void RsyncClient::Recover() {
    file_set_.insert("filename");
  // 从远程读取 meta
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