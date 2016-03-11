#include "pika_binlog_sender_thread.h"

#include <glog/logging.h>
#include <poll.h>

#include "pika_server.h"
#include "pika_define.h"
#include "pika_binlog_sender_thread.h"
#include "pika_master_conn.h"

using slash::Status;
using slash::Slice;

extern PikaServer* g_pika_server;

PikaBinlogSenderThread::PikaBinlogSenderThread(std::string &ip, int port, slash::SequentialFile *queue, uint32_t filenum, uint64_t con_offset) :
    con_offset_(con_offset),
    filenum_(filenum),
    initial_offset_(0),
    end_of_buffer_offset_(kBlockSize),
    queue_(queue),
    backing_store_(new char[kBlockSize]),
    buffer_(),
    ip_(ip),
    port_(port),
    should_exit_(false) {
      last_record_offset_ = con_offset % kBlockSize;
      pthread_rwlock_init(&rwlock_, NULL);
    }

PikaBinlogSenderThread::~PikaBinlogSenderThread() {
  delete [] backing_store_;
}

int PikaBinlogSenderThread::trim() {
  slash::Status s;
  uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((con_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = con_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      con_offset_ = start_block + res;
      break;
    }
    ret = get_next(is_error);
    if (is_error == true) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = con_offset_ % kBlockSize;

  return 0;
}

uint64_t PikaBinlogSenderThread::get_next(bool &is_error) {
  uint64_t offset = 0;
  slash::Status s;
  is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[3];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_);
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  return offset;
}

unsigned int PikaBinlogSenderThread::ReadPhysicalRecord(slash::Slice *result) {
  slash::Status s;
  if (end_of_buffer_offset_ - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
    con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[3];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  //std::cout<<"2 --> con_offset_: "<<con_offset_<<" last_record_offset_: "<<last_record_offset_<<std::endl;
  s = queue_->Read(length, &buffer_, backing_store_);
  *result = slash::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    con_offset_ += (kHeaderSize + length);
  }
  return type;
}

Status PikaBinlogSenderThread::Consume(std::string &scratch) {
  Status s;
  if (last_record_offset_ < initial_offset_) {
    return slash::Status::IOError("last_record_offset exceed");
  }

  slash::Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    switch (record_type) {
      case kFullType:
        scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch.assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch.append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::Corruption("Data Corruption");
      case kOldRecord:
        return Status::EndFile("Eof");
      default:
        return Status::Corruption("Unknow reason");
    }
    // TODO:do handler here
    if (s.ok()) {
      break;
    }
  }
  //DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch;
  return Status::OK();
}

bool PikaBinlogSenderThread::Init() {

  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd_ == -1) {
    LOG(WARNING) << "BinlogSender socket error: " << strerror(errno);
    return false;
  }

  int flags = fcntl(sockfd_, F_GETFL, 0);
  fcntl(sockfd_, F_SETFL, flags | O_NONBLOCK);

  int yes = 1;
  if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
    LOG(WARNING) << "BinlogSender setsockopt SO_REUSEADDR error: " << strerror(errno);
    return false;
  }
  if (setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
    LOG(WARNING) << "BinlogSender setsockopt SO_KEEPALIVE: error: " << strerror(errno);
    return false;
  }

  return true;
}

bool PikaBinlogSenderThread::Connect() {

  struct sockaddr_in s_addr;
  memset(&s_addr, 0, sizeof(s_addr));
  s_addr.sin_family = AF_INET;
  s_addr.sin_addr.s_addr = inet_addr(ip_.c_str());
  s_addr.sin_port = htons(port_);

  if (-1 == connect(sockfd_, (struct sockaddr*)(&s_addr), sizeof(s_addr))) {
    if (errno == EINPROGRESS) {
      struct pollfd   wfd[1];
      wfd[0].fd     = sockfd_;
      wfd[0].events = POLLOUT;

      int res;
      if ((res = poll(wfd, 1, 500)) == -1) {
        LOG(WARNING) << "BinlogSender Connect, poll error: " << strerror(errno);
        return false;
      } else if (res == 0) {
        LOG(WARNING) << "BinlogSender Connect, timeout";
        return false;
      }

      int err = 0;
      socklen_t errlen = sizeof(err);
      if (getsockopt(sockfd_, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
        LOG(WARNING) << "BinlogSender Connect, getsockopt error";
        return false;
      }
      if (err) {
        errno = err;
        LOG(WARNING) << "BinlogSender Connect, error: " << strerror(errno);
        return false;
      }
    }
  }
  return true;
}

bool PikaBinlogSenderThread::Send(const std::string &msg) {
  // length to small
  char wbuf[2097152]; // 2M
  int wbuf_len = msg.size();
  int wbuf_pos = 0;
  int nwritten = 0;
  memcpy(wbuf, msg.data(), msg.size()); 

  while (1) {
    while (wbuf_len > 0) {
      nwritten = write(sockfd_, wbuf + wbuf_pos, wbuf_len - wbuf_pos);
      if (nwritten < 0) {
        break;
      }
      wbuf_pos += nwritten;
      if (wbuf_pos == wbuf_len) {
        wbuf_len = 0;
      }
    }
    if (nwritten == -1) {
      if (errno == EAGAIN) {
        continue;
      } else {
        LOG(WARNING) << "BinlogSender Send, error: " << strerror(errno);
        return false;
      }
    }
    if (wbuf_len == 0) {
      return true;
    }	
  }
}

Status PikaBinlogSenderThread::Parse() {
  std::string scratch("");
  Status s;

  Version* version = g_pika_server->logger_->version_;
  while (!IsExit()) {
    if (filenum_ == version->pro_num() && con_offset_ == version->pro_offset()) {
//      DLOG(INFO) << "BinlogSender Parse no new msg";
      usleep(10000);
      continue;
    }

    scratch = "";

    s = Consume(scratch);

    //DLOG(INFO) << "BinlogSender Parse a msg return: " << s.ToString();
    if (s.IsEndFile()) {
      std::string confile = NewFileName(g_pika_server->logger_->filename, filenum_ + 1);

      // Roll to next File
      if (slash::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        delete queue_;
        queue_ = NULL;

        slash::NewSequentialFile(confile, &(queue_));

        filenum_++;
        con_offset_ = 0;
        initial_offset_ = 0;
        end_of_buffer_offset_ = kBlockSize;
        last_record_offset_ = con_offset_ % kBlockSize;
      } else {
        usleep(10000);
      }
    } else if (s.ok()) {
//      DLOG(INFO) << "BinlogSender Parse ok, filenum = " << filenum_ << ", con_offset = " << con_offset_;
      if (Send(scratch)) {
        return s;
      } else {
        return Status::Corruption("Send error");
      }
    } else if (s.IsCorruption()) {
      return s;
    }
  }

  if (IsExit()) {
    return Status::Corruption("should exit");
  }
  return s;
}

void* PikaBinlogSenderThread::ThreadMain() {

  Status s;

  // 1. Connect to slave 
  while (!IsExit()) {
    DLOG(INFO) << "BinlogSender start Connect";
    if (Init()) {
      if (Connect()) {
        DLOG(INFO) << "BinlogSender Connect slave(" << ip_ << ":" << port_ << ") ok";

        do {
          s = Parse();
        } while (s.ok());
        DLOG(INFO) << s.ToString();
        close(sockfd_);

      } else {
        close(sockfd_);
      }
    }
    sleep(1);
  }
  return NULL;

  //  pthread_exit(NULL);
}

