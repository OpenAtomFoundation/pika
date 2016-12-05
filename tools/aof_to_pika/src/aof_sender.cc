#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sstream> 
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>

#include "aof_sender.h"
#include "aof_info.h"

bool AOFSender::rconnect(const std::string &host, const std::string &port, const std::string &auth){
  if (host.empty() || port.empty()) return false;
  int s = -1, ret;
  struct addrinfo hints, *servinfo, *p;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  if ((ret = getaddrinfo(host.c_str(), port.c_str(), &hints, &servinfo)) != 0) {
    LOG_ERR("Failed to getaddinfo");
    return false;
  }

  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
      continue;
    //connect
    if (connect(s, p->ai_addr, p->ai_addrlen) < 0) {
      close(s);
      break;
    }
    //set non block
    if (!set_nonblock_(s)) {
      LOG_ERR("Failed to set non block");
      close(s);
      break;
    }
    sockfd_ = s;
    if (!auth.empty()){
      std::stringstream auth_info;
      auth_info << "*2\r\n$4\r\nauth\r\n$" << auth.size() << "\r\n" << auth << "\r\n";
      to_send_.assign(auth_info.str());
    }
    conn_info_ = new ConnInfo(host, port, auth);
    return true;
  }

  LOG_ERR("Failed to connect to server");
  sockfd_ = -1;
  return false;
}

AOFSender::~AOFSender(){
  buf_mutex_.Lock();
  buf_rcond_.SignalAll();
  buf_wcond_.SignalAll();
  buf_mutex_.Unlock();
  if (conn_info_ != NULL) delete conn_info_;
  close(sockfd_);
}

bool AOFSender::message_add(const std::string &message){
  if (message.empty()) return false;

  buf_mutex_.Lock();
  while (read_buffer_.size() >= READ_BUF_MAX){
    LOG_DEBUG("Reader waiting for write buffer.");
    buf_wcond_.Wait();
  }
  if (read_buffer_.size() < READ_BUF_MAX) {
    read_buffer_.push_back(message);
    buf_rcond_.SignalAll();
  }
  buf_mutex_.Unlock();

  return true;
}

bool AOFSender::message_get_(){
  buf_mutex_.Lock();
  while (read_buffer_.size() == 0)
  {
    LOG_DEBUG("Sender waiting for read buffer.");
    buf_rcond_.Wait();   // awake when the the wait send queue not empty
  }
  if (read_buffer_.size() > 0)
  {
    to_send_ = read_buffer_.front();
    read_buffer_.pop_front();
    buf_wcond_.SignalAll();
  }
  buf_mutex_.Unlock();

  return true;
}


bool AOFSender::process(){
  if (sockfd_ == -1){
    LOG_ERR("Invalid socket fd!");
    return false;
  }
  char ibuf[1024 * 16];
  memset(&ibuf, 0, sizeof(ibuf));
  long nsucc = 0, nfail = 0, inter = 0;;
  int empty_loop = 0;

  while (true) {
    int mask =  RM_READBLE;
    if (last_sent_.empty() || empty_loop > 3) {
      mask |= RM_WRITABLE;
    }
    mask = mask_wait_(sockfd_, mask, 1000);

    if (mask & RM_RECONN) {
      close(sockfd_);
      if (false == rconnect(conn_info_->host_, conn_info_->port_, conn_info_->auth_)) {
        LOG_ERR("Failed to reconnect remote server! host: " + conn_info_->host_ + " port : " + conn_info_->port_ + 
            " try again 1 second later!");
        usleep(1000000);
      } else {
        empty_loop += 4; // no need to wait read when reconnect
      }
    } else if (mask & RM_READBLE) {
      empty_loop = 0;
      // Read from socket
      ssize_t count;
      std::string reply;
      do {
        count = read(sockfd_, ibuf, sizeof(ibuf));
        if (count == -1 && errno != EAGAIN && errno != EINTR) {
          LOG_ERR("Error reading from the server.");
          return -1;
        }
        if (count > 0) {
          std::string tmp(ibuf);
          reply += tmp;
          memset(&ibuf, 0, sizeof(ibuf));
        }
      } while (count > 0);

      // Success read data
      if (!reply.empty()){ 
        std::stringstream ss;
        if (check_succ_(reply, nsucc, nfail)) {
          last_sent_.clear();
          ss << "Process OK!" << " SUCC : " << nsucc << ", FAILED: " << nfail;
          if ((nsucc - inter) > 10000) {
            LOG_INFO(ss.str()); inter = nsucc;
          } else { LOG_TRACE(ss.str()); }
        } else {
          ss << "Process Failed for :[" << last_sent_ << "] with reply : [" << reply + "]";
          LOG_ERR(ss.str());
          ss.str(std::string());
          ss << "SUCC : " << nsucc << ", FAILED: " << nfail;
          LOG_INFO(ss.str());
          //return -1;
        }
      }
    } else if (mask & RM_WRITABLE) {  
      empty_loop = 0;
      //Read from queue
      if (to_send_.empty()) {   
        message_get_();
      }

      // Send to socket
      if (!to_send_.empty()) {
        ssize_t total_nwritten = 0;
        do {
          ssize_t nwritten = write(sockfd_, to_send_.c_str() + total_nwritten, to_send_.size() - total_nwritten);

          if (nwritten == -1) {
            if (errno == EAGAIN) break;
            if (errno == EINTR) continue;
            LOG_ERR("Error writing to the server");
            return -1;
          }
          total_nwritten += nwritten;

        } while((unsigned)total_nwritten < to_send_.size());

        last_sent_.assign(to_send_.substr(0, total_nwritten));
        to_send_.assign(to_send_.substr(total_nwritten));
      }
    } else{	
      empty_loop++;
    }

  }
  return 0;
}

int AOFSender::mask_wait_(int fd, int mask, long long milliseconds) {
  struct pollfd pfd;
  int retmask = RM_NONE;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & RM_READBLE) pfd.events |= POLLIN;
  if (mask & RM_WRITABLE) pfd.events |= POLLOUT;
  pfd.events |= POLLERR;
  pfd.events |= POLLHUP;


  switch (poll(&pfd, 1, milliseconds)) {
    case 1:
      if (pfd.revents & POLLIN) retmask |= RM_READBLE;
      if (pfd.revents & POLLOUT) retmask |= RM_WRITABLE;
      if (pfd.revents & POLLERR || pfd.revents & POLLHUP){
        retmask |= RM_RECONN;
        close(fd);
      }
      if (pfd.revents & POLLNVAL) retmask |= RM_RECONN;
      break;
    case -1:
      retmask |= RM_RECONN;
      break;
  }

  return retmask;
}

bool AOFSender::set_nonblock_(int fd) {
  int flags;

  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    LOG_ERR("fcntl(F_GETFL) Error");
    return false;
  }

  flags |= O_NONBLOCK;

  if (fcntl(fd, F_SETFL, flags) == -1) {
    LOG_ERR("fcntl(F_SETFL, O_NONBLOCK) Error");
    return false;
  }
  return true;
}

bool AOFSender::check_succ_(const std::string &reply, long &succ, long &fail){
  if (reply.empty()) return false;

  std::string line;
  std::stringstream ss(reply);
  int tmp_fail = fail;
  while(std::getline(ss, line)){
    if (line.empty() || line[0] == '\r') continue;
    if (line.find("ERR") != std::string::npos) fail++;
    else succ++;
  }
  //skip last empty line
  if (tmp_fail != fail) return false;

  return true;
}

void AOFSender::print_result() {
  std::stringstream ss;
  ss << "Process FINISH!" << " SUCC : " << nsucc_ << ", FAILED: " << nfail_;
  LOG_WARN(ss.str());
}
