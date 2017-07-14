#include "sender_thread.h"

enum REDIS_STATUS {
  REDIS_ETIMEOUT = -2,
  REDIS_ERR = -1,
  REDIS_OK = 0,
  REDIS_HALF,
  REDIS_REPLY_STRING,
  REDIS_REPLY_ARRAY,
  REDIS_REPLY_INTEGER,
  REDIS_REPLY_NIL,
  REDIS_REPLY_STATUS,
  REDIS_REPLY_ERROR
};

SenderThread::SenderThread(pink::PinkCli *cli) :
    cli_(cli),
    buf_len_(0),
    buf_pos_(0),
    buf_r_cond_(&buf_mutex_),
    buf_w_cond_(&buf_mutex_)
{
  /*
  // Auth
  std::string authentication = "*2\r\n$4\r\nAUTH\r\n$11\r\nshq19950614\r\n";
  int fd = cli_->fd();
  int nwritten = write(fd, authentication.c_str(), authentication.size());
  if (nwritten == -1) {
    if (errno != EAGAIN && errno != EINTR) {
      // std::cout << "Error writting to the server :" << strerror(errno) << std::endl;
      log_err("Error writting to the server : %s", strerror(errno));
    } else {
      nwritten = 0;
    }
  }

  char auth_resp[128];
  int nreadlen = read(fd, auth_resp, 128);
  if (nreadlen != 0) {
    if (std::string(auth_resp) == "+OK\r\n") {
      log_info("Authentic success");
    } else {
      log_info("Invalid password");
    }
  }
  */
}

SenderThread::~SenderThread() {
  delete cli_;
}

int SenderThread::Wait(int fd, int mask, long long milliseconds) {
  struct pollfd pfd;
  int retmask = 0;
  int retval;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & kReadable) pfd.events |= POLLIN;
  if (mask & kWritable) pfd.events |= POLLOUT;

  if ((retval = poll(&pfd, 1, milliseconds)) == 1) {
    if (pfd.revents & POLLIN) retmask |= kReadable;
    if (pfd.revents & POLLOUT) retmask |= kWritable;
    if (pfd.revents & POLLERR) retmask |= kWritable;
    if (pfd.revents & POLLHUP) retmask |= kWritable;
    return retmask;
  } else {
    return kWritable;
  }
}

void SenderThread::LoadCmd(const std::string &cmd) {
  slash::MutexLock l(&buf_mutex_);
  while (buf_pos_ + buf_len_ + cmd.size() > kBufSize) {
    buf_w_cond_.Wait();
  }
  memcpy(buf_ + buf_pos_ + buf_len_ , cmd.data(), cmd.size());
  buf_len_ += cmd.size();
  buf_r_cond_.Signal();
}

int64_t SenderThread::NowMicros_() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void SenderThread::Stop() {
  slash::MutexLock l(&buf_mutex_);
  buf_r_cond_.Signal();
  should_exit_ = true;
}

void *SenderThread::ThreadMain() {
  int fd = cli_->fd();
  char magic[20];
  srand(time(NULL));
  int eof = 0;
  int done = 0;
  int flags;
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    log_err("fcntl(F_GETFL): %s", strerror(errno));
  }
  flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, flags) == -1) {
    log_err("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
  }

  while (!done) {
    int mask = kReadable;
    if (buf_len_ != 0) mask |= kWritable;
    mask = Wait(fd, mask, 1000);
    if (mask & kReadable) {
      if (rbuf_pos_ > 0) {
        if (rbuf_offset_ > 0) {
          memmove(rbuf_, rbuf_ + rbuf_pos_, rbuf_offset_);
        }
        rbuf_pos_ = 0;
      }
      // read from socket
      ssize_t nread;
      do {
        nread = read(fd, rbuf_ + rbuf_pos_ + rbuf_offset_,
                     rbuf_size_ - rbuf_pos_ - rbuf_offset_);
        if (nread == -1 && errno != EINTR && errno != EAGAIN) {
          log_err("Error reading from the server : %s", strerror(errno));
        }
        if (nread > 0) {
          rbuf_offset_ += nread;
          // parse result
          do {
            int type = TryReadType();
            if (type == REDIS_ERR) {
              log_err("Bad data from server");
              return NULL;
            } if (type == REDIS_HALF) {
              break;
            }

            int len;
            char *p = ReadLine(&len);
            if (p == NULL) {
              rbuf_offset_ += 1;
              rbuf_pos_ -= 1;
              break;
            }

            if (type == REDIS_REPLY_ERROR) {
              err_++;
              log_warn("%s", std::string(p, len).data());
            } else {
              elements_++;
              // Recive command "ECHO magic" reply
              if (eof && type == REDIS_REPLY_STRING) {
                if ((memcmp(p, "20", 2) == 0) && (memcmp(p + 4, magic, 20) == 0)) {
                  // std::cout << "Last reply received from server.\n";
                  // log_info("Last reply received from server.");
                  done = 1;
                  elements_--;
                  rbuf_pos_ = 0;
                  rbuf_offset_ = 0;
                  break;
                }
              }
            }
          } while (1);
        } // end of parse
      } while (nread > 0 && !done);
    }

    if (mask & kWritable) {
      size_t loop_nwritten = 0;
      while (!eof) {
        size_t len;
        {
          slash::MutexLock l(&buf_mutex_);
          // no data to send and should exit
          if (buf_len_ == 0) {
            if (should_exit_) {
              char echo[] =
                  "\r\n*2\r\n$4\r\nECHO\r\n$20\r\n01234567890123456789\r\n";
              for(int i = 0; i < 20; i++) {
                magic[i] = rand() % 26 + 65;
              }
              memcpy(echo + 21, magic, 20);
              memcpy(buf_, echo, sizeof(echo) - 1);
              buf_len_ = sizeof(echo) - 1;
              buf_pos_ = 0;
              // log_info("All data transferred. Waiting for the last reply...");
              eof = 1;
            } else {
              buf_r_cond_.Wait();
              if (should_exit_) {
                continue;
              }
            }
          }
          if (buf_len_ > 1024 * 16) {
            len = 1024 * 16;
          } else {
            len = buf_len_;
          }
        }

        int nwritten = write(fd, buf_ + buf_pos_, len);
        if (nwritten == -1) {
          if (errno != EAGAIN && errno != EINTR) {
            // std::cout << "Error writting to the server :" << strerror(errno) << std::endl;
            log_err("Error writting to the server : %s", strerror(errno));
            return NULL;
          } else {
            nwritten = 0;
          }
        }
        {
          slash::MutexLock l(&buf_mutex_);
          buf_len_ -= nwritten;
          buf_pos_ += nwritten;
          loop_nwritten += nwritten;
          buf_w_cond_.Signal();
          if (buf_len_ == 0) {
            buf_pos_ = 0;
          }
          if (loop_nwritten > kWirteLoopMaxBYTES || (eof && buf_len_ == 0)  || (size_t)nwritten < len) {
            break;
          }
        }
      }
    } // end of writable
  }   // end of while

  // std::cout << "Sender " << pthread_self() << "exit" << std::endl;
  // std::cout << "thread:" << pthread_self() << " replies:" << elements_;
  // std::cout << "errors:" << err_ << "\n";
  return NULL;
}

int SenderThread::TryReadType() {
  if (rbuf_offset_ == 0) {
    return REDIS_HALF;
  }

  char *p;
  if ((p = ReadBytes(1)) == NULL) {
    return REDIS_HALF;
  }

  int type;
  switch (*p) {
    case '-':
      type = REDIS_REPLY_ERROR;
      break;
    case '+':
      type = REDIS_REPLY_STATUS;
      break;
    case ':':
      type = REDIS_REPLY_INTEGER;
      break;
    case '$':
      type = REDIS_REPLY_STRING;
      break;
    case '*':
      type = REDIS_REPLY_ARRAY;
      break;
    default:
      return REDIS_ERR;
  }
  return type;
}

// int SenderThread::TryReadLine(char *_p, int *plen) {
// char *p;
// // when not a complete, p == NULL
// if ((p = ReadLine(plen)) == NULL) {
// return REDIS_HALF;
// }
// _p = p;
// return REDIS_OK;
// }

char *SenderThread::ReadLine(int *_len) {
  char *p, *s;
  int len;

  p = rbuf_ + rbuf_pos_;
  s = seekNewline(p, rbuf_offset_);
  if (s != NULL) {
    len = s - (rbuf_ + rbuf_pos_);
    rbuf_pos_ += len + 2; /* skip \r\n */
    rbuf_offset_ -= len + 2;
    if (_len) *_len = len;
    return p;
  }
  return NULL;
}
/* Find pointer to \r\n. */
char *SenderThread::seekNewline(char *s, size_t len) {
  int pos = 0;
  int _len = len - 1;

  /* Position should be < len-1 because the character at "pos" should be
   * followed by a \n. Note that strchr cannot be used because it doesn't
   * allow to search a limited length and the buffer that is being searched
   * might not have a trailing NULL character. */
  while (pos < _len) {
    while (pos < _len && s[pos] != '\r') pos++;
    if (s[pos] != '\r') {
      /* Not found. */
      return NULL;
    } else {
      if (s[pos+1] == '\n') {
        /* Found. */
        return s+pos;
      } else {
        /* Continue searching. */
        pos++;
      }
    }
  }
  return NULL;
}

char* SenderThread::ReadBytes(unsigned int bytes) {
  char *p = NULL;
  if ((unsigned int)rbuf_offset_ >= bytes) {
    p = rbuf_ + rbuf_pos_;
    rbuf_pos_ += bytes;
    rbuf_offset_ -= bytes;
  }
  return p;
}
