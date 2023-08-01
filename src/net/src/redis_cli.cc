// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/redis_cli.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstdarg>

#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "pstd/include/noncopyable.h"
#include "net/include/net_cli.h"
#include "net/include/net_define.h"

using pstd::Status;

namespace net {

class RedisCli : public NetCli {
 public:
  RedisCli();
  ~RedisCli() override;

  // msg should have been parsed
  Status Send(void* msg) override;

  // Read, parse and store the reply
  Status Recv(void* trival = nullptr) override;

 private:
  RedisCmdArgsType argv_;  // The parsed result

  char* rbuf_;
  int32_t rbuf_size_{REDIS_IOBUF_LEN};
  int32_t rbuf_pos_{0};
  int32_t rbuf_offset_{0};
  int32_t elements_;  // the elements number of this current reply
  int32_t err_;

  int32_t GetReply();
  int32_t GetReplyFromReader();

  int32_t ProcessLineItem();
  int32_t ProcessBulkItem();
  int32_t ProcessMultiBulkItem();

  ssize_t BufferRead();
  char* ReadBytes(uint32_t bytes);
  char* ReadLine(int32_t* _len);

};

enum REDIS_STATUS {
  REDIS_ETIMEOUT = -5,
  REDIS_EREAD_NULL = -4,
  REDIS_EREAD = -3,  // errno is set
  REDIS_EPARSE_TYPE = -2,
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

RedisCli::RedisCli()  {
  rbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * rbuf_size_));
}

RedisCli::~RedisCli() { free(rbuf_); }

// We use passed-in send buffer here
Status RedisCli::Send(void* msg) {
  Status s;

  // TODO(anan) use socket_->SendRaw instead
  auto storage = reinterpret_cast<std::string*>(msg);
  const char* wbuf = storage->data();
  size_t nleft = storage->size();

  ssize_t wbuf_pos = 0;

  ssize_t nwritten;
  while (nleft > 0) {
    if ((nwritten = write(fd(), wbuf + wbuf_pos, nleft)) <= 0) {
      if (errno == EINTR) {
        nwritten = 0;
        continue;
        // blocking fd after setting setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,...)
        // will return EAGAIN | EWOULDBLOCK for timeout
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        s = Status::Timeout("Send timeout");
      } else {
        s = Status::IOError("write error " + std::string(strerror(errno)));
      }
      return s;
    }

    nleft -= nwritten;
    wbuf_pos += nwritten;
  }

  return s;
}

// The result is useless
Status RedisCli::Recv(void* trival) {
  argv_.clear();
  int32_t result = GetReply();
  switch (result) {
    case REDIS_OK:
      if (trival) {
        *static_cast<RedisCmdArgsType*>(trival) = argv_;
      }
      return Status::OK();
    case REDIS_ETIMEOUT:
      return Status::Timeout("");
    case REDIS_EREAD_NULL:
      return Status::IOError("Read null");
    case REDIS_EREAD:
      return Status::IOError("read failed caz " + std::string(strerror(errno)));
    case REDIS_EPARSE_TYPE:
      return Status::IOError("invalid type");
    default:  // other error
      return Status::IOError("other error, maybe " + std::string(strerror(errno)));
  }
}

ssize_t RedisCli::BufferRead() {
  // memmove the remain chars to rbuf begin
  if (rbuf_pos_ > 0) {
    if (rbuf_offset_ > 0) {
      memmove(rbuf_, rbuf_ + rbuf_pos_, rbuf_offset_);
    }
    rbuf_pos_ = 0;
  }

  ssize_t nread;

  while (true) {
    nread = read(fd(), rbuf_ + rbuf_offset_, rbuf_size_ - rbuf_offset_);

    if (nread == -1) {
      if (errno == EINTR) {
        continue;
        // blocking fd after setting setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,...)
        // will return EAGAIN for timeout
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return REDIS_ETIMEOUT;
      } else {
        return REDIS_EREAD;
      }
    } else if (nread == 0) {  // we consider read null an error
      return REDIS_EREAD_NULL;
    }

    rbuf_offset_ += static_cast<int32_t>(nread);
    return nread;
  }
}

/* Find pointer to \r\n. */
static char* seekNewline(char* s, size_t len) {
  int32_t pos = 0;
  auto _len = static_cast<int32_t>(len - 1);

  /* Position should be < len-1 because the character at "pos" should be
   * followed by a \n. Note that strchr cannot be used because it doesn't
   * allow to search a limited length and the buffer that is being searched
   * might not have a trailing nullptr character. */
  while (pos < _len) {
    while (pos < _len && s[pos] != '\r') {
      pos++;
    }
    if (s[pos] != '\r' || pos >= _len) {
      /* Not found. */
      return nullptr;
    } else {
      if (s[pos + 1] == '\n') {
        /* Found. */
        return s + pos;
      } else {
        /* Continue searching. */
        pos++;
      }
    }
  }
  return nullptr;
}

/* Read a int64_t value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static int64_t readLongLong(char* s) {
  int64_t v = 0;
  int32_t dec;
  int32_t mult = 1;
  char c;

  if (*s == '-') {
    mult = -1;
    s++;
  } else if (*s == '+') {
    mult = 1;
    s++;
  }

  while ((c = *(s++)) != '\r') {
    dec = c - '0';
    if (dec >= 0 && dec < 10) {
      v *= 10;
      v += dec;
    } else {
      /* Should not happen... */
      return -1;
    }
  }

  return mult * v;
}

int32_t RedisCli::ProcessLineItem() {
  char* p;
  int32_t len;

  if (!(p = ReadLine(&len))) {
    return REDIS_HALF;
  }

  std::string arg(p, len);
  argv_.push_back(arg);
  elements_--;

  return REDIS_OK;
}

int32_t RedisCli::ProcessBulkItem() {
  char *p;
  char *s;
  int32_t len;
  int32_t bytelen;

  p = rbuf_ + rbuf_pos_;
  s = seekNewline(p, rbuf_offset_);
  if (s) {
    bytelen = static_cast<int32_t>(s - p + 2); /* include \r\n */
    len = static_cast<int32_t>(readLongLong(p));

    if (len == -1) {
      elements_--;

      rbuf_pos_ += bytelen; /* case '$-1\r\n' */
      rbuf_offset_ -= bytelen;
      return REDIS_OK;
    } else if (len + 2 <= rbuf_offset_) {
      argv_.push_back(std::string(p + bytelen, len));
      elements_--;

      bytelen += len + 2; /* include \r\n */
      rbuf_pos_ += bytelen;
      rbuf_offset_ -= bytelen;
      return REDIS_OK;
    }
  }

  return REDIS_HALF;
}

int32_t RedisCli::ProcessMultiBulkItem() {
  char* p;
  int32_t len;

  if (p = ReadLine(&len); p) {
    elements_ = static_cast<int32_t>(readLongLong(p));
    return REDIS_OK;
  }

  return REDIS_HALF;
}

int32_t RedisCli::GetReply() {
  int32_t result = REDIS_OK;

  elements_ = 1;
  while (elements_ > 0) {
    // Should read again
    if (rbuf_offset_ == 0 || result == REDIS_HALF) {
      if ((result = static_cast<int32_t>(BufferRead())) < 0) {
        return result;
      }
    }

    // stop if error occured.
    if ((result = GetReplyFromReader()) < REDIS_OK) {
      break;
    }
  }

  return result;
}

char* RedisCli::ReadBytes(uint32_t bytes) {
  char* p = nullptr;
  if (static_cast<uint32_t>(rbuf_offset_) >= bytes) {
    p = rbuf_ + rbuf_pos_;
    rbuf_pos_ += static_cast<int32_t>(bytes);
    rbuf_offset_ -= static_cast<int32_t>(bytes);
  }
  return p;
}

char* RedisCli::ReadLine(int32_t* _len) {
  char *p;
  char *s;
  int32_t len;

  p = rbuf_ + rbuf_pos_;
  s = seekNewline(p, rbuf_offset_);
  if (s) {
    len = static_cast<int32_t>(s - (rbuf_ + rbuf_pos_));
    rbuf_pos_ += len + 2; /* skip \r\n */
    rbuf_offset_ -= len + 2;
    if (_len) {
      *_len = len;
    }
    return p;
  }
  return nullptr;
}

int32_t RedisCli::GetReplyFromReader() {
  // if (err_) {
  //   return REDIS_ERR;
  // }

  if (rbuf_offset_ == 0) {
    return REDIS_HALF;
  }

  char* p;
  if (!(p = ReadBytes(1))) {
    return REDIS_HALF;
  }

  int32_t type;
  // Check reply type
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
      return REDIS_EPARSE_TYPE;
  }

  switch (type) {
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_INTEGER:
      // elements_ = 1;
      return ProcessLineItem();
    case REDIS_REPLY_STRING:
      // need processBulkItem();
      // elements_ = 1;
      return ProcessBulkItem();
    case REDIS_REPLY_ARRAY:
      // need processMultiBulkItem();
      return ProcessMultiBulkItem();
    default:
      return REDIS_EPARSE_TYPE;  // Avoid warning.
  }
}

NetCli* NewRedisCli() { return new RedisCli(); }
//
// Redis protocol related funcitons
//

// Calculate the number of bytes needed to represent an integer as string.
static int32_t intlen(int32_t i) {
  int32_t len = 0;
  if (i < 0) {
    len++;
    i = -i;
  }
  do {
    len++;
    i /= 10;
  } while (i != 0);
  return len;
}

// Helper that calculates the bulk length given a certain string length.
static size_t bulklen(size_t len) { return 1 + intlen(static_cast<int32_t>(len)) + 2 + len + 2; }

int32_t redisvFormatCommand(std::string* cmd, const char* format, va_list ap) {
  const char* c = format;
  std::string curarg;
  char buf[1048576];
  std::vector<std::string> args;
  int32_t touched = 0; /* was the current argument touched? */
  size_t totlen = 0;

  while (*c != '\0') {
    if (*c != '%' || c[1] == '\0') {
      if (*c == ' ') {
        if (touched != 0) {
          args.push_back(curarg);
          totlen += bulklen(curarg.size());
          curarg.clear();
          touched = 0;
        }
      } else {
        curarg.append(c, 1);
        touched = 1;
      }
    } else {
      char* arg = nullptr;
      size_t size = 0;

      switch (c[1]) {
        case 's':
          arg = va_arg(ap, char*);
          size = strlen(arg);
          if (size > 0) {
            curarg.append(arg, size);
          }
          break;
        case 'b':
          arg = va_arg(ap, char*);
          size = va_arg(ap, size_t);
          if (size > 0) {
            curarg.append(arg, size);
          }
          break;
        case '%':
          curarg.append(arg, size);
          break;
        default:
          /* Try to detect printf format */
          {
            static const char intfmts[] = "diouxX";
            char _format[16];
            const char* _p = c + 1;
            size_t _l = 0;
            va_list _cpy;
            bool fmt_valid = false;

            /* Flags */
            if (*_p != '\0' && *_p == '#') {
              _p++;
            }
            if (*_p != '\0' && *_p == '0') {
              _p++;
            }
            if (*_p != '\0' && *_p == '-') {
              _p++;
            }
            if (*_p != '\0' && *_p == ' ') {
              _p++;
            }
            if (*_p != '\0' && *_p == '+') {
              _p++;
            }

            /* Field width */
            while (*_p != '\0' && (isdigit(*_p) != 0)) {
              _p++;
            }

            /* Precision */
            if (*_p == '.') {
              _p++;
              while (*_p != '\0' && (isdigit(*_p) != 0)) {
                _p++;
              }
            }

            /* Copy va_list before consuming with va_arg */
            va_copy(_cpy, ap);

            if (strchr(intfmts, *_p)) {
              /* Integer conversion (without modifiers) */
              va_arg(ap, int32_t);
              fmt_valid = true;
            } else if (strchr("eEfFgGaA", *_p)) {
              /* Double conversion (without modifiers) */
              va_arg(ap, double);
              fmt_valid = true;
            } else if (_p[0] == 'h' && _p[1] == 'h') { /* Size: char */
              _p += 2;
              if (*_p != '\0' && strchr(intfmts, *_p)) {
                va_arg(ap, int32_t); /* char gets promoted to int32_t */
                fmt_valid = true;
              }
            } else if (_p[0] == 'h') { /* Size: int16_t */
              _p += 1;
              if (*_p != '\0' && strchr(intfmts, *_p)) {
                va_arg(ap, int32_t); /* int16_t gets promoted to int32_t */
                fmt_valid = true;
              }
            } else if (_p[0] == 'l' && _p[1] == 'l') { /* Size: int64_t */
              _p += 2;
              if (*_p != '\0' && strchr(intfmts, *_p)) {
                va_arg(ap, int64_t);
                fmt_valid = true;
              }
            } else if (_p[0] == 'l') { /* Size: int32_t */
              _p += 1;
              if (*_p != '\0' && strchr(intfmts, *_p)) {
                va_arg(ap, int32_t);
                fmt_valid = true;
              }
            }

            if (!fmt_valid) {
              va_end(_cpy);
              return REDIS_ERR;
            }

            _l = (_p + 1) - c;
            if (_l < sizeof(_format) - 2) {
              memcpy(_format, c, _l);
              _format[_l] = '\0';

              int32_t n = vsnprintf(buf, sizeof(buf), _format, _cpy);
              curarg.append(buf, n);

              /* Update current position (note: outer blocks
               * increment c twice so compensate here) */
              c = _p - 1;
            }

            va_end(_cpy);
            break;
          }
      }

      if (curarg.empty()) {
        return REDIS_ERR;
      }

      touched = 1;
      c++;
    }
    c++;
  }

  /* Add the last argument if needed */
  if (touched != 0) {
    args.push_back(curarg);
    totlen += bulklen(curarg.size());
  }

  /* Add bytes needed to hold multi bulk count */
  totlen += 1 + intlen(static_cast<int32_t>(args.size())) + 2;

  /* Build the command at protocol level */
  cmd->clear();
  cmd->reserve(totlen);

  cmd->append(1, '*');
  cmd->append(std::to_string(args.size()));
  cmd->append("\r\n");
  for (auto & arg : args) {
    cmd->append(1, '$');
    cmd->append(std::to_string(arg.size()));
    cmd->append("\r\n");
    cmd->append(arg);
    cmd->append("\r\n");
  }
  assert(cmd->size() == totlen);

  return static_cast<int32_t>(totlen);
}

int32_t redisvAppendCommand(std::string* cmd, const char* format, va_list ap) {
  int32_t len = redisvFormatCommand(cmd, format, ap);
  if (len == -1) {
    return REDIS_ERR;
  }

  return REDIS_OK;
}

int32_t redisFormatCommandArgv(RedisCmdArgsType argv, std::string* cmd) {
  size_t argc = argv.size();

  size_t totlen = 1 + intlen(static_cast<int32_t>(argc)) + 2;
  for (size_t i = 0; i < argc; i++) {
    totlen += bulklen(argv[i].size());
  }

  cmd->clear();
  cmd->reserve(totlen);

  cmd->append(1, '*');
  cmd->append(std::to_string(argc));
  cmd->append("\r\n");
  for (size_t i = 0; i < argc; i++) {
    cmd->append(1, '$');
    cmd->append(std::to_string(argv[i].size()));
    cmd->append("\r\n");
    cmd->append(argv[i]);
    cmd->append("\r\n");
  }

  return REDIS_OK;
}

int32_t SerializeRedisCommand(std::string* cmd, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  int32_t result = redisvAppendCommand(cmd, format, ap);
  va_end(ap);
  return result;
}

int32_t SerializeRedisCommand(RedisCmdArgsType argv, std::string* cmd) { return redisFormatCommandArgv(std::move(argv), cmd); }

};  // namespace net
