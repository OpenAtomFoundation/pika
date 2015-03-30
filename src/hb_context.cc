#include "hb_context.h"
#include "csapp.h"
#include "xdebug.h"
#include "pika_util.h"
#include "pika_define.h"

HbContext::HbContext()
{
    fd_ = -1;
    obuf_ = (char *)malloc(sizeof(char) * HB_MAX_BUFFER);
    obuf_len_ = 0;
    header_len_ = 0;
    r_opcode_ = 0;
    rbuf_len_ = 0;
    rbuf_ = (char *)malloc(sizeof(char) * HB_MAX_BUFFER);
}

HbContext::~HbContext()
{
    free(obuf_);
    free(rbuf_);
    SafeClose(fd_);
}


Status HbContext::SetBlockType(BlockType type)
{
    Status s;
    if ((flags_ = fcntl(fd_, F_GETFL, 0)) < 0) {
        s = Status::Corruption("F_GETFEL error");
        SafeClose(fd_);
        return s;
    }
    if (type == kBlock) {
        flags_ &= (~O_NONBLOCK);
    } else if (type == kNonBlock) {
        flags_ |= O_NONBLOCK;
    }
    if (fcntl(fd_, F_SETFL, flags_) < 0) {
        s = Status::Corruption("F_SETFL error");
        SafeClose(fd_);
        return s;
    }
    return Status::OK();
}

Status HbContext::SetTcpNoDelay()
{
    Status s;
    int yes = 1;
    if (setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
        s = Status::Corruption("setsockopt(TCO_NODELAY) error");
        SafeClose(fd_);
        return s;
    }
    return s;
}

char* HbContext::GetContextBuffer(int *buf_len)
{
   if(!buf_len){
       return NULL;
   }
   int offset = COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH;
   *buf_len = HB_MAX_BUFFER - offset; 
   return obuf_ + offset; 
}

Status HbContext::BuildObuf(int32_t opcode, const int packet_len)
{
    Status s;
    //obuf_ = (char *)realloc(obuf_, sizeof(char) * (packet_len + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH));

    uint32_t code_len = COMMAND_CODE_LENGTH + packet_len;
    uint32_t u;

    u = htonl(code_len);
    memcpy(obuf_, &u, sizeof(uint32_t));
    u = htonl(opcode);
    memcpy(obuf_ + COMMAND_CODE_LENGTH, &u, sizeof(uint32_t));
    //memcpy(obuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH, packet, packet_len);

    obuf_len_ = COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH + packet_len;

    r_opcode_  = opcode;
    return s;
}

Status HbContext::HbBufferWrite()
{
    log_info("Hb obuf_len %d", obuf_len_);
    Status s;
    int32_t nwritten = 0;
    nwritten = rio_writen(fd_, (void *)obuf_, obuf_len_);
    if (nwritten == -1) {
        if ((errno == EAGAIN && (flags_ & O_NONBLOCK)) || (errno == EINTR)) {
        } else {
            s = Status::IOError(obuf_, "write heartbeat context error");
            return s;
        }
    }
    return Status::OK();
}

Status HbContext::HbBufferRead()
{
    Status s;
    rio_t rio;
    rio_readinitb(&rio, fd_);
    s = HbBufferReadHeader(&rio);
    if (!s.ok()) {
        return s;
    }
    s = HbBufferReadCode(&rio);
    if (!s.ok()) {
        return s;
    }
    s = HbBufferReadPacket(&rio);
    return s;
}


Status HbContext::HbBufferReadHeader(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_HEADER_LENGTH);
        if (nread == -1) {
            if ((errno == EAGAIN && (flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("read command header error");
                return s;
            }
        } else if (nread == 0){
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    memcpy((char *)(&integer), buf, sizeof(int32_t));
    header_len_ = ntohl(integer);
    return Status::OK();
}

Status HbContext::HbBufferReadCode(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread = 0;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_CODE_LENGTH);
        if (nread == -1) {
            if ((errno == EAGAIN && (flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("read command code error");
                return s;
            }
        } else if (nread == 0){
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    memcpy((char *)(&integer), buf, sizeof(int32_t));
    r_opcode_ = ntohl(integer);
    return Status::OK();
}

Status HbContext::HbBufferReadPacket(rio_t *rio)
{
    Status s;
    //char buf[MAX_PACKAGE_LEN];
    int nread = 0;
    while (1) {
        nread = rio_readnb(rio, (void*)rbuf_, header_len_ - 4); 
        if (nread == -1) {
            if ((errno == EAGAIN && (flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("read data error");
                return s;
            }
        } else if (nread == 0){
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    // todo to check the length of rbuf_
    //rbuf_ = (char *)realloc(rbuf_, sizeof(char) * nread);
    //memcpy(rbuf_, buf, nread);
    rbuf_len_ = nread;
    return s;
}



