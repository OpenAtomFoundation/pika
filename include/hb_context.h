#ifndef __TICK_HB_CONTEXT_H__
#define __TICK_HB_CONTEXT_H_

#include "tick_define.h"
#include "fcntl.h"
#include "tick_define.h"
#include "status.h"
#include "csapp.h"

#include <errno.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdint.h>

class HbContext {
public:
    HbContext();
    ~HbContext();
    char* GetContextBuffer(int *buf_len);

    Status BuildObuf(int32_t opcode, const int packet_len);

    Status HbBufferWrite();
    Status HbBufferRead();


    void set_fd(const int fd) { fd_ = fd; }
    void set_flags(const int flags) { flags_ = flags; }

    Status SetBlockType(BlockType);
    Status SetTcpNoDelay();

    int fd() { return fd_; }
    int flags() { return flags_; }
    const char *rbuf() { return rbuf_; }
    int32_t rbuf_len() { return rbuf_len_; }
    int32_t r_opcode() { return r_opcode_; }
    // redisReader *reader; /* Protocol reader */
private:

    int fd_;
    int flags_;
    Status HbBufferReadHeader(rio_t *rio);
    Status HbBufferReadCode(rio_t *rio);
    Status HbBufferReadPacket(rio_t *rio);
    char *obuf_; /* Write buffer */
    int32_t obuf_len_;

    int header_len_;
    char *rbuf_;
    int32_t rbuf_len_;
    int32_t r_opcode_;

    HbContext(const HbContext&);
    void operator = (const HbContext&);
};

#endif
