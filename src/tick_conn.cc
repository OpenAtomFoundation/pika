#include "tick_conn.h"
#include "tick_util.h"
#include "tick_packet.h"
#include "tick_server.h"
#include "leveldb/db.h"

extern TickServer *g_tickServer;

TickConn::TickConn(int fd) :
    fd_(fd)
{
    thread_ = NULL;

    // init the rbuf
    rbuf_ = (char *)malloc(sizeof(char) * TICK_MAX_MESSAGE);
    header_len_ = -1;
    r_opcode_ = 0;
    cur_pos_ = 0;
    rbuf_len_ = 0;
}

TickConn::~TickConn()
{
    free(rbuf_);
}

bool TickConn::SetNonblock()
{
    flags_ = Setnonblocking(fd_);
    if (flags_ == -1) {
        return false;
    }
    return true;
}


Status TickConn::TickReadBuf()
{
    Status s;
    rio_t rio;
    rio_readinitb(&rio, fd_);
    s = TickReadHeader(&rio);
    if (!s.ok()) {
        return s;
    }
    s = TickReadCode(&rio);
    if (!s.ok()) {
        return s;
    }
    s = TickReadPacket(&rio);
    return s;
}

Status TickConn::TickWriteBuf()
{
    return Status::OK();
}

void TickConn::DriveMachine()
{
/*
 *     while (1) {
 *         switch (connStatus) {
 *         kHeader:
 * 
 *     }
 * 
 */
}

Status TickConn::TickAReadHeader()
{
    ssize_t nread;
    int readlen;
    nread = read(fd_, rbuf_, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            return Status::IOError("IO Error");
        }
    } else if (nread == 0) {
        return Status::IOError("IO Error");
    }

    int32_t integer = 0;
    bool flag = true;
    if (nread) {
        rbuf_len_ += nread;
        while (flag) {
            switch (connStatus) {
            case kHeader:
                if (rbuf_len_ - cur_pos_ >= COMMAND_HEADER_LENGTH) {
                    memcpy((char *)(&integer), rbuf_ + cur_pos_, sizeof(int32_t));
                    header_len_ = ntohl(integer);
                    connStatus = kCode;
                    cur_pos_ += COMMAND_HEADER_LENGTH;
                } else {
                    flag = false;
                }
                break;
            case kCode:
                if (rbuf_len_ - cur_pos_ >= COMMAND_CODE_LENGTH) {
                    memcpy((char *)(&integer), rbuf_ + cur_pos_, sizeof(int32_t));
                    r_opcode_ = ntohl(integer);
                    connStatus = kPacket;
                    cur_pos_ += COMMAND_CODE_LENGTH;
                } else {
                    flag = false;
                }
                break;
            case kPacket:
                if (rbuf_len_ - cur_pos_ >= header_len_ - COMMAND_HEADER_LENGTH - COMMAND_CODE_LENGTH) {
                    connStatus = kComplete;
                } else {
                    flag = false;
                }
                break;
            case kComplete:
                std::string *key = new std::string();
                std::string *value = new std::string();
                SetBufferParse(r_opcode_, rbuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH, rbuf_len_ - COMMAND_HEADER_LENGTH - COMMAND_CODE_LENGTH, key, value);

                printf("%s %s\n", key->c_str(), value->c_str());
                g_tickServer->db_->Put(leveldb::WriteOptions(), (*key), (*value));
                delete(key);
                delete(value);
                return Status::OK();
                break;
/*
 * 
 *             default:
 *                 flag = false;
 *                 break;
 */
            }
        }
    } else {
        return Status::IOError("IO Error");
    }
}


Status TickConn::TickReadHeader(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread;
    header_len_ = 0;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_HEADER_LENGTH);
        // log_info("nread %d", nread);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read command header error");
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

Status TickConn::TickReadCode(rio_t *rio)
{
    Status s;
    char buf[1024];
    int32_t integer = 0;
    ssize_t nread = 0;
    r_opcode_ = 0;
    while (1) {
        nread = rio_readnb(rio, buf, COMMAND_CODE_LENGTH);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read command code error");
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

Status TickConn::TickReadPacket(rio_t *rio)
{
    Status s;
    int nread = 0;
    if (header_len_ < 4) {
        return Status::Corruption("The packet no integrity");
    }
    while (1) {
        nread = rio_readnb(rio, (void *)(rbuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH), header_len_ - 4);
        if (nread == -1) {
            if ((errno == EAGAIN && !(flags_ & O_NONBLOCK)) || (errno == EINTR)) {
                continue;
            } else {
                s = Status::IOError("Read data error");
                return s;
            }
        } else if (nread == 0) {
            return Status::Corruption("Connect has interrupt");
        } else {
            break;
        }
    }
    rbuf_len_ = nread;
    log_info("rbuf len %d", rbuf_len_);
    return Status::OK();
}

Status TickConn::BuildObuf()
{
    uint32_t code_len = COMMAND_CODE_LENGTH + rbuf_len_;
    uint32_t u;

    u = htonl(code_len);
    memcpy(rbuf_, &u, sizeof(uint32_t));
    u = htonl(r_opcode_);
    memcpy(rbuf_ + COMMAND_CODE_LENGTH, &u, sizeof(uint32_t));

    return Status::OK();
}
