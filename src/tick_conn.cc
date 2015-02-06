#include "tick_conn.h"
#include "tick_util.h"
#include "tick_packet.h"
#include "tick_server.h"
#include "leveldb/db.h"
#include "bada_sdk.pb.h"
#include "tick_define.h"

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

    wbuf_ = (char *)malloc(sizeof(char) * TICK_MAX_MESSAGE);
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

void TickConn::DriveMachine()
{
/*
 *     while (1) {
 *         switch (connStatus_) {
 *         kHeader:
 * 
 *     }
 * 
 */
}

void TickConn::TickGetRequest()
{
    ssize_t nread = 0;
    nread = read(fd_, rbuf_ + rbuf_len_, TICK_MAX_MESSAGE);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            return ;
        }
    } else if (nread == 0) {
        return ;
    }

    int32_t integer = 0;
    bool flag = true;
    std::string *key;
    std::string *value;
    SdkSetRet sdkSetRet;
    int packet_len;
    if (nread) {
        rbuf_len_ += nread;
        while (flag) {
            switch (connStatus_) {
            case kHeader:
                if (rbuf_len_ - cur_pos_ >= COMMAND_HEADER_LENGTH) {
                    memcpy((char *)(&integer), rbuf_ + cur_pos_, sizeof(int32_t));
                    header_len_ = ntohl(integer);
                    log_info("Header_len %d", header_len_);
                    connStatus_ = kCode;
                    cur_pos_ += COMMAND_HEADER_LENGTH;
                } else {
                    flag = false;
                }
                break;
            case kCode:
                if (rbuf_len_ - cur_pos_ >= COMMAND_CODE_LENGTH) {
                    memcpy((char *)(&integer), rbuf_ + cur_pos_, sizeof(int32_t));
                    r_opcode_ = ntohl(integer);
                    connStatus_ = kPacket;
                    cur_pos_ += COMMAND_CODE_LENGTH;
                } else {
                    flag = false;
                }
                break;
            case kPacket:
                if (rbuf_len_ >= header_len_ - COMMAND_CODE_LENGTH) {
                    cur_pos_ += (header_len_ - COMMAND_CODE_LENGTH);
                    connStatus_ = kComplete;
                } else {
                    flag = false;
                }
                break;
            case kComplete:
                key = new std::string();
                value = new std::string();
                SetBufferParse(r_opcode_, rbuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH, rbuf_len_ - COMMAND_HEADER_LENGTH - COMMAND_CODE_LENGTH, key, value);

                // printf("%s %s\n", key->c_str(), value->c_str());
                g_tickServer->db_->Put(leveldb::WriteOptions(), (*key), (*value));
                SetBufferBuild(true, &sdkSetRet);
                packet_len = TICK_MAX_MESSAGE;
                sdkSetRet.SerializeToArray(wbuf_ + COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH, packet_len);
                delete(key);
                delete(value);
                BuildObuf(kSdkSetRet, sdkSetRet.ByteSize());
                connStatus_ = kHeader;
                if (cur_pos_ == rbuf_len_) {
                    cur_pos_ = 0;
                    rbuf_len_ = 0;
                }
                return 0;
                break;

            /*
             * Add this switch case just for delete compile warning
             */
            case kBuildObuf:
                break;

            case kWriteObuf:
                break;
            }
        }
    }
}

int TickConn::TickSendReply()
{
    ssize_t nwritten = 0;
    log_info("wbuf_len %d", wbuf_len_);
    while (wbuf_len_ > 0) {
        /*
         * log_info("write buf %s\n", wbuf_ + wbuf_pos_);
         */
        // for (int i = 0; i < wbuf_len_; i++) {
        //     log_info("%c", wbuf_[i]);
        // }
        nwritten = write(fd_, wbuf_ + wbuf_pos_, wbuf_len_ - wbuf_pos_);
        if (nwritten <= 0) {
            break;
        }
        wbuf_pos_ += nwritten;
        if (wbuf_pos_ == wbuf_len_) {
            wbuf_len_ = 0;
            wbuf_pos_ = 0;
        }

        if (nwritten == -1) {
            if (errno == EAGAIN) {
                nwritten = 0;
            } else {
                /*
                 * Here we clear this connection
                 */
                return 0;
            }
        }
    }
    if (wbuf_len_ == 0) {
        return 0;
    } else {
        return -1;
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

Status TickConn::BuildObuf(int32_t opcode, const int packet_len)
{
    uint32_t code_len = COMMAND_CODE_LENGTH + packet_len;
    uint32_t u;

    u = htonl(code_len);
    memcpy(wbuf_, &u, sizeof(uint32_t));
    u = htonl(opcode);
    memcpy(wbuf_ + COMMAND_CODE_LENGTH, &u, sizeof(uint32_t));

    wbuf_len_ = COMMAND_HEADER_LENGTH + COMMAND_CODE_LENGTH + packet_len;

    return Status::OK();
}
