#include "tick_conn.h"
#include "tick_util.h"

TickConn::TickConn(int fd) :
    fd_(fd)
{
    thread_ = NULL;
}

bool Tick::SetNonblock()
{
    flag_ = Setnonblocking(fd_);
    if (flag_ == -1) {
        return false;
    }
    return true;
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

Status TickServer::TickReadCode(rio_t *rio)
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

Status TickServer::TickReadPacket(rio_t *rio)
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
    return Status::OK();
}
