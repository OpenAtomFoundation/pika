#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include "ErrCode.h"
#include "Receive.h"
#include "Utils.h"
#include "Logger.h"

KafkaBoundedByteBufferReceive::KafkaBoundedByteBufferReceive(int maxBufSize)
    : buffer_(NULL), maxBufSize_(maxBufSize), remainingBytes_(0),
      isSizeRead_(false), complete_(false)
{
    maxBufSize_ = (maxBufSize_ > 0) ? maxBufSize_ : 1048576; // 1MB
}

KafkaBoundedByteBufferReceive::~KafkaBoundedByteBufferReceive()
{
    if (NULL != buffer_) {
        free(buffer_);
        buffer_ = NULL;
    }
}

int KafkaBoundedByteBufferReceive::readRequestSize(int stream, int *readSize)
{
    char buf[4], errbuf[64];
    int offset = 0, recvSize = 0;

    *readSize = 0;

    if (isSizeRead_) return ErrCode::OK;

    while (offset < 4) {
        recvSize = recv(stream, buf + offset, 4 - offset, 0);
        if (recvSize == -1) {
            LOG_ERR("readRequestSize recv error. %s", KafkaUtils::strerr_ts(errno, errbuf, sizeof(errbuf)));
            // server may have closed the sock or error ocurred
            return ErrCode::ERR_SOCK_RECV;
        }
        if (recvSize == 0) {
            LOG_ERR("readRequestSize recv, the peer has performed an orderly shutdown");
            // server may have closed the sock or error ocurred
            return ErrCode::ERR_SOCK_RECV;
        }
        offset += recvSize;
        *readSize += recvSize;
    }

    // no need at all
    if (offset != 4) {
        LOG_ERR("2 readRequestSize recv error. %s", KafkaUtils::strerr_ts(errno, errbuf, sizeof(errbuf)));
        return ErrCode::ERR_SOCK_RECV;
    }

    size_ = KafkaUtils::readUint32((u_char *)buf, NULL);
    if (size_ > maxBufSize_) {
        char *p = (char *)realloc(buffer_, sizeof(char) * size_);
        if (p == NULL) {
            LOG_ERR("realloc (size:%ld) failed. %s", size_, KafkaUtils::strerr_ts(errno, errbuf, sizeof(errbuf)));
            return ErrCode::ERR_MALLOC_MEM;
        }
        maxBufSize_ = size_;
        buffer_ = p;
    }

    remainingBytes_ = size_;

    isSizeRead_ = true;

    return ErrCode::OK;
}

int KafkaBoundedByteBufferReceive::readFrom(int stream, int *totalRead)
{
		char errbuf[64];
    int ret = ErrCode::OK, readReqSize = 0;
    // have we read the request size yet?
    if (ErrCode::OK != (ret = readRequestSize(stream, &readReqSize))) {
        return ret;
    }
    (*totalRead) += readReqSize;

    // have we allocated the request buffer yet?
    if (NULL == buffer_) {
        if (NULL == (buffer_ = (char*)malloc(sizeof(char) * maxBufSize_))) {
            LOG_ERR("malloc (size:%d) failed.  %s", maxBufSize_, KafkaUtils::strerr_ts(errno, errbuf, sizeof(errbuf)));
            ret = ErrCode::ERR_MALLOC_MEM;
        }
    }

    if (! complete_ && NULL != buffer_) {
        int bytesRecv = recv(stream, buffer_ + size_ - remainingBytes_,
                maxBufSize_ - size_ + remainingBytes_, 0);
        // server may have closed the socket or error ocurred
       if (-1 == bytesRecv) {
            LOG_ERR("readFrom recv(recv bytes:%d) error. %s", bytesRecv, KafkaUtils::strerr_ts(errno, errbuf, sizeof(errbuf)));
            ret = ErrCode::ERR_SOCK_RECV;
        } else if (0 == bytesRecv) {
            LOG_ERR("readFrom recv, the peer has performed an orderly shutdown");
            ret = ErrCode::ERR_SOCK_RECV;
				} else {
            remainingBytes_ -= bytesRecv;
            (*totalRead) += bytesRecv;
            if (remainingBytes_ <= 0) {
                complete_ = true;
            }
        }
    }

    return ret;
}

/**
 * Read all the available bytes in the stream
 *
 * @param stream    socket descriptor
 * @param recvSize  receive data size
 *
 * @return error code
 */
int KafkaBoundedByteBufferReceive::readCompletely(int stream, int *recvSize)
{
    int ret = ErrCode::OK;

    *recvSize = 0;
    while (! complete_ && ErrCode::OK == ret) {
        ret = readFrom(stream, recvSize);
    }

    return ret;
}

char* KafkaBoundedByteBufferReceive::getBuffer()
{
    return buffer_;
}

long int KafkaBoundedByteBufferReceive::getBufferSize()
{
    return size_ - remainingBytes_;
}
