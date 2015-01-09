#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include "Logger.h"
#include "Utils.h"
#include "ErrCode.h"
#include "Send.h"

KafkaBoundedByteBufferSend::KafkaBoundedByteBufferSend(KafkaFetchRequest *req)
    : sendSize_(0), buffer_(NULL), sizeWritten_(false), complete_(false)
{
    int offset = 4;
    size_ = req->sizeInBytes() + 2 + 4;
    buffer_ = (char*)malloc(sizeof(char) * size_); 
    KafkaUtils::writeInt16(req->getId(), (unsigned char*)buffer_, &offset);
    req->writeTo((unsigned char*)buffer_, &offset);
}

KafkaBoundedByteBufferSend::~KafkaBoundedByteBufferSend()
{
    if (NULL != buffer_) {
        free(buffer_);
        buffer_ = NULL;
    }
}
/**
 * Try to write the request size if we haven't already
 * 
 * @param resource $stream Stream resource
 *
 * @return integer Number of bytes read
 * @throws RuntimeException when size is <=0 or >= $maxSize
 */
int KafkaBoundedByteBufferSend::writeRequestSize() 
{
    int ret = ErrCode::OK, offset = 0;
    if ( ! sizeWritten_) {
        KafkaUtils::writeInt32(size_, (unsigned char*)buffer_, &offset);
        sizeWritten_ = true;
    }
    return ret;
}

/**
 * Write a chunk of data to the stream
 * 
 * @param resource $stream Stream resource
 * 
 * @return integer number of written bytes
 * @throws RuntimeException
 */
int KafkaBoundedByteBufferSend::writeTo(int stream) 
{
    int sent = 0;
    // have we written the request size yet?
    int ret = writeRequestSize();

    // try to write the actual buffer itself
    if ( sizeWritten_ && ret == ErrCode::OK){
        if (-1 == (sent = send(stream, buffer_ + sendSize_, size_ + 4 - sendSize_, 0))) {
            LOG_ERR("send error. %s", strerror(errno));
            ret = ErrCode::ERR_SOCK_SEND;
        }
        else {
            sendSize_ += sent;
        }
    }
    // if we are done, mark it off
    if (sendSize_ == size_ + 4) {
        complete_ = true;
    }
    return ret;
}

/**
 * Write the entire request to the stream
 * 
 * @param resource $stream Stream resource
 * 
 * @return integer number of written bytes
 */
int KafkaBoundedByteBufferSend::writeCompletely(int stream, int *sendSize) 
{
    int writeRet = ErrCode::OK;
    while (! complete_ && ErrCode::OK == writeRet) {
        writeRet = writeTo(stream);
    }

    if (ErrCode::OK == writeRet) {
        *sendSize = sendSize_;
        //LOG_INFO("KafkaBoundedByteBufferSend::writeCompletely send(send size:%d) OK", sendSize_);
    }
    
    return writeRet;
}
