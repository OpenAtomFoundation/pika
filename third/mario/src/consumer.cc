#include "consumer.h"
#include "xdebug.h"
#include "mario_item.h"
#include "version.h"
#include <iostream>


namespace mario {


//Consumer::Consumer(SequentialFile* const queue, Handler *h, Version* version,
//        uint32_t filenum)

Consumer::Consumer(SequentialFile* const queue, Handler *h, uint64_t con_offset,
        uint32_t filenum)
    : h_(h),
    initial_offset_(0),
    end_of_buffer_offset_(kBlockSize),
    queue_(queue),
    backing_store_(new char[kBlockSize]),
    buffer_(),
    eof_(false),
    con_offset_(con_offset),
    filenum_(filenum)
{
//    last_record_offset_ = con_offset % kBlockSize;
//    queue_->Skip(con_offset);
}

Consumer::~Consumer()
{
    delete [] backing_store_;
}

int Consumer::trim() {
    Status s;
    uint64_t start_block = (con_offset_ / kBlockSize) * kBlockSize;
    s = queue_->Skip((con_offset_ / kBlockSize) * kBlockSize);
    uint64_t block_offset = con_offset_ % kBlockSize;
    uint64_t ret = 0;
    uint64_t res = 0;
    bool is_error = false;
    while (true) {
        if (res >= block_offset) {
            con_offset_ = start_block + res;
            break;
        }
        ret = get_next(is_error);
        if (is_error == true) {
            return -1;
        }
        res += ret;
    }
    return 0;
}

uint64_t Consumer::get_next(bool &is_error) {
    uint64_t offset = 0;
    Status s;
    is_error = false;
    while (true) {
        buffer_.clear();
        s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
        if (!s.ok()) {
            is_error = true;
        }
        const char* header = buffer_.data();
        const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
        const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
        const unsigned int type = header[3];
        const uint32_t length = a | (b << 8) | (c << 16);
        if (type == kFullType) {
            s = queue_->Read(length, &buffer_, backing_store_);
            offset += kHeaderSize+length;
            break;
        } else if (type == kFirstType) {
            s = queue_->Read(length, &buffer_, backing_store_);
            offset += kHeaderSize+length;
        } else if (type == kMiddleType) {
            s = queue_->Read(length, &buffer_, backing_store_);
            offset += kHeaderSize+length;
        } else if (type == kLastType) {
            s = queue_->Read(length, &buffer_, backing_store_);
            offset += kHeaderSize+length;
            break;
        } else {
            is_error = true;
            break;
        }  
    }
    return offset;
    
}

unsigned int Consumer::ReadPhysicalRecord(Slice *result)
{
    Status s;
    if (end_of_buffer_offset_ - last_record_offset_ <= kHeaderSize) {
        queue_->Skip(end_of_buffer_offset_ - last_record_offset_);
        con_offset_ += (end_of_buffer_offset_ - last_record_offset_);
        last_record_offset_ = 0;
    }
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_);
    if (s.IsEndFile()) {
        return kEof;
    } else if (!s.ok()) {
        return kBadRecord;
    }
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[3];
    const uint32_t length = a | (b << 8) | (c << 16);
    if (type == kZeroType || length == 0) {
        buffer_.clear();
        return kOldRecord;
    }
    buffer_.clear();
    s = queue_->Read(length, &buffer_, backing_store_);
    *result = Slice(buffer_.data(), buffer_.size());
    last_record_offset_ += kHeaderSize + length;
    if (s.ok()) {
        con_offset_ += (kHeaderSize + length);
    }
    return type;
}


Consumer::Handler::~Handler() {};

Status Consumer::Consume(std::string &scratch)
{
    Status s;
    if (last_record_offset_ < initial_offset_) {
        return Status::IOError("last_record_offset exceed");
    }

    Slice fragment;
    while (true) {

        const unsigned int record_type = ReadPhysicalRecord(&fragment);
        switch (record_type) {
        case kFullType:
            scratch = std::string(fragment.data(), fragment.size());
            s = Status::OK();
            break;
        case kFirstType:
            scratch.assign(fragment.data(), fragment.size());
            s = Status::NotFound("Middle Status");
            break;
        case kMiddleType:
            scratch.append(fragment.data(), fragment.size());
            s = Status::NotFound("Middle Status");
            break;
        case kLastType:
            scratch.append(fragment.data(), fragment.size());
            s = Status::OK();
            break;
        case kEof:
            return Status::EndFile("Eof");
        case kBadRecord:
            return Status::Corruption("Data Corruption");
        case kOldRecord:
            return Status::EndFile("Eof");
        default:
            s = Status::Corruption("Unknow reason");
            break;
        }
        // TODO:do handler here
        if (s.ok()) {
            break;
        }
    }
    return Status::OK();
}

}
