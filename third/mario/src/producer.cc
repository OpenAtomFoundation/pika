#include "producer.h"
#include "xdebug.h"
#include "mario_item.h"

namespace mario {

Producer::Producer(WritableFile *queue, Version *version) :
    queue_(queue),
    version_(version)
{
    // log_info("offset %d kBlockSize %d block_offset_ %d", offset, kBlockSize,
    //         block_offset_);
    uint64_t filesize = queue->Filesize();
    // log_info("file size %lld", filesize);
    block_offset_ = filesize % kBlockSize;
    assert(queue_ != NULL);
}

Status Producer::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n)
{
    Status s;
    assert(n <= 0xffffff);
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    char buf[kHeaderSize];

    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(t);

    s = queue_->Append(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = queue_->Append(Slice(ptr, n));
        if (s.ok()) {
            s = queue_->Flush();
        }
    }
    block_offset_ += static_cast<int>(kHeaderSize + n);
    // log_info("block_offset %d", (kHeaderSize + n));
    version_->rise_pro_offset((uint64_t)(kHeaderSize + n));
    version_->StableSave();
    return s;
}


Producer::~Producer()
{

}

Status Producer::Produce(const Slice &item)
{
    Status s;
    const char *ptr = item.data();
    size_t left = item.size();
    bool begin = true;
    do {
        const int leftover = static_cast<int>(kBlockSize) - block_offset_;
        assert(leftover >= 0);
        if (static_cast<size_t>(leftover) < kHeaderSize) {
            if (leftover > 0) {
                queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
                version_->rise_pro_offset(leftover);
                version_->StableSave();
            }
            block_offset_ = 0;
        }

        const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
        const size_t fragment_length = (left < avail) ? left : avail;
        RecordType type;
        const bool end = (left == fragment_length);
        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }

        s = EmitPhysicalRecord(type, ptr, fragment_length);
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (s.ok() && left > 0);
    return s;
}

} // namespace mario
