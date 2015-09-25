#ifndef __MARIO_ITEM_H__
#define __MARIO_ITEM_H__

namespace mario {

enum RecordType {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4,
    kEof = 5,
    kBadRecord = 6,
    kOldRecord = 7
};

/*
 * the block size that we read and write from write2file
 * the default size is 64KB
 */
static const size_t kBlockSize = 64 * 1024;

/*
 * Header is Type(1 byte), length (2 bytes)
 */
static const size_t kHeaderSize = 1 + 3;

/*
 * the size of memory when we use memory mode
 * the default memory size is 2GB
 */
static const int64_t kPoolSize = 1073741824;

/*
 * The size of write2file when we need to rotate
 */
static const uint64_t kMmapSize = 1024 * 1024 * 100;

static std::string kWrite2file = "/write2file";

static std::string kManifest = "/manifest";

}

#endif
