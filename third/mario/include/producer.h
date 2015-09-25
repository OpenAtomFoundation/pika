#ifndef __MARIO_PRODUCER_H_
#define __MARIO_PRODUCER_H_

#include "env.h"
#include "mario_item.h"
#include "status.h"
#include "version.h"

namespace mario {

class Version;

class Producer 
{
public:

    explicit Producer(WritableFile *queue, Version *version);
    ~Producer();
    Status Produce(const Slice &item);

private:

    WritableFile *queue_;
    Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n);
    Version *version_;
    int block_offset_;

    // No copying allowed
    Producer(const Producer&);
    void operator=(const Producer&);
};

}

#endif
