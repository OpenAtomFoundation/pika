#ifndef __MARIO_CONSUMER_H_
#define __MARIO_CONSUMER_H_

#include "env.h"
#include "status.h"

namespace mario {

class Version;


class Consumer
{
public:
    class Handler
    {
    public:
        virtual ~Handler();
        virtual bool processMsg(const std::string &item) = 0;
    };

//    Consumer(SequentialFile *queue, Handler *h, Version *version, uint32_t filenum);
    Consumer(SequentialFile *queue, Handler *h, uint64_t con_offset, uint32_t filenum);
    uint64_t last_record_offset () const { return last_record_offset_; }
    uint32_t filenum() { return filenum_; }
    uint64_t con_offset() { return con_offset_; }
    int trim();
    uint64_t get_next(bool &is_error);

    ~Consumer();
    Status Consume(std::string &scratch);
    Handler *h() { return h_; }

private:
    Handler *h_;
    uint64_t initial_offset_;
    uint64_t last_record_offset_;
    uint64_t end_of_buffer_offset_;

    SequentialFile* const queue_;
    char* const backing_store_;
    Slice buffer_;
    bool eof_;
//    Version* version_;
    uint64_t con_offset_;
    uint32_t filenum_;

    unsigned int ReadPhysicalRecord(Slice *fragment);

    // No copying allowed
    Consumer(const Consumer&);
    void operator=(const Consumer&);
};

}
#endif
