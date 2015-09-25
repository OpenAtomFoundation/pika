#ifndef __MARIO_VERSION_H_
#define __MARIO_VERSION_H_

#include <stdio.h>
#include <stdint.h>
#include "status.h"
#include "env.h"
#include "xdebug.h"
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>


namespace mario {

class Version 
{
public:
    Version(RWFile *save);
    ~Version();

    // Status Recovery(WritableFile *save);

    Status StableSave();
    Status InitSelf();

    uint64_t pro_offset() { return pro_offset_; }
    void set_pro_offset(uint64_t pro_offset) { pro_offset_ = pro_offset; }
    void rise_pro_offset(uint64_t r) { pro_offset_ += r; }

    uint64_t con_offset() { return con_offset_; }
    void set_con_offset(uint64_t con_offset) { con_offset_ = con_offset; }
    void rise_con_offset(uint64_t r) { con_offset_ += r; }

    uint32_t item_num() { return item_num_; }
    void set_item_num(uint32_t item_num) { item_num_ = item_num; }
    void plus_item_num() { item_num_++; }
    void minus_item_num() { item_num_--; }

    uint32_t pronum() { return pronum_; }
    void set_pronum(uint32_t pronum) { pronum_ = pronum; }

    uint32_t connum() { return connum_; }
    void set_connum(uint32_t connum) { connum_ = connum; }

    void debug() {
        log_info("Current pro_offset %" PRIu64 " con_offset %" PRIu64 " itemnum %u pronum %u connum %u", 
                pro_offset_, con_offset_, item_num_, pronum_, connum_);
    }

private:
    uint64_t pro_offset_;
    uint64_t con_offset_;
    uint32_t item_num_;
    uint32_t pronum_;
    uint32_t connum_;

    RWFile *save_;
    // port::Mutex mutex_;


    // No copying allowed;
    Version(const Version&);
    void operator=(const Version&);
};

} // namespace mario

#endif
