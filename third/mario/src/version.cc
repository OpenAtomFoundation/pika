#include "version.h"
#include "xdebug.h"

namespace mario {

Version::Version(RWFile *save) :
    save_(save)
{
    save_->GetData();
    pro_offset_ = 0;
    con_offset_ = 0;
    item_num_ = 0;
    pronum_ = 0;
    connum_ = 0;
    assert(save_ != NULL);
}

Version::~Version()
{
    StableSave();
}

Status Version::StableSave()
{

    char *p = save_->GetData();
    memcpy(p, &pro_offset_, sizeof(uint64_t));
    p += 8;
    memcpy(p, &con_offset_, sizeof(uint64_t));
    p += 8;
    memcpy(p, &item_num_, sizeof(uint32_t));
    p += 4;
    memcpy(p, &pronum_, sizeof(uint32_t));
    p += 4;
    memcpy(p, &connum_, sizeof(uint32_t));
    p += 4;
    return Status::OK();
}

Status Version::InitSelf()
{
    Status s;
    if (save_->GetData() != NULL) {
        memcpy((char*)(&pro_offset_), save_->GetData(), sizeof(uint64_t));
        memcpy((char*)(&con_offset_), save_->GetData() + 8, sizeof(uint64_t));
        memcpy((char*)(&item_num_), save_->GetData() + 16, sizeof(uint32_t));
        memcpy((char*)(&pronum_), save_->GetData() + 20, sizeof(uint32_t));
        memcpy((char*)(&connum_), save_->GetData() + 24, sizeof(uint32_t));
        // log_info("InitSelf pro_offset %llu itemnum %u pronum %u connum %u", pro_offset_, item_num_, pronum_, connum_);
        return Status::OK();
    } else {
        return Status::Corruption("version init error");
    }
}

} // namespace mario
