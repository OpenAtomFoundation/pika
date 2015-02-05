#include "tick_packet.h"
#include "tick_define.h"

Status SetBufferParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key, std::string *value)
{
    Status s;
    if (opcode == kSdkInvalidOperation) {
        SdkInvalidOperation sdkInvalidOperation;
        if (sdkInvalidOperation.ParseFromArray(rbuf, rbuf_len)) {
            s = Status::Corruption("Parse invalid operation error");
            return s;
        }
        if (sdkInvalidOperation.what() == 1003) {
            return Status::NotFound("Can't not found the key");
        }
        return Status::InvalidArgument("Invalid operation");
    } else {
        SdkSet sdkSet;
        if (!sdkSet.ParseFromArray(rbuf, rbuf_len)) {
            s = Status::Corruption("Parse error");
            return s;
        }
        if (sdkSet.opcode() == kSdkSet) {
            key->assign(sdkSet.key().data(), sdkSet.key().size());
            value->assign(sdkSet.value().data(), sdkSet.value().size());
        } else {
            s = Status::IOError("Set error");
        }
    }
    return s;
}
