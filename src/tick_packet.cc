#include "tick_packet.h"
#include "tick_define.h"

Status SetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key, std::string *value)
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

void SetRetBuild(bool status, SdkSetRet *sdkSetRet)
{
    sdkSetRet->set_opcode(kSdkSetRet);
    sdkSetRet->set_status(status);
}

Status GetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key)
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
        SdkGet sdkGet;
        if (!sdkGet.ParseFromArray(rbuf, rbuf_len)) {
            s = Status::Corruption("Parse error");
            return s;
        }
        if (sdkGet.opcode() == kSdkGet) {
            key->assign(sdkGet.key().data(), sdkGet.key().size());
        } else {
            s = Status::IOError("Get error");
        }
    }
    return s;
}

void GetRetBuild(std::string &value, SdkGetRet *sdkGetRet)
{
    sdkGetRet->set_opcode(kSdkGetRet);
    sdkGetRet->set_value(value);
}

Status HbSendParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *host, int &port)
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
        HbSend hbSend;
        if (!hbSend.ParseFromArray(rbuf, rbuf_len)) {
            s = Status::Corruption("Parse error");
            return s;
        }
        if (hbSend.opcode() == kHbSend) {
            host->assign(hbSend.host().data(), hbSend.host().size());
            port = hbSend.port();
        } else {
            s = Status::IOError("Get error");
        }
    }
    return s;
}

void HbSendRetBuild(bool status, HbSendRet *hbSendRet)
{
    hbSendRet->set_opcode(kHbSendRet);
    hbSendRet->set_status(status);
}
