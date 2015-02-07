#ifndef __TICK_PACKET_H__
#define __TICK_PACKET_H__

#include "status.h"
#include <string>
#include "bada_sdk.pb.h"


Status SetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key, std::string *value);

void SetRetBuild(bool status, SdkSetRet *sdkSetRet);


Status GetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key);

void GetRetBuild(std::string &val, SdkGetRet *sdkGetRet);
#endif
