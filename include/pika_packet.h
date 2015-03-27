#ifndef __TICK_PACKET_H__
#define __TICK_PACKET_H__

#include "status.h"
#include <string>
#include "bada_sdk.pb.h"


/*
 * sdkSet proto parse and sdkSetRet build
 */
Status SetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key, std::string *value);

void SetRetBuild(bool status, SdkSetRet *sdkSetRet);


/*
 * sdkGet proto parse and sdkGetRet build
 */
Status GetParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *key);

void GetRetBuild(std::string &val, SdkGetRet *sdkGetRet);

/*
 * hbSend proto parse and hbSendRet build
 */

Status HbSendParse(const int32_t opcode, const char *rbuf, const int32_t rbuf_len, std::string *host, int &port);

void HbSendBuild(const std::string &host, const int port, HbSend* hbSend);

void HbSendRetBuild(bool status, HbSendRet *hbSendRet);
#endif
