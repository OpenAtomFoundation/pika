#ifndef __PIKA_COMMAND_H__
#define __PIKA_COMMAND_H__

#include <list>
#include <string>

class Cmd
{
public:
    Cmd(int a, bool i = true) : arity(a) , is_sync(i){};
    virtual void Do(std::list<std::string> &argvs, std::string &ret) {};
    int arity;
    bool is_sync;
};

/*
 * admin
 */
class AuthCmd : public Cmd {
public:
    AuthCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class PingCmd : public Cmd {
public:
    PingCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ClientCmd : public Cmd {
public:
    ClientCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SlaveofCmd : public Cmd {
public:
    SlaveofCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class PikasyncCmd : public Cmd {
public:
    PikasyncCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

//class BemasterCmd : public Cmd {
//public:
//    BemasterCmd(int a, bool i = false) : Cmd(a, i) {};
//    virtual void Do(std::list<std::string> &argvs, std::string &ret);
//};

class ConfigCmd : public Cmd {
public:
    ConfigCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class InfoCmd : public Cmd {
public:
    InfoCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class UcanpsyncCmd : public Cmd {
public:
    UcanpsyncCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SyncerrorCmd : public Cmd {
public:
    SyncerrorCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LoaddbCmd : public Cmd {
public:
    LoaddbCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class DumpCmd : public Cmd {
public:
    DumpCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ReadonlyCmd : public Cmd {
public:
    ReadonlyCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

/*
 * kv
 */
class SetCmd : public Cmd {
public:
    SetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class GetCmd : public Cmd {
public:
    GetCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class DelCmd : public Cmd {
public:
    DelCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class IncrCmd : public Cmd {
public:
    IncrCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class IncrbyCmd : public Cmd {
public:
    IncrbyCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class IncrbyfloatCmd : public Cmd {
public:
    IncrbyfloatCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class DecrCmd : public Cmd {
public:
    DecrCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class DecrbyCmd : public Cmd {
public:
    DecrbyCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class GetsetCmd : public Cmd {
public:
    GetsetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class AppendCmd : public Cmd {
public:
    AppendCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class MgetCmd : public Cmd {
public:
    MgetCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SetnxCmd : public Cmd {
public:
    SetnxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SetexCmd : public Cmd {
public:
    SetexCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class MsetCmd : public Cmd {
public:
    MsetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class MsetnxCmd : public Cmd {
public:
    MsetnxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class GetrangeCmd : public Cmd {
public:
    GetrangeCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SetrangeCmd : public Cmd {
public:
    SetrangeCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class StrlenCmd : public Cmd {
public:
    StrlenCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ExistsCmd : public Cmd {
public:
    ExistsCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ExpireCmd : public Cmd {
public:
    ExpireCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ExpireatCmd : public Cmd {
public:
    ExpireatCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class TtlCmd : public Cmd {
public:
    TtlCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class PersistCmd : public Cmd {
public:
    PersistCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ScanCmd : public Cmd {
public:
    ScanCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

/*
 * hash
 */
class HSetCmd : public Cmd {
public:
    HSetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HGetCmd : public Cmd {
public:
    HGetCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HDelCmd : public Cmd {
public:
    HDelCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HExistsCmd : public Cmd {
public:
    HExistsCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HGetallCmd : public Cmd {
public:
    HGetallCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HIncrbyCmd : public Cmd {
public:
    HIncrbyCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HIncrbyfloatCmd : public Cmd {
public:
    HIncrbyfloatCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HKeysCmd : public Cmd {
public:
    HKeysCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HLenCmd : public Cmd {
public:
    HLenCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HMSetCmd : public Cmd {
public:
    HMSetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HMGetCmd : public Cmd {
public:
    HMGetCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HSetnxCmd : public Cmd {
public:
    HSetnxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HValsCmd : public Cmd {
public:
    HValsCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HStrlenCmd : public Cmd {
public:
    HStrlenCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HScanCmd : public Cmd {
public:
    HScanCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

/*
 * Lists
 */
class LIndexCmd : public Cmd {
public:
    LIndexCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LInsertCmd : public Cmd {
public:
    LInsertCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LLenCmd : public Cmd {
public:
    LLenCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LPopCmd : public Cmd {
public:
    LPopCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LPushCmd : public Cmd {
public:
    LPushCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LPushxCmd : public Cmd {
public:
    LPushxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LRangeCmd : public Cmd {
public:
    LRangeCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LRemCmd : public Cmd {
public:
    LRemCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LSetCmd : public Cmd {
public:
    LSetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LTrimCmd : public Cmd {
public:
    LTrimCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class RPopCmd : public Cmd {
public:
    RPopCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class RPopLPushCmd : public Cmd {
public:
    RPopLPushCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class RPushCmd : public Cmd {
public:
    RPushCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class RPushxCmd : public Cmd {
public:
    RPushxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};
/*
 * zset
 */
class ZAddCmd : public Cmd {
public:
    ZAddCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZCardCmd : public Cmd {
public:
    ZCardCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZScanCmd : public Cmd {
public:
    ZScanCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZIncrbyCmd : public Cmd {
public:
    ZIncrbyCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangeCmd : public Cmd {
public:
    ZRangeCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangebyscoreCmd : public Cmd {
public:
    ZRangebyscoreCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZCountCmd : public Cmd {
public:
    ZCountCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRemCmd : public Cmd {
public:
    ZRemCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZUnionstoreCmd : public Cmd {
public:
    ZUnionstoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZInterstoreCmd : public Cmd {
public:
    ZInterstoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRankCmd : public Cmd {
public:
    ZRankCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrankCmd : public Cmd {
public:
    ZRevrankCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZScoreCmd : public Cmd {
public:
    ZScoreCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrangeCmd : public Cmd {
public:
    ZRevrangeCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrangebyscoreCmd : public Cmd {
public:
    ZRevrangebyscoreCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangebylexCmd : public Cmd {
public:
    ZRangebylexCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZLexcountCmd : public Cmd {
public:
    ZLexcountCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRemrangebylexCmd : public Cmd {
public:
    ZRemrangebylexCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRemrangebyrankCmd : public Cmd {
public:
    ZRemrangebyrankCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRemrangebyscoreCmd : public Cmd {
public:
    ZRemrangebyscoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

/*
 * set
 */

class SAddCmd : public Cmd {
public:
    SAddCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SRemCmd : public Cmd {
public:
    SRemCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SCardCmd : public Cmd {
public:
    SCardCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SMembersCmd : public Cmd {
public:
    SMembersCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SScanCmd : public Cmd {
public:
    SScanCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SUnionCmd : public Cmd {
public:
    SUnionCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SUnionstoreCmd : public Cmd {
public:
    SUnionstoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SInterCmd : public Cmd {
public:
    SInterCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SInterstoreCmd : public Cmd {
public:
    SInterstoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SIsmemberCmd : public Cmd {
public:
    SIsmemberCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SDiffCmd : public Cmd {
public:
    SDiffCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SDiffstoreCmd : public Cmd {
public:
    SDiffstoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SMoveCmd : public Cmd {
public:
    SMoveCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SPopCmd : public Cmd {
public:
    SPopCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class SRandmemberCmd : public Cmd {
public:
    SRandmemberCmd(int a, bool i = false) : Cmd(a, i) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

#endif
