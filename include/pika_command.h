#ifndef __PIKA_COMMAND_H__
#define __PIKA_COMMAND_H__

#include <list>
#include <string>

class Cmd
{
public:
    Cmd(int a) : arity(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret) {};
    int arity;
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
    GetCmd(int a) : Cmd(a) {};
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

class ScanCmd : public Cmd {
public:
    ScanCmd(int a) : Cmd(a) {};
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
    HGetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HDelCmd : public Cmd {
public:
    HDelCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HExistsCmd : public Cmd {
public:
    HExistsCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HGetallCmd : public Cmd {
public:
    HGetallCmd(int a) : Cmd(a) {};
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
    HKeysCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HLenCmd : public Cmd {
public:
    HLenCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HMSetCmd : public Cmd {
public:
    HMSetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HMGetCmd : public Cmd {
public:
    HMGetCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HSetnxCmd : public Cmd {
public:
    HSetnxCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HValsCmd : public Cmd {
public:
    HValsCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HStrlenCmd : public Cmd {
public:
    HStrlenCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class HScanCmd : public Cmd {
public:
    HScanCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

/*
 * Lists
 */
class LIndexCmd : public Cmd {
public:
    LIndexCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LInsertCmd : public Cmd {
public:
    LInsertCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class LLenCmd : public Cmd {
public:
    LLenCmd(int a) : Cmd(a) {};
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
    LRangeCmd(int a) : Cmd(a) {};
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
    ZCardCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZScanCmd : public Cmd {
public:
    ZScanCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZIncrbyCmd : public Cmd {
public:
    ZIncrbyCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangeCmd : public Cmd {
public:
    ZRangeCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangebyscoreCmd : public Cmd {
public:
    ZRangebyscoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZCountCmd : public Cmd {
public:
    ZCountCmd(int a) : Cmd(a) {};
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
    ZRankCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrankCmd : public Cmd {
public:
    ZRevrankCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZScoreCmd : public Cmd {
public:
    ZScoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrangeCmd : public Cmd {
public:
    ZRevrangeCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRevrangebyscoreCmd : public Cmd {
public:
    ZRevrangebyscoreCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZRangebylexCmd : public Cmd {
public:
    ZRangebylexCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};

class ZLexcountCmd : public Cmd {
public:
    ZLexcountCmd(int a) : Cmd(a) {};
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

#endif
