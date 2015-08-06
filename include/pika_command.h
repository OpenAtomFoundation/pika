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

class LPushCmd : public Cmd {
public:
    LPushCmd(int a) : Cmd(a) {};
    virtual void Do(std::list<std::string> &argvs, std::string &ret);
};
#endif
