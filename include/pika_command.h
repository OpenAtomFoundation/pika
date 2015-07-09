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

#endif
