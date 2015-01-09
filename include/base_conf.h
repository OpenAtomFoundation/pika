#ifndef __BASE_CONF_H__
#define __BASE_CONF_H__

#include "tick_define.h"
#include "stdlib.h"
#include "stdio.h"
#include "xdebug.h"
#include "status.h"


class BaseConf
{
public:
    explicit BaseConf(const char* path);
    virtual ~BaseConf();
    bool ReadConf(const char* path);
    
    bool getConfInt(const char* name, int* value);
    bool getConfStr(const char* name, char* value);
    void DumpConf();

private:
    struct ConfItem {
        char name[TICK_WORD_SIZE];
        char value[TICK_WORD_SIZE];
    };

    ConfItem *item_;
    int num_;
    int size_;

    /*
     * No copy && no assign operator
     */
    BaseConf(const BaseConf&);
    void operator=(const BaseConf&);

};

#endif
