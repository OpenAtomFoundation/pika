#ifndef __ENV_H__
#define __ENV_H__

#include <stdio.h>

class QbusEnv
{
    public:
        static int initEnv();
        static void destoryEnv();

    private:
        static FILE* zkLogFp_;
        static void initSignals();
        static void initSignal(int sig);
        static int initZkLogStream();
        static void destoryZkLogStream();
        static bool inited_;
        static pthread_mutex_t env_lock_;
        static int logRefCount_;
};

#endif // __ENV_H__
