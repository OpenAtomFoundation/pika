#ifndef __PIKA_H__
#define __PIKA_H__

#include <stdio.h>
#include <pthread.h>

class PikaWorker;
class PikaHb;

class Pika
{
public:
    Pika();
    ~Pika();

    int RunHb();
    static void* StartHb(void* args);
    int RunWorker();
    static void* StartWorker(void* args);

    PikaWorker *PikaWorker_() { return pikaWorker_; }
    PikaHb *PikaHb_() { return pikaHb_; }

private:

    pthread_t workerId_, hbId_;
    PikaWorker *pikaWorker_;
    PikaHb *pikaHb_;


    // No copying allowed
    Pika(const Pika&);
    void operator=(const Pika&);

};

#endif
