#ifndef __PIKA_THREAD_H__
#define __PIKA_THREAD_H__

#include "xdebug.h"

#include <pthread.h>
#include <sys/epoll.h>
#include <queue>
#include <map>

#include "port.h"
#include "pika_item.h"
#include "status.h"
#include "csapp.h"


class PikaItem;
class PikaEpoll;
class PikaConn;

class PikaThread
{
public:
    PikaThread();
    ~PikaThread();

    void RunProcess();


    int notify_receive_fd() { return notify_receive_fd_; }
    int notify_send_fd() { return notify_send_fd_; }

    static void CreateThread(pthread_t &pid, PikaThread* pikaThread);
    static void* StartThread(void* arg);

    pthread_t thread_id_;

    // port::Mutex mutex() { return mutex_; }

private:

    friend class PikaWorker;
    friend class PikaHb;

    /*
     * These two fd receive the notify from master thread
     */
    int notify_receive_fd_;
    int notify_send_fd_;

    /*
     * The PikaItem queue is the fd queue, receive from master thread
     */
    std::queue<PikaItem> conn_queue_;

    /*
     * The epoll handler
     */
    PikaEpoll *pikaEpoll_;

    std::map<int, PikaConn*> conns_;


    port::Mutex mutex_;

    // No copy || assigned operator allowed
    PikaThread(const PikaThread&);
    void operator=(const PikaThread &);
};

#endif
