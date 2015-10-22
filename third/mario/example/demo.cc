#include <iostream>
#include <string>
#include <queue>
#include <sys/time.h>

#include "mario.h"
#include "consumer.h"
#include "mutexlock.h"
#include "port.h"

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
int count = 0;

/**
 * @brief The handler inherit from the Consumer::Handler
 * The processMsg is pure virtual function, so you need implementation your own
 * version
 */
class FileHandler : public mario::Consumer::Handler
{
public:
    FileHandler(int i) : 
    i_(i){};
    virtual bool processMsg(const std::string &item) {
        pthread_mutex_lock(&mutex);
        count++;
        pthread_mutex_unlock(&mutex);
        log_info("thread %d consume data %s", i_, item.data());
        return true;
    }
    int i_;
};

int main()
{
    mario::Status s;
    FileHandler *fh1 = new FileHandler(1);
    FileHandler *fh2 = new FileHandler(2);
    FileHandler *fh3 = new FileHandler(3);
    /**
     * @brief 
     *
     * @param 1 is the thread number
     * @param fh is the handler that you implement. It tell the mario how
     * to consume the data
     * @param 2 is the retry times. The default value is 10 if you don't set it.
     *
     * @return 
     */
    mario::Mario *m = new mario::Mario(2);

    std::string item = "item";
    int cnt = 10;
    for (int i = 0; i < cnt; i++) {
        std::string msg = item + std::to_string(i);
        s = m->Put(msg);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }
    
    std::string ip = "127.0.0.1";

    s = m->AddConsumer(0, 0, fh1, ip, 9221);
    log_info("\nAdd a consumer 9221");

    m->AddConsumer(0, 0, fh2, ip, 9222);
    log_info("\nAdd a consumer 9222");
    sleep(3);
    
    cnt = 20;
    for (int i = 10; i < cnt; i++) {
        std::string msg = item + std::to_string(i);
        s = m->Put(msg);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }

    m->RemoveConsumer(ip, 9221);
    log_info("Remove a consumer 9221");

    cnt = 30;
    for (int i = 20; i < cnt; i++) {
        std::string msg = item + std::to_string(i);
        s = m->Put(msg);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }

    m->RemoveConsumer(ip, 9222);
    log_info("Remove a consumer 9222");

    m->SetProducerStatus(100, 50);

    uint32_t filenum;
    uint64_t offset;
    m->GetProducerStatus(&filenum, &offset);
    printf ("\nafter set filenum=%u, offset=%lu\n", filenum, offset);

    s = m->AddConsumer(100, 50, fh1, ip, 9221);
    log_info("\nAdd a consumer 9221 at(100,50) again");
    cnt = 60;
    for (int i = 50; i < cnt; i++) {
        std::string msg = item + std::to_string(i);
        s = m->Put(msg);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }

    sleep(10);
    delete m;
    log_info("count %d", count);
    delete fh1;
    delete fh2;
    delete fh3;
    return 0;
}
