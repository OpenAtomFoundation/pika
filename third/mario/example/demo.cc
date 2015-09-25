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

    std::string item2 = "heiheiadfasdf";
    std::string item = "a";
    std::string item1 = "abcde";
    std::string item3 = "abcdeasdfwupeiq";
    int i = 20000;
    while (i--) {
        s = m->Put(item);
        s = m->Put(item1);
        s = m->Put(item2);
        s = m->Put(item3);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }
    
    s = m->AddConsumer(0, 0, fh1);
    sleep(1);

    i = 20000;
    while (i--) {
        s = m->Put(item);
        s = m->Put(item1);
        s = m->Put(item2);
        s = m->Put(item3);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }

    m->AddConsumer(0, 0, fh2);

    i = 20000;
    while (i--) {
        s = m->Put(item);
        s = m->Put(item1);
        s = m->Put(item2);
        s = m->Put(item3);
        if (!s.ok()) {
            log_err("Put error");
            exit(-1);
        }
    }
    m->AddConsumer(0, 0, fh3);
//    sleep(10);
    delete m;
    log_info("count %d", count);
    delete fh1;
    delete fh2;
    delete fh3;
    return 0;
}
