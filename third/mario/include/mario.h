#ifndef __MARIO_H_
#define __MARIO_H_

#include <list>
#include <deque>
#include <vector>
#include <pthread.h>

#include "mutexlock.h"
#include "consumer.h"
#include "env.h"
#include "port.h"
#include "status.h"
#include "xdebug.h"


namespace mario {

class Producer;
class Consumer;
class Version;
//class RWLock;

class ConsumerItem {
public:
    ConsumerItem(SequentialFile* readfile, Consumer* consumer, Consumer::Handler* h, int fd = 0, pthread_t tid = 0) :
        readfile_(readfile),
        consumer_(consumer),
        h_(h),
        fd_(fd), tid_(tid),
        should_exit_(false) {
            pthread_rwlock_init(&rwlock_, NULL);
        };
    ~ConsumerItem() {
        pthread_rwlock_destroy(&rwlock_);
        delete consumer_;
        delete readfile_;
        delete h_;
    }

    bool IsExit()     { RWLock l(&rwlock_, false); return should_exit_; }
    void SetExit()    { RWLock l(&rwlock_, true);  should_exit_ = true; }

    SequentialFile* readfile_;
    Consumer* consumer_;
    Consumer::Handler* h_;

    //std::string ip_;
    //int port_;
    int fd_;
    pthread_t tid_;

private:
    bool should_exit_;
    pthread_rwlock_t rwlock_;
};

struct ThreadArg {
    void* ptr;
    ConsumerItem* consumer_item;
};

class Mario
{
public:
    Mario(const char* mario_path, uint64_t file_size, int32_t retry = 10);
    ~Mario();
    Status Put(const std::string &item);
    Status Put(const char* item, int len);
    Status PutNoLock(const std::string &item);
    Status PutNoLock(const char* item, int len);

    void Lock()         { mutex_.Lock(); }
    void Unlock()       { mutex_.Unlock(); }

    Status AddConsumer(uint32_t filenum, uint64_t con_offset, Consumer::Handler* h, int fd); 
    Status RemoveConsumer(int fd);

    Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset);
    Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset);


    // set the filenum and con_offset of the consumer which has the given ip and port;
    // return NotFound when can not find the consumer with the given ip and port;
    // return InvalidArgument when the filenum and con_offset are invalid;
    Status SetConsumer(int fd, uint32_t filenum, uint64_t con_offset);
    // no lock 
    Status GetConsumerStatus(int fd, uint32_t* filenum, uint64_t* con_offset);

    Status GetStatus(uint32_t* max);
    Status AppendBlank(WritableFile *file, uint64_t len);

    Env *env() { return env_; }
    WritableFile *writefile() { return writefile_; }

private:

    struct Writer;
    Producer *producer_;
    std::list<ConsumerItem*> consumers_;
    uint32_t consumer_num_;
    uint64_t item_num_;
    Env* env_;
    WritableFile *writefile_;
    RWFile *versionfile_;
    Version* version_;
    port::Mutex mutex_;
//    port::CondVar bg_cv_;
    uint32_t pronum_;
    int32_t retry_;
    ThreadArg arg_;

    std::string filename_;

    static void SplitLogWork(void* m);
    void SplitLogCall();

    static void BGWork(void* m);
    void BackgroundCall(ConsumerItem* consumer_item);

    std::deque<Writer *> writers_;

    char* pool_;
    bool exit_all_consume_;
    const std::string mario_path_;

    // No copying allowed
    Mario(const Mario&);
    void operator=(const Mario&);

};

} // namespace mario

#endif
