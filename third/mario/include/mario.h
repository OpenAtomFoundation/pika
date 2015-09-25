#ifndef __MARIO_H_
#define __MARIO_H_

#include <deque>
#include <vector>
#include "consumer.h"
#include "env.h"
#include "port.h"
#include "status.h"
#include "xdebug.h"

namespace mario {

class Producer;
class Consumer;
class Version;

class ConsumerItem {
public:
    ConsumerItem(SequentialFile* readfile, Consumer* consumer, Consumer::Handler* h) :
        readfile_(readfile),
        consumer_(consumer),
        h_(h) {
        };
    ~ConsumerItem() {
        delete consumer_;
        delete readfile_;
    }
    SequentialFile* readfile_;
    Consumer* consumer_;
    Consumer::Handler* h_;
};

struct ThreadArg {
    void* ptr;
    ConsumerItem* consumer_item;
};

class Mario
{
public:
    Mario(int32_t retry = 10);
    ~Mario();
    Status Put(const std::string &item);
    Status Put(const char* item, int len);
    Status AddConsumer(uint32_t filenum, uint64_t con_offset, Consumer::Handler* h); 
    Env *env() { return env_; }
    WritableFile *writefile() { return writefile_; }

private:

    struct Writer;
    Producer *producer_;
    std::vector<ConsumerItem*> consumers_;
    uint32_t consumer_num_;
    uint64_t item_num_;
    Env* env_;
    WritableFile *writefile_;
    RWFile *versionfile_;
    Version* version_;
    port::Mutex mutex_;
    port::CondVar bg_cv_;
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
