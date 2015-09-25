## Overview

**A library that make the write from synchronous to asynchronous.**

It is thread safe and easily used.

## Idea
The base idea of the Mario is that if you have operators that cost lots of time. But you want quickly return from the operator, and leave the work to the background.

So mario library can easily solve your problem. You just need the implement your own consume message function. Then mario will create background thread do the other things.

#### Two engine type

* memory

This type will leave the cache buffer in memory. The advantage is the Insert speed is faster. But when your application shutdown before you have consume the data in the buffer, The data will lost.

* file

Thie type will make a write2file in the local log path. When you put message, you will write the data in the write2file. The write is append operator, so it is also very fast. The application don't initiativly flush the data to disk, it flush according to the operator system.

If there is still some data havn't been consumed after shutdown you application, it doesn't matter. it will leave a menifest file in the log path, after you restart you application. it will first get the menifest file, and get the offset. Then it consume the data from the offset

## Usage

    make ENGINE=memory

then you will get a output path in this path, it include the lib and the include file.

You also can see how to used in the example path

## Example

```

#include "mario.h"
#include "consumer.h"
#include "mutexlock.h"
#include "port.h"

/**
 * @brief The handler inherit from the Consumer::Handler
 * The processMsg is pure virtual function, so you need implementation your own
 * version
 */
class FileHandler : public mario::Consumer::Handler
{
    public:
        FileHandler() {};
        virtual void processMsg(const std::string &item) {
            log_info("consume data %s", item.data());
        }
};

int main()
{
    mario::Status s;
    FileHandler *fh = new FileHandler();
    /**
     * @brief
     *
     * @param 1 is the thread number
     * @param fh is the handler that you implement. It tell the mario how
     * to consume the data
     *
     * @return
     */
    mario::Mario *m = new mario::Mario(1, fh);

    std::string item = "Put data in mario";
    s = m->Put(item);
    if (!s.ok()) {
        log_err("Put error");
        exit(-1);
    }

    delete m;
    delete fh;
    return 0;
}

```

