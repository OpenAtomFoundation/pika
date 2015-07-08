#ifndef __PIKA_ITEM_H__
#define __PIKA_ITEM_H__

#include "pika_define.h"

class PikaItem
{
public:
    PikaItem() {};
    PikaItem(int fd);
    ~PikaItem();

    int fd() { return fd_; }

private:

    int fd_;

    /*
     * No copy && assigned allowed
     */
    /*
     * PikaItem(const PikaItem&);
     * void operator=(const PikaItem&);
     */

};

#endif
