#ifndef __PIKA_ITEM_H__
#define __PIKA_ITEM_H__

#include "pika_define.h"
#include <string>

class PikaItem
{
public:
    PikaItem() {};
    PikaItem(int fd, std::string ip_port);
    ~PikaItem();

    int fd() { return fd_; }
    std::string ip_port() { return ip_port_; }

private:

    int fd_;
    std::string ip_port_;

    /*
     * No copy && assigned allowed
     */
    /*
     * PikaItem(const PikaItem&);
     * void operator=(const PikaItem&);
     */

};

#endif
