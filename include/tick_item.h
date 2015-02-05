#ifndef __TICK_ITEM_H__
#define __TICK_ITEM_H__

#include "status.h"
#include "tick_define.h"

class TickItem
{
public:
    TickItem() {};
    TickItem(int fd);
    /*
     * ~TickItem();
     */

    int fd() { return fd_; }

private:

    int fd_;

    /*
     * No copy && assigned allowed
     */
    /*
     * TickItem(const TickItem&);
     * void operator=(const TickItem&);
     */

};

#endif
