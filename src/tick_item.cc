#include "tick_item.h"
#include "tick_define.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include "csapp.h"

TickItem::TickItem(const char* str, int len) :
    len_(len)
{
    msg_ = (char *)malloc(sizeof(char) * len_);
    memcpy(msg_, str, len_);
}

TickItem::~TickItem()
{
    free(msg_);
}
