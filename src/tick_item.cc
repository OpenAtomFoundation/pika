#include "tick_item.h"
#include "tick_define.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include "csapp.h"

TickItem::TickItem(int fd) :
    fd_(fd)
{
}

TickItem::~TickItem()
{
}
