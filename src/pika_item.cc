#include "pika_item.h"
#include "pika_define.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include "csapp.h"

PikaItem::PikaItem(int fd) :
    fd_(fd)
{
}

PikaItem::~PikaItem()
{
}
