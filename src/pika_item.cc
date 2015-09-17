#include "pika_item.h"
#include "pika_define.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include "csapp.h"

PikaItem::PikaItem(int fd, std::string ip_port) :
    fd_(fd), ip_port_(ip_port)
{
}

PikaItem::~PikaItem()
{
}
