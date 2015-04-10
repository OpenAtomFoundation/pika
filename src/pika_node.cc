#include "pika_node.h"

bool PikaNode::operator==(const PikaNode& rval)
{
    if ((*(rval.host())) == this->host_ && rval.port() == this->port_) {
        return true;
    }
    return false;
}
