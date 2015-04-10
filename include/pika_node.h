#ifndef PIKA_NODE_H_
#define PIKA_NODE_H_

#include <stdio.h>
#include <string>

class PikaNode
{
public:
    PikaNode(std::string host, int port) :
        host_(host),
        port_(port)
    {};

    const std::string* host() { return &host_; }
    const int port() { return port_; }

    bool operator==(const PikaNode& rval);
private:

    std::string host_;
    int port_;
};

#endif
