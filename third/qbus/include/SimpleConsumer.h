#ifndef __SIMPLECONSUMER_H__
#define __SIMPLECONSUMER_H__

#include <string>
#include "FetchRequest.h"
#include "MessageSet.h"
#include "Receive.h"

class KafkaSimpleConsumer
{
    public:
        std::string host_;
    	int port_;
	    int sockTimeout_;
	    int sockBufSize_ ;
        char *sockBuf_;
	    int conn_;

    public:
	    KafkaSimpleConsumer(std::string host, int port, int sockTimeout, int sockBufSize);
        ~KafkaSimpleConsumer();
        int connectBroker();
        int reconnectBroker();
        int closeBrokerConnection();
	    KafkaMessageSet* fetch(KafkaFetchRequest *req);
	    int sendRequest(KafkaFetchRequest *req);

    protected:
        KafkaBoundedByteBufferReceive* getResponse(int *errorCode);
};

#endif // __SIMPLECONSUMER_H__
