#ifndef __SEND_H__
#define __SEND_H__

#include "FetchRequest.h"

#ifdef __cplusplus
extern "C" { 
#endif 

class KafkaBoundedByteBufferSend 
{
    private:
        int writeRequestSize();
	    int writeTo(int stream); 
    
    protected:
        int size_;
        int sendSize_;
	    char* buffer_;
	    bool sizeWritten_;
	    bool complete_;
    
    public:
	    KafkaBoundedByteBufferSend(KafkaFetchRequest *req); 
        ~KafkaBoundedByteBufferSend();
        int writeCompletely(int stream, int *sendSize); 
};

#ifdef __cplusplus
} 
#endif 

#endif // __SEND_H__
