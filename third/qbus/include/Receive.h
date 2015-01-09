#ifndef __RECEIVE_H__
#define __RECEIVE_H__

class KafkaBoundedByteBufferReceive
{
    private:
        int readRequestSize(int stream, int *readSize);

    protected:
	    char* buffer_;
        long int size_;
        long int maxBufSize_;
	    long int remainingBytes_;
	    bool isSizeRead_;
	    bool complete_;

    public:
        KafkaBoundedByteBufferReceive(int maxBufSize);
	    ~KafkaBoundedByteBufferReceive();
        int readFrom(int stream, int *totalRead);
        int readCompletely(int stream, int *recvSize);
        char* getBuffer();
        long int getBufferSize();
};

#endif // __RECEIVE_H__
