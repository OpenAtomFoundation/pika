#ifndef __FETCHREQUEST_H__
#define __FETCHREQUEST_H__

#include <string>
#include <sys/types.h>

class KafkaFetchRequest 
{
    private:
        int id_;
        std::string topic_;
        int partition_;
        int64_t offset_;
        int maxFetchSize_;

    public:
        KafkaFetchRequest(std::string topic, int partition, int64_t offset, int maxSize);
        void writeTo(unsigned char* buffer, int *offset); 
        int getId();
        int sizeInBytes(); 
        std::string getTopic();
        int getPartition();
        int64_t getOffset();
        int getMaxFetchSize();
};

#endif //__FETCHREQUEST_H__ 
