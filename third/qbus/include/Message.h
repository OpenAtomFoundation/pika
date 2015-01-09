#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <string>
#include <vector>

class KafkaMessage
{
    private:
	    unsigned int size_;
	    char compression_;
	    unsigned int crc_;
	    bool valid_;
        char * data_;
        unsigned int dataSize_;

    public:
        std::vector<std::string> payload_;

        KafkaMessage(char* data, unsigned int dataSize);
        int init();
#if 0
	    std::string encode();
#endif
	    unsigned int size();
	    int magic();
	    unsigned int checksum();
        bool isValid();
};

#endif // __MESSAGE_H__
