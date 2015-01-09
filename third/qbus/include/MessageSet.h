#ifndef __MESSAGESET_H__
#define __MESSAGESET_H__

#include "Message.h"
#include "SimpleVector.h"

#ifdef __cplusplus
extern "C" {
#endif

class KafkaMessageSet
{
    public:
	    int validByteCount_ ;
	    bool valid_;
        int curOffset_; // array element offset
        SimpleVector messages_;

    public:
        KafkaMessageSet(char *buf, long int len);
	    ~KafkaMessageSet();

    public:
        int validBytes();
	    int sizeInBytes();
	    void next();
	    bool valid();
	    int key();
	    KafkaMessage* current();
	    void rewind();
};

#ifdef __cplusplus
}
#endif

#endif // __MESSAGESET_H__
