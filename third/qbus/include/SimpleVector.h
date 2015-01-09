#ifndef _SIMPLEVECTOR_H
#define _SIMPLEVECTOR_H

#include <sys/types.h>
#include <stdint.h>


class SimpleVector {
private:
    void *array_;
    unsigned int elem_sz_;
    unsigned int capacity_;
    unsigned int size_;
    int errno_;

    bool expand();

public:
    SimpleVector(unsigned int elem_sz = 8, unsigned int capacity = 32);
    ~SimpleVector();

    bool push_back(const void *val, bool is_ptr = true);
    void *pop_back();
    void *at(unsigned int n) const;
    void clear();
    bool erase(unsigned int n);

    unsigned int capacity() const;
    unsigned int size() const;

    void *data() const;
    unsigned int data_cursor(int flag = 1);
};


#endif
