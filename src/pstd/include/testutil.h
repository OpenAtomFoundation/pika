#ifndef __PSTD_INCLUDE_TESTUTIL_H__
#define __PSTD_INCLUDE_TESTUTIL_H__

#include <string>
#include <iostream>

namespace pstd {

extern std::string RandomString(const int len);
extern int RandomSeed();
extern int GetTestDirectory(std::string* result);

extern char* get_date_time();

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

void current_time_str(char * str, size_t max_len);
#define output(fmt, args...) do { \
        char __time_str__[1024];\
        pstd::current_time_str(__time_str__, sizeof(__time_str__)); \
        printf("[%s] [%s] [%d]" fmt "\n", __time_str__, __FILENAME__, __LINE__, ##args); \
    } while (0)

};  // namespace pstd

#endif  // __PSTD_INCLUDE_TESTUTIL_H__
